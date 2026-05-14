"""Operations for syncing UC Davis IAM data into beanie User records.

Three ops live here:

  - `SyncUserIAM` — single-user sync. Implements the full state machine:
      hit / hit_restored / miss_first / miss_within_grace / miss_offboarding /
      miss_already_expiring / miss_never_seen / no_iam_id.
  - `SyncAllUsersIAM` — driver that loops `SyncUserIAM` over candidate users
    (filtered to `IAM_SYNCABLE_USER_TYPES` so administrative `system`/`class`/
    `shared` accounts are never touched).
  - `ReapOffboardedUsers` — separate op that flips `offboarding` users whose
    `expires_at` has passed to `inactive`. No IAM I/O; runs on its own
    schedule.

Transient errors (5xx, transport, timeout) propagate out of `execute()`
*before* any DB write so the operation's transaction rolls back. The driver
catches them and continues with the next user.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from beanie.operators import In, LTE
from pymongo import AsyncMongoClient
from pymongo.asynchronous.client_session import AsyncClientSession

from ..constants import IAM_SYNCABLE_USER_TYPES
from ..iam_async import (
    IAM_MISSING,
    AsyncIAMAPI,
    IAMTransientError,
    IAMUserPayload,
    build_ucdiam_info,
)
from ..models.user import UCDIAMInfo, User
from ..queries.access_status import find_status_group, resolve_status_name
from .base import Operation

logger = logging.getLogger(__name__)


@dataclass
class SyncUserIAMResult:
    """Compact result returned from SyncUserIAM. The History entry's
    describe() carries a richer dict; this is for direct callers."""

    username: str
    outcome: str
    iam_id: int | None
    expires_at: datetime | None
    status: str


def _isoformat(dt: datetime | None) -> str | None:
    return dt.isoformat() if dt is not None else None


def _naive_utc(dt: datetime | None) -> datetime:
    """Normalize a datetime to naive UTC.

    The async mongo client is configured `tz_aware=False`, so values read
    back from the DB are naive. To compare/subtract them safely against
    the operation's `now`, we strip any tzinfo on input. `None` defaults
    to the current naive UTC instant.
    """
    if dt is None:
        return datetime.now(timezone.utc).replace(tzinfo=None)
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


class SyncUserIAM(Operation):
    """Sync one user against the IAM API and update their state machine.

    Transient errors raise out of execute() before any writes; the operation's
    transaction rolls back and no History entry is written for that user.
    Caller (typically SyncAllUsersIAM) is expected to catch and continue.
    """

    op_name = 'sync_user_iam'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        username: str,
        iam_api: AsyncIAMAPI,
        grace_days: int,
        expiry_offset_days: int,
        now: datetime | None = None,
    ) -> None:
        super().__init__(client, author)
        self.username = username
        self.iam_api = iam_api
        self.grace_days = grace_days
        self.expiry_offset_days = expiry_offset_days
        self.now = _naive_utc(now)
        # Filled in by execute() so describe() can report on what happened.
        self._outcome: str = ''
        self._iam_id: int | None = None
        self._expires_at: datetime | None = None
        self._first_missing_at: datetime | None = None
        self._last_seen_at: datetime | None = None
        self._status: str = ''

    async def execute(self, session: AsyncClientSession) -> SyncUserIAMResult:
        user = await User.find_one(User.name == self.username, session=session)
        if user is None:
            raise ValueError(f'User {self.username!r} does not exist')

        if user.type not in IAM_SYNCABLE_USER_TYPES:
            raise ValueError(
                f'IAM sync not applicable to {user.type!r} accounts '
                f'(allowed: {list(IAM_SYNCABLE_USER_TYPES)})'
            )

        # Resolve IAM ID and fetch the bundle. `bundle` is either
        # IAMUserPayload (hit) or IAM_MISSING. Users with no prior IAM data
        # whose username does not resolve are also treated as misses — many
        # legacy users have never been synced and need to enter the
        # missing -> offboarding pipeline the same way as a recently-departed
        # user would.
        bundle = await self._resolve_and_fetch(user)

        if bundle is IAM_MISSING:
            await self._handle_miss(user, session)
        else:
            await self._handle_hit(user, bundle, session)

        # Snapshot for describe(): resolve the StatusGroup link to a string
        # so the History entry stays JSON-serializable.
        self._expires_at = user.expires_at
        self._status = await resolve_status_name(user.status) or ''
        if user.iam is not None:
            self._first_missing_at = user.iam.first_missing_at
            self._last_seen_at = user.iam.last_seen_at

        return self._build_result()

    async def _resolve_and_fetch(
        self, user: User,
    ) -> IAMUserPayload | type(IAM_MISSING):
        """Determine the user's iam_id and fetch the IAM bundle.

        Returns IAMUserPayload on a hit, or IAM_MISSING when the user is
        either confirmed gone (200-empty / 404 from get_person) OR has no
        IAM record at all (resolver finds no iam_id). Both paths feed the
        same missing-streak bookkeeping; legacy users without any prior
        IAM data are not distinguished from recently-departed users.
        """
        # Use cached iam_id if we have one.
        if user.iam is not None and user.iam.person is not None:
            iam_id = user.iam.person.iam_id
            self._iam_id = iam_id
            return await self.iam_api.fetch_user_bundle(iam_id)

        # No cached iam_id (either never synced, or last sync's lookup
        # didn't produce a person snapshot). Re-resolve.
        resolved = await self.iam_api.resolve_iam_id_by_username(user.name)
        if resolved is IAM_MISSING:
            return IAM_MISSING
        iam_id = int(resolved['iamId'])
        self._iam_id = iam_id
        return await self.iam_api.fetch_user_bundle(iam_id)

    async def _handle_hit(self, user, bundle, session) -> None:
        # IAM is the source of truth: build a fresh UCDIAMInfo and replace
        # the existing one wholesale (associations, user_types, etc.).
        user.iam = build_ucdiam_info(bundle, now=self.now)

        # Restore from offboarding only if status is still 'offboarding' AND
        # expires_at hasn't passed. Operator-set statuses (inactive, disabled)
        # are NOT auto-resurrected.
        restored = False
        current_status = await resolve_status_name(user.status)
        if (
            user.expires_at is not None
            and user.expires_at > self.now
            and current_status == 'offboarding'
        ):
            user.expires_at = None
            user.status = await find_status_group('active')
            restored = True

        await user.save(session=session)
        self._outcome = 'hit_restored' if restored else 'hit'

    async def _handle_miss(self, user, session) -> None:
        # Always record the missing-state bookkeeping. Even when this is
        # the first sync ever for this user (resolver hit, then get_person
        # 404'd), we still need a UCDIAMInfo so the grace clock starts.
        if user.iam is None:
            user.iam = UCDIAMInfo()
        iam = user.iam
        iam.iam_status = 'missing'
        iam.iam_synced_at = self.now

        if iam.first_missing_at is None:
            iam.first_missing_at = self.now
            self._outcome = 'miss_first'
            await user.save(session=session)
            return

        elapsed = self.now - iam.first_missing_at
        if elapsed >= timedelta(days=self.grace_days):
            # Past grace: set expires_at and flip status, but only if we
            # haven't already set a future expiry (don't clobber operator
            # work).
            if user.expires_at is None or user.expires_at <= self.now:
                user.expires_at = self.now + timedelta(
                    days=self.expiry_offset_days
                )
                current_status = await resolve_status_name(user.status)
                if current_status == 'active':
                    user.status = await find_status_group('offboarding')
                self._outcome = 'miss_offboarding'
            else:
                self._outcome = 'miss_already_expiring'
        else:
            self._outcome = 'miss_within_grace'

        await user.save(session=session)

    def _build_result(self) -> SyncUserIAMResult:
        return SyncUserIAMResult(
            username=self.username,
            outcome=self._outcome,
            iam_id=self._iam_id,
            expires_at=self._expires_at,
            status=self._status,
        )

    def describe(self) -> dict[str, Any]:
        return {
            'username': self.username,
            'outcome': self._outcome,
            'iam_id': self._iam_id,
            'expires_at': _isoformat(self._expires_at),
            'first_missing_at': _isoformat(self._first_missing_at),
            'last_seen_at': _isoformat(self._last_seen_at),
            'status': self._status,
            'grace_days': self.grace_days,
            'expiry_offset_days': self.expiry_offset_days,
        }


# ---------------------------------------------------------------------------
# SyncAllUsersIAM — driver
# ---------------------------------------------------------------------------


_TALLY_KEYS = (
    'hit',
    'hit_restored',
    'miss_first',
    'miss_within_grace',
    'miss_offboarding',
    'miss_already_expiring',
    'transient_error',
    'error',
)


class SyncAllUsersIAM(Operation):
    """Loop SyncUserIAM over candidate users, tallying outcomes.

    Inner SyncUserIAM.run() calls each get their own session/transaction —
    one user failing does not roll back the others. Transient errors are
    counted and skipped.

    The `types` filter is intersected with IAM_SYNCABLE_USER_TYPES; there is
    no way to opt in `system`/`class`/`shared` accounts.
    """

    op_name = 'sync_all_users_iam'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        iam_api: AsyncIAMAPI,
        grace_days: int,
        expiry_offset_days: int,
        types: list[str] | None = None,
        max_users: int | None = None,
        concurrency: int = 1,
        now: datetime | None = None,
    ) -> None:
        super().__init__(client, author)
        self.iam_api = iam_api
        self.grace_days = grace_days
        self.expiry_offset_days = expiry_offset_days
        # Always intersect with IAM_SYNCABLE_USER_TYPES so a caller can
        # narrow but never widen the filter.
        if types is None:
            self.types = list(IAM_SYNCABLE_USER_TYPES)
        else:
            self.types = [t for t in types if t in IAM_SYNCABLE_USER_TYPES]
        self.max_users = max_users
        self.concurrency = max(1, concurrency)
        self.now = _naive_utc(now)
        self._tally: dict[str, int] = {k: 0 for k in _TALLY_KEYS}
        self._total = 0

    async def execute(self, session: AsyncClientSession) -> dict[str, int]:
        query = User.find(In(User.type, self.types))
        if self.max_users is not None:
            query = query.limit(self.max_users)
        users = await query.to_list()
        self._total = len(users)

        if self.concurrency == 1:
            for user in users:
                await self._sync_one(user.name)
        else:
            sem = asyncio.Semaphore(self.concurrency)

            async def _bounded(name: str) -> None:
                async with sem:
                    await self._sync_one(name)

            await asyncio.gather(*(_bounded(u.name) for u in users))

        return dict(self._tally)

    async def _sync_one(self, username: str) -> None:
        try:
            result = await SyncUserIAM.run(
                self.client, self.author,
                username=username,
                iam_api=self.iam_api,
                grace_days=self.grace_days,
                expiry_offset_days=self.expiry_offset_days,
                now=self.now,
            )
        except IAMTransientError as e:
            logger.warning('IAM transient error for %s: %s', username, e)
            self._tally['transient_error'] += 1
            return
        except Exception as e:
            logger.exception('IAM sync failed for %s: %s', username, e)
            self._tally['error'] += 1
            return

        if result.outcome in self._tally:
            self._tally[result.outcome] += 1
        else:
            # Unknown outcome — defensive; means SyncUserIAM grew a new state.
            self._tally.setdefault(result.outcome, 0)
            self._tally[result.outcome] += 1

    def describe(self) -> dict[str, Any]:
        return {
            'total': self._total,
            'tally': dict(self._tally),
            'types': self.types,
            'grace_days': self.grace_days,
            'expiry_offset_days': self.expiry_offset_days,
            'concurrency': self.concurrency,
        }


# ---------------------------------------------------------------------------
# ReapOffboardedUsers — separate flip op
# ---------------------------------------------------------------------------


class ReapOffboardedUsers(Operation):
    """Find users in 'offboarding' status whose expires_at has passed and
    flip them to 'inactive'. No IAM I/O.

    One History entry is written for the operation as a whole (with a list
    of the affected users in describe()). This is intentionally a single
    op rather than per-user History entries so a sweep that reaps many
    users doesn't flood the History collection.
    """

    op_name = 'reap_offboarded_users'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        now: datetime | None = None,
    ) -> None:
        super().__init__(client, author)
        self.now = _naive_utc(now)
        self._reaped: list[str] = []

    async def execute(self, session: AsyncClientSession) -> list[str]:
        offboarding_sg = await find_status_group('offboarding')
        inactive_sg = await find_status_group('inactive')
        if offboarding_sg is None or inactive_sg is None:
            # SeedAccessStatusGroups hasn't been run; nothing can have status
            # 'offboarding' yet, so nothing to reap.
            return []

        users = await User.find(
            User.status.id == offboarding_sg.id,
            LTE(User.expires_at, self.now),
        ).to_list()
        for user in users:
            user.status = inactive_sg
            await user.save(session=session)
            self._reaped.append(user.name)
            logger.info(
                'reaped offboarded user %s (expires_at=%s)',
                user.name, user.expires_at,
            )
        return list(self._reaped)

    def describe(self) -> dict[str, Any]:
        return {
            'reaped_count': len(self._reaped),
            'reaped_users': list(self._reaped),
        }
