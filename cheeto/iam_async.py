"""Async client wrapper around the generated UC Davis IAM API.

This module exists to:
  - Use the `*.asyncio_detailed` variants from `cheeto/iamapi/` directly (no
    `asyncio.to_thread` hops), with one consistent envelope-parsing path.
  - Distinguish *transient* errors (5xx, transport, timeout) from *definitive*
    misses (200-empty, 404). Sync logic uses that distinction to decide whether
    to advance the missing-state bookkeeping on `UCDIAMInfo`.
  - Provide a pure helper (`build_ucdiam_info`) that maps raw IAM JSON into a
    populated `UCDIAMInfo` so the operation layer can stay focused on
    state-machine logic.

Auth is via the `key=` query parameter (matches the v1 client and how the IAM
API is actually wired today).

The v1 hand-written client at `cheeto/iam.py` is intentionally not modified.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from http import HTTPStatus
from typing import Any, Final, NamedTuple

import httpx

from .config import IAMConfig
from .iamapi.api.organization_info_controller import search_ppsbo_us
from .iamapi.api.people_associations_controller import get_pps_assocs_using_iam_id
from .iamapi.api.people_ctlr import (
    get_person_using_iam_id,
    search_contact_info,
    search_pri_kerb_acct,
)
from .iamapi.client import Client
from .models.user import UCDIAMAssociation, UCDIAMInfo, UCDIAMPerson

logger = logging.getLogger(__name__)


class IAMTransientError(Exception):
    """Raised on 5xx, transport, or timeout errors. Callers MUST NOT advance
    missing-state bookkeeping on this; the user's IAM state is unknown."""


class IAMMissing:
    """Sentinel for a definitive 'user not found in IAM' answer (200-empty
    or 404). Distinct from None so callers can match it explicitly."""

    __slots__ = ()

    def __repr__(self) -> str:
        return 'IAM_MISSING'

    def __bool__(self) -> bool:
        return False


IAM_MISSING: Final = IAMMissing()


class IAMUserPayload(NamedTuple):
    """Successful IAM lookup bundle. `divisions` is keyed by `bouOrgOId`."""

    person: dict
    associations: list[dict]
    divisions: dict[str, dict]


def _parse_response(response) -> list[dict] | IAMMissing:
    """Classify a generated-client `Response` into either a list of result
    dicts or `IAM_MISSING`. Raises `IAMTransientError` on 5xx/unexpected.

    Note: an HTTP 200 with `responseData.results == []` counts as a definitive
    miss, not a transient error — that's how the IAM API signals departure.
    """
    sc = response.status_code
    if sc == HTTPStatus.OK:
        try:
            payload = json.loads(response.content)
        except (json.JSONDecodeError, ValueError) as e:
            raise IAMTransientError(
                f'IAM returned 200 with non-JSON body: {e}'
            ) from e
        results = payload.get('responseData', {}).get('results') or []
        return results
    if sc == HTTPStatus.NOT_FOUND:
        return IAM_MISSING
    if 500 <= sc < 600:
        raise IAMTransientError(f'IAM returned {sc}')
    raise IAMTransientError(f'IAM returned unexpected status {sc}')


class AsyncIAMAPI:
    """Async client wrapper. Use as `async with AsyncIAMAPI(config) as api:`.

    The optional `httpx_client` constructor knob lets tests inject an
    `httpx.AsyncClient` backed by `httpx.MockTransport` instead of the real
    network. Production callers leave it None.
    """

    def __init__(
        self,
        config: IAMConfig,
        *,
        httpx_client: httpx.AsyncClient | None = None,
    ) -> None:
        self._client = Client(
            base_url=config.base_url,
            follow_redirects=True,
            timeout=httpx.Timeout(config.request_timeout_seconds),
        )
        if httpx_client is not None:
            self._client.set_async_httpx_client(httpx_client)
        self._key = config.api_key

    async def __aenter__(self) -> AsyncIAMAPI:
        await self._client.__aenter__()
        return self

    async def __aexit__(self, *args, **kwargs) -> None:
        await self._client.__aexit__(*args, **kwargs)

    async def _call(self, fn, /, **kwargs):
        """Invoke a generated `*.asyncio_detailed` and parse the response.

        Translates `httpx.TransportError` and `httpx.TimeoutException` into
        `IAMTransientError`, matching the policy in `_parse_response`.
        """
        try:
            response = await fn(client=self._client, key=self._key, **kwargs)
        except (httpx.TransportError, httpx.TimeoutException) as e:
            raise IAMTransientError(f'IAM transport error: {e}') from e
        return _parse_response(response)

    async def resolve_iam_id_by_username(self, name: str) -> dict | IAMMissing:
        """Look up a user's IAM record by their kerberos username.
        Returns the first hit dict (containing `iamId`), or IAM_MISSING."""
        results = await self._call(
            search_pri_kerb_acct.asyncio_detailed, user_id=name,
        )
        if isinstance(results, IAMMissing):
            return results
        return results[0] if results else IAM_MISSING

    async def resolve_iam_id_by_email(self, email: str) -> dict | IAMMissing:
        """Look up a user's IAM record by email (contact info).
        Returns the first hit dict (containing `iamId`), or IAM_MISSING."""
        results = await self._call(
            search_contact_info.asyncio_detailed, email=email,
        )
        if isinstance(results, IAMMissing):
            return results
        return results[0] if results else IAM_MISSING

    async def get_person(self, iam_id: int) -> dict | IAMMissing:
        """Fetch the full IAM person record. Returns dict or IAM_MISSING."""
        results = await self._call(
            get_person_using_iam_id.asyncio_detailed, iam_id=str(iam_id),
        )
        if isinstance(results, IAMMissing):
            return results
        return results[0] if results else IAM_MISSING

    async def get_pps_associations(self, iam_id: int) -> list[dict]:
        """Fetch PPS departmental associations. An empty list is a normal
        result for some user types (e.g. students) and is NOT a missing
        signal — only `get_person` decides that."""
        results = await self._call(
            get_pps_assocs_using_iam_id.asyncio_detailed, iam_id=str(iam_id),
        )
        if isinstance(results, IAMMissing):
            return []
        return results

    async def get_division(self, org_o_id: str) -> dict | None:
        """Fetch the BOU/division record for a `bouOrgOId`. Returns the first
        hit or None."""
        results = await self._call(
            search_ppsbo_us.asyncio_detailed, org_o_id=org_o_id,
        )
        if isinstance(results, IAMMissing):
            return None
        return results[0] if results else None

    async def fetch_user_bundle(
        self, iam_id: int,
    ) -> IAMUserPayload | IAMMissing:
        """Compose person + associations + per-bouOrgOId divisions in one go.

        Divisions are deduplicated by `bouOrgOId` so multiple associations
        sharing an org only pay one division round-trip.
        """
        person = await self.get_person(iam_id)
        if isinstance(person, IAMMissing):
            return IAM_MISSING
        associations = await self.get_pps_associations(iam_id)

        divisions: dict[str, dict] = {}
        for assoc in associations:
            org_id = assoc.get('bouOrgOId')
            if org_id and org_id not in divisions:
                division = await self.get_division(org_id)
                if division is not None:
                    divisions[org_id] = division
        return IAMUserPayload(
            person=person, associations=associations, divisions=divisions,
        )


# ---------------------------------------------------------------------------
# Pure mapping: raw IAM JSON  ->  UCDIAMInfo
# ---------------------------------------------------------------------------


def _user_types_from_person(person: dict) -> list[str]:
    """Translate the person record's `is*` booleans to our IAM_USER_TYPES.

    Note `'employee'` covers either `isEmployee` or `isHSEmployee` per the
    constant's comment in `cheeto/constants.py`.
    """
    out: list[str] = []
    if person.get('isEmployee') or person.get('isHSEmployee'):
        out.append('employee')
    if person.get('isFaculty'):
        out.append('faculty')
    if person.get('isStaff'):
        out.append('staff')
    if person.get('isStudent'):
        out.append('student')
    if person.get('isExternal'):
        out.append('external')
    return out


def _coerce_int(value: Any) -> int:
    """IAM returns numeric IDs as strings most of the time; tolerate both."""
    if isinstance(value, int):
        return value
    return int(value)


def _build_associations(
    associations: list[dict],
    divisions: dict[str, dict],
) -> list[UCDIAMAssociation]:
    out: list[UCDIAMAssociation] = []
    for assoc in associations:
        org_id = assoc.get('bouOrgOId') or ''
        division = divisions.get(org_id) or {}
        out.append(UCDIAMAssociation(
            org_id=org_id,
            org_name=division.get('deptOfficialName') or '',
            # Keep dept codes as strings — IAM zero-pads them ('072067').
            org_code=str(division.get('deptCode') or ''),
            dept_name=assoc.get('apptDeptOfficialName') or '',
            dept_code=str(assoc.get('apptDeptCode') or ''),
            title=assoc.get('titleOfficialName') or '',
            title_type=assoc.get('emplClassDesc') or '',
        ))
    return out


def build_ucdiam_person(payload: IAMUserPayload) -> UCDIAMPerson:
    """Build the person-snapshot half of UCDIAMInfo. Pure mapping; no I/O."""
    person = payload.person
    return UCDIAMPerson(
        iam_id=_coerce_int(person['iamId']),
        mothra_id=_coerce_int(person['mothraId']),
        user_types=_user_types_from_person(person),
        associations=_build_associations(payload.associations, payload.divisions),
    )


def build_ucdiam_info(
    payload: IAMUserPayload,
    *,
    now: datetime,
) -> UCDIAMInfo:
    """Map a successful IAM lookup bundle into a fresh `UCDIAMInfo`.

    Pure function — no I/O. Sets `iam_status='present'`, captures the person
    snapshot, and stamps `iam_synced_at`/`last_seen_at`. `first_missing_at`
    is cleared so the missing-streak resets when a previously-missing user
    is found again.
    """
    return UCDIAMInfo(
        iam_status='present',
        person=build_ucdiam_person(payload),
        iam_synced_at=now,
        last_seen_at=now,
        first_missing_at=None,
    )
