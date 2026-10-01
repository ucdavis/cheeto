from __future__ import annotations

from typing import Any

from beanie.operators import In
from pymongo import AsyncMongoClient
from pymongo.asynchronous.client_session import AsyncClientSession

from ..models.base import link_target_id
from ..models.group import Group, StatusGroup
from ..models.group_membership import SITE_USER_ROLES, GroupMembership
from ..models.group_site_info import GroupSiteInfo
from ..models.site import Site
from ..models.slurm import SlurmAccount
from ..models.storage import Storage
from ..models.user import User
from ..models.user_site_info import UserSiteInfo
from ..queries.group import resolve_group_names
from .base import Operation
from .group_site import ensure_group_site


async def _find_personal_group(user: User) -> Group | None:
    """The user's personal group (same name, type='user'). May be absent
    for rows predating personal-group creation."""
    return await Group.find_one(Group.name == user.name, Group.type == 'user')


async def ensure_user_site(
    user: User, site: Site, session: AsyncClientSession | None,
    *, status: StatusGroup | None = None,
) -> tuple[UserSiteInfo, bool]:
    """Idempotent find-or-insert of the `(user, site)` presence record.

    Returns `(record, created)`. `status` applies only on insert; the
    default `None` makes the site inherit `User.status`, so an implicit
    add (e.g. via a group role) never re-activates a disabled user. On
    insert the user's personal group follows onto the site so its
    primary-gid group resolves there.

    The find_one must pass `session` for the same reason as
    `ensure_group_site`: inside a transaction, sessionless reads cannot see
    the transaction's own uncommitted inserts. Do NOT catch
    DuplicateKeyError here -- a write error inside a Mongo transaction is
    unrecoverable in-transaction; the unique (user, site) index backstops
    races.
    """
    existing = await UserSiteInfo.find_one(
        UserSiteInfo.user.id == user.id,
        UserSiteInfo.site.id == site.id,
        session=session,
    )
    if existing is not None:
        return existing, False

    usi = UserSiteInfo(user=user, site=site, status=status)
    await usi.insert(session=session)

    personal_group = await _find_personal_group(user)
    if personal_group is not None:
        await ensure_group_site(personal_group, site, session)
    return usi, True


class AddSiteUser(Operation):
    op_name = 'add_site_user'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        user_name: str,
        site_name: str,
    ) -> None:
        super().__init__(client, author)
        self.user_name = user_name
        self.site_name = site_name

    async def execute(self, session: AsyncClientSession) -> UserSiteInfo:
        user = await User.find_one(User.name == self.user_name)
        if user is None:
            raise ValueError(f'User {self.user_name} does not exist')

        site = await Site.find_one(Site.name == self.site_name)
        if site is None:
            raise ValueError(f'Site {self.site_name} does not exist')

        existing = await UserSiteInfo.find_one(
            UserSiteInfo.user.id == user.id,
            UserSiteInfo.site.id == site.id,
        )
        if existing is not None:
            raise ValueError(
                f'User {self.user_name} already on site {self.site_name}'
            )

        # Default new site users to 'active' status if a StatusGroup record
        # exists for it (matches v1 implicit default). Falls back to None
        # gracefully if the seed step hasn't run yet.
        active = await StatusGroup.find_one(StatusGroup.status_name == 'active')
        usi, _ = await ensure_user_site(user, site, session, status=active)

        self._usi = usi
        return usi

    def describe(self) -> dict[str, Any]:
        return {'user': self.user_name, 'site': self.site_name}


class RemoveSiteUser(Operation):
    """Remove a user from a site, with every site-scoped reference to them:
    their `UserSiteInfo`, all their `GroupMembership` edges at the site (any
    role), and their coordinator entries on the site's Slurm accounts.

    Edges are deleted per-document so the LDAP dirty hooks fire. A user with
    orphan edges but no `UserSiteInfo` (pre-backfill data) is still cleaned
    up; only a user with neither is "not on site".
    """

    op_name = 'remove_site_user'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        user_name: str,
        site_name: str,
    ) -> None:
        super().__init__(client, author)
        self.user_name = user_name
        self.site_name = site_name
        self._kept_group_site = False
        self._memberships_deleted = 0
        self._coordinator_accounts: list[str] = []

    async def execute(self, session: AsyncClientSession) -> dict[str, Any]:
        user = await User.find_one(User.name == self.user_name)
        if user is None:
            raise ValueError(f'User {self.user_name} does not exist')

        site = await Site.find_one(Site.name == self.site_name)
        if site is None:
            raise ValueError(f'Site {self.site_name} does not exist')

        usi = await UserSiteInfo.find_one(
            UserSiteInfo.user.id == user.id,
            UserSiteInfo.site.id == site.id,
        )
        edges = await GroupMembership.find(
            GroupMembership.user.id == user.id,
            GroupMembership.site.id == site.id,
        ).to_list()
        if usi is None and not edges:
            raise ValueError(
                f'User {self.user_name} not on site {self.site_name}'
            )

        for edge in edges:
            await edge.delete(session=session)
        self._memberships_deleted = len(edges)

        # No index on coordinators — filtered in Python, as DeleteUser does.
        coordinator_groups = []
        for acct in await SlurmAccount.find(
            SlurmAccount.site.id == site.id,
        ).to_list():
            kept = [
                c for c in acct.coordinators
                if link_target_id(c) != user.id
            ]
            if len(kept) != len(acct.coordinators):
                acct.coordinators = kept
                await acct.save(session=session)
                coordinator_groups.append(acct.group)
        self._coordinator_accounts = await resolve_group_names(
            coordinator_groups,
        )

        if usi is not None:
            await usi.delete(session=session)

        # The personal group leaves with its owner — unless its home
        # storage still exists at the site (rehome/cleanup pending), in
        # which case presence is kept so the export still resolves the gid.
        personal_group = await _find_personal_group(user)
        if personal_group is not None:
            home_storage = await Storage.find_one(
                Storage.group.id == personal_group.id,
                Storage.site.id == site.id,
            )
            if home_storage is not None:
                self._kept_group_site = True
            else:
                gsi = await GroupSiteInfo.find_one(
                    GroupSiteInfo.group.id == personal_group.id,
                    GroupSiteInfo.site.id == site.id,
                )
                if gsi is not None:
                    await gsi.delete(session=session)

        return {
            'memberships_deleted': self._memberships_deleted,
            'coordinator_accounts': self._coordinator_accounts,
        }

    def describe(self) -> dict[str, Any]:
        return {
            'user': self.user_name,
            'site': self.site_name,
            'kept_group_site': self._kept_group_site,
            'memberships_deleted': self._memberships_deleted,
            'coordinator_accounts': self._coordinator_accounts,
        }


class BackfillUserSiteInfo(Operation):
    """Create `UserSiteInfo` records for users who hold a site-implying role
    (`SITE_USER_ROLES`) on a group at a site but have no presence record
    there — the state left by membership ops before they ensured presence,
    and by the legacy group migration. Sponsor-only edges are left alone.
    New records inherit `User.status` (status=None). Safe to re-run.
    """

    op_name = 'backfill_user_site_info'
    # Whole-collection scan across sites; each insert is independently
    # idempotent under the unique (user, site) index, so a bare session is
    # fine and avoids transactionLifetimeLimit on large backfills.
    transactional = False

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        site_name: str | None = None,
        dry_run: bool = False,
    ) -> None:
        super().__init__(client, author)
        self.site_name = site_name
        self.dry_run = dry_run
        self._report: dict[str, dict[str, Any]] = {}

    async def execute(
        self, session: AsyncClientSession,
    ) -> dict[str, dict[str, Any]]:
        if self.site_name is not None:
            site = await Site.find_one(Site.name == self.site_name)
            if site is None:
                raise ValueError(f'Site {self.site_name} does not exist')
            sites = [site]
        else:
            sites = await Site.find_all().to_list()

        report: dict[str, dict[str, Any]] = {}
        for site in sites:
            edges = await GroupMembership.find(
                GroupMembership.site.id == site.id,
                In(GroupMembership.roles, sorted(SITE_USER_ROLES)),
            ).to_list()
            edge_user_ids = {link_target_id(e.user) for e in edges}
            edge_user_ids.discard(None)
            usis = await UserSiteInfo.find(
                UserSiteInfo.site.id == site.id,
            ).to_list()
            existing_ids = {link_target_id(u.user) for u in usis}
            existing_ids.discard(None)
            missing = edge_user_ids - existing_ids

            users = (
                await User.find(In(User.id, list(missing))).to_list()
                if missing else []
            )
            created: list[str] = []
            for user in sorted(users, key=lambda u: u.name):
                if not self.dry_run:
                    await ensure_user_site(user, site, session)
                created.append(user.name)

            report[site.name] = {
                'created': created,
                'existing': len(existing_ids),
            }

        self._report = report
        return report

    def describe(self) -> dict[str, Any]:
        # Counts only — keep History rows bounded on big backfills.
        return {
            'dry_run': self.dry_run,
            'site': self.site_name,
            'sites': {
                name: {
                    'created': len(r['created']),
                    'existing': r['existing'],
                }
                for name, r in self._report.items()
            },
        }
