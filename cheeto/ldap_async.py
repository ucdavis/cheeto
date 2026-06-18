"""Async LDAP wrapper around the bonsai client.

`AsyncLDAPManager` is the public surface. Internally a handful of helpers
(`_add_entry`, `_modify_attrs`, `_patch_member_set`, `_ensure_entry`,
`_delete_dn_idempotent`) absorb the bonsai search/add/modify/delete
boilerplate so the public CRUD methods stay narrative.

LDAP attribute names (`uid`, `mail`, `memberUid`, ...) are written inline
where they're used rather than threaded through a config-driven mapping;
cheeto only targets standard `posixAccount` / `posixGroup` /
`inetOrgPerson` / `ldapPublicKey` / `shadowAccount` schemas.
"""

from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Iterable, TypeVar

import bonsai
from bonsai import LDAPClient, LDAPEntry, LDAPSearchScope
from bonsai.asyncio import AIOConnectionPool

from .config import LDAPConfig

logger = logging.getLogger(__name__)

T = TypeVar('T')


# Cheeto creates a fixed automount layout per site (autofs ghost mounts
# of $HOME and /group/<name>). These names are referenced by both the
# manager (bootstrap, upsert) and operations (prune).
AUTO_MASTER = 'auto.master'
AUTO_HOME = 'auto.home'
AUTO_GROUP = 'auto.group'


# Object-class sets cheeto writes for new entries. shadowAccount lets
# us project User.expires_at as `shadowExpire` (days since epoch).
USER_OBJECT_CLASSES = (
    'top', 'inetOrgPerson', 'posixAccount', 'ldapPublicKey', 'shadowAccount',
)
GROUP_OBJECT_CLASSES = ('top', 'groupOfMembers', 'posixGroup')


# ---------------------------------------------------------------------------
# Exception hierarchy
# ---------------------------------------------------------------------------


class LDAPError(Exception):
    """Base for all LDAP wrapper exceptions."""


class LDAPNotFound(LDAPError):
    pass


class LDAPAlreadyExists(LDAPError):
    pass


class LDAPInvalidUser(LDAPError):
    """Raised when a username can't be resolved to a DN (e.g. for membership)."""


class LDAPCommitFailed(LDAPError):
    pass


class LDAPTransientError(LDAPError):
    """Retryable — network blip, server unavailable. Callers MUST NOT advance
    idempotency state on this; the operation should be retried later."""


class LDAPPruneAborted(LDAPError):
    """Raised when a prune operation would exceed its max_deletions safety
    cap. `would_delete` carries the planned list for caller logging."""

    def __init__(self, message: str, would_delete: dict[str, list[str]]) -> None:
        super().__init__(message)
        self.would_delete = would_delete


def _translate_bonsai_error(exc: Exception) -> LDAPError:
    if isinstance(exc, bonsai.NoSuchObjectError):
        return LDAPNotFound(str(exc))
    if isinstance(exc, bonsai.AlreadyExists):
        return LDAPAlreadyExists(str(exc))
    if isinstance(exc, (bonsai.ConnectionError, asyncio.TimeoutError)):
        return LDAPTransientError(str(exc))
    return LDAPCommitFailed(str(exc))


# ---------------------------------------------------------------------------
# Record dataclasses (no I/O)
# ---------------------------------------------------------------------------


@dataclass
class LDAPUserRecord:
    username: str
    email: str
    uid: int
    gid: int
    fullname: str
    home_directory: str
    shell: str
    surname: str = ''
    ssh_keys: list[str] = field(default_factory=list)
    password: str | None = None
    expires_at: datetime | None = None


@dataclass
class LDAPGroupRecord:
    groupname: str
    gid: int
    members: set[str] = field(default_factory=set)


@dataclass
class LDAPAutomountRecord:
    """`automountInformation` packs host, mount path, and options into one
    string at the wire layer; we keep them split here for ergonomics."""

    mountname: str
    mapname: str
    host: str
    path: str
    options: str = ''


# ---------------------------------------------------------------------------
# Pure DN construction
# ---------------------------------------------------------------------------


# RFC 4514 special characters that need backslash-escaping in DN values.
_DN_ESCAPE_RE = re.compile(r'([,\\+<>;"=])')


def escape_dn_value(value: str) -> str:
    """Escape an RDN attribute value per RFC 4514."""
    if not value:
        return value
    s = _DN_ESCAPE_RE.sub(r'\\\1', value)
    if s.startswith('#') or s.startswith(' '):
        s = '\\' + s
    if s.endswith(' '):
        s = s[:-1] + '\\ '
    return s


@dataclass
class DNBuilder:
    searchbase: str
    user_base: str
    sitename: str

    def __post_init__(self) -> None:
        site_rdn = f'ou={escape_dn_value(self.sitename)}'
        self.site_ou_dn = f'{site_rdn},{self.searchbase}'
        self.groups_ou_dn = f'ou=groups,{self.site_ou_dn}'
        self.automount_ou_dn = f'ou=automount,{self.site_ou_dn}'

    def user_dn(self, username: str) -> str:
        return f'uid={escape_dn_value(username)},{self.user_base}'

    def group_dn(self, groupname: str) -> str:
        return f'cn={escape_dn_value(groupname)},{self.groups_ou_dn}'

    def automount_map_dn(self, mapname: str) -> str:
        return f'automountMapName={escape_dn_value(mapname)},{self.automount_ou_dn}'

    def automount_dn(self, mountname: str, mapname: str) -> str:
        return (
            f'automountKey={escape_dn_value(mountname)},'
            f'{self.automount_map_dn(mapname)}'
        )


# ---------------------------------------------------------------------------
# Entry <-> record mapping (pure)
# ---------------------------------------------------------------------------


def _first(entry: LDAPEntry, attr: str) -> Any:
    """First value of an LDAP attribute on `entry`, or None when absent."""
    values = entry.get(attr)
    return values[0] if values else None


def _days_since_epoch(dt: datetime) -> int:
    """`shadowExpire` is days since 1970-01-01 (RFC 2307). Cheeto stores
    timestamps as naive UTC (see `cheeto/operations/iam.py::_naive_utc`);
    attach UTC explicitly before `.timestamp()` so a naive value isn't
    interpreted as local time."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() // 86400)


def _days_to_naive_utc(days: int) -> datetime:
    return datetime.fromtimestamp(days * 86400, tz=timezone.utc).replace(
        tzinfo=None,
    )


def entry_to_user(entry: LDAPEntry) -> LDAPUserRecord:
    username = _first(entry, 'uid') or ''
    shadow_expire = _first(entry, 'shadowExpire')
    return LDAPUserRecord(
        username=username,
        email=_first(entry, 'mail') or '',
        uid=int(_first(entry, 'uidNumber') or 0),
        gid=int(_first(entry, 'gidNumber') or 0),
        fullname=_first(entry, 'displayName') or _first(entry, 'cn') or '',
        surname=_first(entry, 'sn') or '',
        home_directory=(
            _first(entry, 'homeDirectory') or f'/home/{username}'
        ),
        shell=_first(entry, 'loginShell') or '/usr/bin/bash',
        ssh_keys=list(entry.get('sshPublicKey') or ()),
        expires_at=(
            _days_to_naive_utc(int(shadow_expire))
            if shadow_expire is not None else None
        ),
    )


def user_to_entry_attrs(record: LDAPUserRecord) -> dict[str, list[Any]]:
    out: dict[str, list[Any]] = {
        'objectClass': list(USER_OBJECT_CLASSES),
        'uid': [record.username],
        'mail': [record.email],
        'uidNumber': [record.uid],
        'gidNumber': [record.gid],
        'cn': [record.fullname],
        'displayName': [record.fullname],
        'homeDirectory': [record.home_directory],
        'loginShell': [record.shell],
    }
    if record.surname:
        out['sn'] = [record.surname]
    if record.ssh_keys:
        out['sshPublicKey'] = list(record.ssh_keys)
    if record.password is not None:
        out['userPassword'] = [record.password]
    if record.expires_at is not None:
        # RFC 2307: shadowExpire is days since 1970-01-01, as an int string.
        out['shadowExpire'] = [str(_days_since_epoch(record.expires_at))]
    return out


def entry_to_group(entry: LDAPEntry) -> LDAPGroupRecord:
    return LDAPGroupRecord(
        groupname=_first(entry, 'cn') or '',
        gid=int(_first(entry, 'gidNumber') or 0),
        members=set(entry.get('memberUid') or ()),
    )


def group_to_entry_attrs(record: LDAPGroupRecord) -> dict[str, list[Any]]:
    out: dict[str, list[Any]] = {
        'objectClass': list(GROUP_OBJECT_CLASSES),
        'cn': [record.groupname],
        'gidNumber': [record.gid],
    }
    if record.members:
        out['memberUid'] = sorted(record.members)
    return out


def _automount_info(record: LDAPAutomountRecord) -> str:
    if record.options:
        return f'{record.options} {record.host}${{HOST_SUFFIX}}:{record.path}'.strip()
    return f'{record.host}${{HOST_SUFFIX}}:{record.path}'


def automount_to_entry_attrs(record: LDAPAutomountRecord) -> dict[str, list[Any]]:
    return {
        'objectClass': ['automount'],
        'automountKey': [record.mountname],
        'automountInformation': [_automount_info(record)],
    }


# ---------------------------------------------------------------------------
# Client factory
# ---------------------------------------------------------------------------


class LDAPClientFactory:
    """Builds configured bonsai LDAPClient instances for a given LDAPConfig.
    Split from the manager so tests can inject (e.g.) a slapd-bound client."""

    def __init__(self, config: LDAPConfig) -> None:
        self.config = config

    def build_client(self) -> LDAPClient:
        # bonsai accepts only one URL per client; we use the first today.
        cli = LDAPClient(self.config.servers[0], tls=self.config.use_tls)
        if self.config.auth_mechanism == 'SIMPLE':
            cli.set_credentials(
                'SIMPLE',
                user=self.config.login_dn,
                password=self.config.password,
            )
        elif self.config.auth_mechanism == 'GSSAPI':
            cli.set_credentials(
                'GSSAPI',
                user=self.config.login_dn,
                password=self.config.password,
                realm=self.config.gssapi_realm,
                keytab=self.config.gssapi_keytab,
            )
        else:
            raise LDAPError(
                f'Unsupported auth_mechanism {self.config.auth_mechanism!r}'
            )
        return cli


# ---------------------------------------------------------------------------
# Conn-bound entry fetcher (runs inside _pooled_op closures)
# ---------------------------------------------------------------------------


async def _fetch_entry_at_dn(
    conn: Any, dn: str, *, timeout: float,
    attrlist: list[str] | None = None,
) -> LDAPEntry | None:
    """BASE-scope fetch returning the entry or None when absent. Translates
    `bonsai.NoSuchObjectError` to None so callers can branch flat."""
    try:
        results = await conn.search(
            dn, LDAPSearchScope.BASE, '(objectClass=*)',
            attrlist=attrlist, timeout=timeout,
        )
    except bonsai.NoSuchObjectError:
        return None
    return results[0] if results else None


# ---------------------------------------------------------------------------
# AsyncLDAPManager
# ---------------------------------------------------------------------------


class AsyncLDAPManager:
    """Site-scoped async LDAP CRUD wrapper.

        async with AsyncLDAPManager(config, sitename='farm') as mgr:
            await mgr.add_user(record)

    Owns an `AIOConnectionPool` and runs every operation through
    `_pooled_op`, which encapsulates the bounded retry loop required by
    bonsai's lack of stale-connection detection.
    """

    def __init__(
        self,
        config: LDAPConfig,
        *,
        sitename: str,
        client_factory: LDAPClientFactory | None = None,
    ) -> None:
        if not config.user_base:
            raise LDAPError(
                'LDAPConfig.user_base is required for the async manager'
            )
        self.config = config
        self.sitename = sitename
        self._factory = client_factory or LDAPClientFactory(config)
        self.dn = DNBuilder(
            searchbase=config.searchbase,
            user_base=config.user_base,
            sitename=sitename,
        )
        self._pool: AIOConnectionPool | None = None

    async def __aenter__(self) -> 'AsyncLDAPManager':
        client = self._factory.build_client()
        self._pool = AIOConnectionPool(
            client,
            minconn=self.config.pool_idle_connections,
            maxconn=self.config.pool_max_connections,
        )
        await self._pool.open()
        return self

    async def __aexit__(self, *exc) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    @property
    def pool(self) -> AIOConnectionPool:
        if self._pool is None:
            raise LDAPError(
                'AsyncLDAPManager pool not open; use `async with` to manage it'
            )
        return self._pool

    @property
    def _timeout(self) -> float:
        return self.config.request_timeout_seconds

    # -----------------------------------------------------------------
    # Pooled-op + entry helpers
    # -----------------------------------------------------------------

    async def _pooled_op(
        self, op: Callable[[Any], Awaitable[T]],
    ) -> T:
        """Run `op` against a pooled connection with bounded retry on
        transient failures. Bonsai does not detect stale connections, so on
        ConnectionError/TimeoutError we close the conn and try a fresh one
        up to `pool.idle_connection + 1` times before giving up."""
        pool = self.pool
        attempts = max(1, pool.idle_connection + 1)
        last_exc: Exception | None = None
        for _ in range(attempts):
            try:
                async with pool.spawn() as conn:
                    try:
                        return await op(conn)
                    except (bonsai.ConnectionError, asyncio.TimeoutError) as e:
                        conn.close()
                        last_exc = e
                        continue
                    except bonsai.LDAPError as e:
                        raise _translate_bonsai_error(e) from e
            except (bonsai.ConnectionError, asyncio.TimeoutError) as e:
                last_exc = e
                continue
        raise LDAPTransientError('LDAP pool exhausted on retry') from last_exc

    async def _add_entry(
        self, dn: str, attrs: dict[str, list[Any]],
    ) -> None:
        async def _op(conn):
            entry = LDAPEntry(dn)
            for k, v in attrs.items():
                entry[k] = v
            try:
                await conn.add(entry, timeout=self._timeout)
            except bonsai.LDAPError as e:
                # Surface which DN failed — bonsai's error message alone is
                # ambiguous when multiple adds happen in a single op.
                raise type(e)(f'{e} (adding dn={dn!r})') from e
        await self._pooled_op(_op)

    async def _delete_dn_idempotent(self, dn: str) -> bool:
        """Return True on delete, False when the DN was already absent."""
        async def _op(conn):
            try:
                await conn.delete(dn, timeout=self._timeout)
                return True
            except bonsai.NoSuchObjectError:
                return False
        return await self._pooled_op(_op)

    async def _modify_attrs(
        self, dn: str, attrs: dict[str, list[Any]], *,
        missing_msg: str,
    ) -> None:
        """Patch attributes on the existing entry at `dn`. Empty `attrs`
        no-ops. Raises `LDAPNotFound(missing_msg)` if the entry is gone."""
        if not attrs:
            return

        async def _op(conn):
            entry = await _fetch_entry_at_dn(
                conn, dn, timeout=self._timeout,
            )
            if entry is None:
                raise LDAPNotFound(missing_msg)
            for k, v in attrs.items():
                entry[k] = v
            await entry.modify()
        await self._pooled_op(_op)

    async def _patch_member_set(
        self, dn: str, members_attr: str, *,
        add: Iterable[str] = (),
        remove: Iterable[str] = (),
        replace: Iterable[str] | None = None,
        missing_msg: str,
    ) -> None:
        """Diff-patch a multi-valued attribute (`memberUid`). Skips modify()
        when the resulting set equals the current. Raises
        `LDAPNotFound(missing_msg)` if the entry is gone."""
        add_set = set(add)
        remove_set = set(remove)

        async def _op(conn):
            entry = await _fetch_entry_at_dn(
                conn, dn, timeout=self._timeout,
            )
            if entry is None:
                raise LDAPNotFound(missing_msg)
            current = set(entry.get(members_attr) or [])
            if replace is not None:
                new = set(replace)
            else:
                new = (current | add_set) - remove_set
            if new == current:
                return
            entry[members_attr] = sorted(new)
            await entry.modify()
        await self._pooled_op(_op)

    async def _ensure_entry(
        self, dn: str, attrs: dict[str, list[Any]],
    ) -> str:
        """Add the entry if missing; return `'created'` or
        `'already_exists'`. Used by bootstrap helpers; lets us skip a
        pre-check round-trip per entry."""
        try:
            await self._add_entry(dn, attrs)
            return 'created'
        except LDAPAlreadyExists:
            return 'already_exists'

    # -----------------------------------------------------------------
    # DN helpers (pass through to DNBuilder)
    # -----------------------------------------------------------------

    def user_ou_dn(self) -> str:
        return self.dn.user_base

    def user_dn(self, username: str) -> str:
        return self.dn.user_dn(username)

    def group_dn(self, groupname: str) -> str:
        return self.dn.group_dn(groupname)

    def automount_dn(self, mountname: str, mapname: str) -> str:
        return self.dn.automount_dn(mountname, mapname)

    def site_ou_dn(self) -> str:
        return self.dn.site_ou_dn

    def groups_ou_dn(self) -> str:
        return self.dn.groups_ou_dn

    def automount_ou_dn(self) -> str:
        return self.dn.automount_ou_dn

    def automount_map_dn(self, mapname: str) -> str:
        return self.dn.automount_map_dn(mapname)

    # -----------------------------------------------------------------
    # Existence + queries
    # -----------------------------------------------------------------

    async def dn_exists(self, dn: str) -> bool:
        async def _op(conn):
            entry = await _fetch_entry_at_dn(
                conn, dn, timeout=self._timeout, attrlist=['objectClass'],
            )
            return entry is not None
        return await self._pooled_op(_op)

    async def user_exists(self, username: str) -> bool:
        return await self.dn_exists(self.user_dn(username))

    async def group_exists(self, groupname: str) -> bool:
        return await self.dn_exists(self.group_dn(groupname))

    async def get_user(self, username: str) -> LDAPUserRecord | None:
        async def _op(conn):
            entry = await _fetch_entry_at_dn(
                conn, self.user_dn(username), timeout=self._timeout,
            )
            return entry_to_user(entry) if entry is not None else None
        return await self._pooled_op(_op)

    async def get_group(self, groupname: str) -> LDAPGroupRecord | None:
        async def _op(conn):
            entry = await _fetch_entry_at_dn(
                conn, self.group_dn(groupname), timeout=self._timeout,
            )
            return entry_to_group(entry) if entry is not None else None
        return await self._pooled_op(_op)

    async def list_users(self) -> list[LDAPUserRecord]:
        """All users under user_base. Useful for prune diff."""
        async def _op(conn):
            results = await conn.search(
                self.config.user_base, LDAPSearchScope.ONELEVEL,
                '(objectClass=posixAccount)', timeout=self._timeout,
            )
            return [entry_to_user(e) for e in results]
        return await self._pooled_op(_op)

    async def list_groups(self) -> list[LDAPGroupRecord]:
        """All groups under groups_ou for this site. Useful for prune diff."""
        async def _op(conn):
            try:
                results = await conn.search(
                    self.groups_ou_dn(), LDAPSearchScope.ONELEVEL,
                    '(objectClass=posixGroup)', timeout=self._timeout,
                )
            except bonsai.NoSuchObjectError:
                return []
            return [entry_to_group(e) for e in results]
        return await self._pooled_op(_op)

    async def list_user_memberships(self, username: str) -> set[str]:
        """Set of cn= values where memberUid=<username> at this site."""
        async def _op(conn):
            try:
                results = await conn.search(
                    self.groups_ou_dn(), LDAPSearchScope.ONELEVEL,
                    f'(memberUid={username})',
                    attrlist=['cn'], timeout=self._timeout,
                )
            except bonsai.NoSuchObjectError:
                return set()
            return {v for e in results if (v := _first(e, 'cn')) is not None}
        return await self._pooled_op(_op)

    async def list_automounts(self, mapname: str) -> list[str]:
        """Return automountKey values under the given map. Useful for prune."""
        async def _op(conn):
            try:
                results = await conn.search(
                    self.automount_map_dn(mapname),
                    LDAPSearchScope.ONELEVEL, '(objectClass=automount)',
                    attrlist=['automountKey'], timeout=self._timeout,
                )
            except bonsai.NoSuchObjectError:
                return []
            return [
                v for e in results
                if (v := _first(e, 'automountKey')) is not None
            ]
        return await self._pooled_op(_op)

    async def list_subtree_dns(
        self, base: str | None = None, *,
        exclude_bases: Iterable[str] = (),
    ) -> list[str]:
        """All DNs under `base` (default: searchbase), excluding any DN
        within `exclude_bases` (suffix match, case-insensitive). Returned
        list is sorted leaf-first so a linear pass can delete children
        before their parents."""
        base = base or self.config.searchbase
        excludes_lower = [b.lower() for b in exclude_bases]

        async def _op(conn):
            try:
                results = await conn.search(
                    base, LDAPSearchScope.SUB, '(objectClass=*)',
                    # 1.1 = no attributes, DNs only.
                    attrlist=['1.1'], timeout=self._timeout,
                )
            except bonsai.NoSuchObjectError:
                return []
            return [str(e.dn) for e in results]

        all_dns = await self._pooled_op(_op)
        base_lower = base.lower()
        kept: list[str] = []
        for dn in all_dns:
            dn_lower = dn.lower()
            if dn_lower == base_lower:
                continue
            if any(
                dn_lower == eb or dn_lower.endswith(',' + eb)
                for eb in excludes_lower
            ):
                continue
            kept.append(dn)
        kept.sort(key=lambda d: -d.count(','))
        return kept

    # -----------------------------------------------------------------
    # User CRUD
    # -----------------------------------------------------------------

    async def add_user(self, record: LDAPUserRecord) -> None:
        await self._add_entry(
            self.user_dn(record.username), user_to_entry_attrs(record),
        )

    async def update_user(self, record: LDAPUserRecord) -> None:
        """Patch the user dn to match `record`. Writes everything `add_user`
        would write minus `objectClass`. Raises `LDAPNotFound` if absent."""
        attrs = user_to_entry_attrs(record)
        attrs.pop('objectClass', None)
        await self._modify_attrs(
            self.user_dn(record.username), attrs,
            missing_msg=f'No such user: {record.username}',
        )

    async def delete_user(self, username: str) -> None:
        """Idempotent."""
        await self._delete_dn_idempotent(self.user_dn(username))

    # -----------------------------------------------------------------
    # Group CRUD
    # -----------------------------------------------------------------

    async def add_group(self, record: LDAPGroupRecord) -> None:
        await self._add_entry(
            self.group_dn(record.groupname), group_to_entry_attrs(record),
        )

    async def delete_group(self, groupname: str) -> None:
        await self._delete_dn_idempotent(self.group_dn(groupname))

    async def add_users_to_group(
        self, groupname: str, usernames: Iterable[str],
        *, verify_users: bool = True,
    ) -> None:
        """Idempotent: members already present are silently skipped."""
        usernames = list(usernames)
        if not usernames:
            return
        if verify_users:
            for u in usernames:
                if not await self.user_exists(u):
                    raise LDAPInvalidUser(f'User does not exist in LDAP: {u}')
        await self._patch_member_set(
            self.group_dn(groupname), 'memberUid',
            add=usernames,
            missing_msg=f'No such group: {groupname}',
        )

    async def remove_users_from_group(
        self, groupname: str, usernames: Iterable[str],
    ) -> None:
        usernames = set(usernames)
        if not usernames:
            return
        await self._patch_member_set(
            self.group_dn(groupname), 'memberUid',
            remove=usernames,
            missing_msg=f'No such group: {groupname}',
        )

    async def set_group_members(
        self, groupname: str, usernames: set[str],
    ) -> None:
        """Replace the group's members with the given set."""
        await self._patch_member_set(
            self.group_dn(groupname), 'memberUid',
            replace=usernames,
            missing_msg=f'No such group: {groupname}',
        )

    # -----------------------------------------------------------------
    # Automount CRUD
    # -----------------------------------------------------------------

    async def add_automount(self, record: LDAPAutomountRecord) -> None:
        await self._add_entry(
            self.automount_dn(record.mountname, record.mapname),
            automount_to_entry_attrs(record),
        )

    async def delete_automount(self, mountname: str, mapname: str) -> None:
        await self._delete_dn_idempotent(self.automount_dn(mountname, mapname))

    async def upsert_automount(
        self, mountname: str, mapname: str,
        host: str, path: str, options: str = '',
    ) -> None:
        """Create-or-replace `automountKey=<mountname>` in `mapname`.
        Delete-then-add is safer than modify under bonsai (no schema
        check on partial entries)."""
        await self.delete_automount(mountname, mapname)
        await self.add_automount(LDAPAutomountRecord(
            mountname=mountname, mapname=mapname,
            host=host, path=path, options=options,
        ))

    # -----------------------------------------------------------------
    # Generic DN delete (for ClearLDAPTree)
    # -----------------------------------------------------------------

    async def delete_dn(self, dn: str, *, missing_ok: bool = True) -> bool:
        """Delete a single DN. Returns True if deleted, False when the
        DN was already absent and `missing_ok=True`."""
        async def _op(conn):
            try:
                await conn.delete(dn, timeout=self._timeout)
                return True
            except bonsai.NoSuchObjectError:
                if missing_ok:
                    return False
                raise
        return await self._pooled_op(_op)

    # -----------------------------------------------------------------
    # Bootstrap helpers
    # -----------------------------------------------------------------

    async def ensure_site_tree(self) -> dict[str, str]:
        """Create the per-site OU tree, automount maps, and /home + /group
        master keys if absent. Idempotent.
        Returns `{dn: 'created' | 'already_exists'}`."""
        plan: list[tuple[str, dict[str, list[Any]]]] = [
            (self.user_ou_dn(),
             {'objectClass': ['organizationalUnit', 'top'], 'ou': ['users']}),
            (self.site_ou_dn(),
             {'objectClass': ['organizationalUnit', 'top'], 'ou': [self.sitename]}),
            (self.groups_ou_dn(),
             {'objectClass': ['organizationalUnit', 'top'], 'ou': ['groups']}),
            (self.automount_ou_dn(),
             {'objectClass': ['organizationalUnit', 'top'], 'ou': ['automount']}),
        ]
        for mapname in (AUTO_MASTER, AUTO_HOME, AUTO_GROUP):
            plan.append((self.automount_map_dn(mapname), {
                'objectClass': ['automountMap', 'top'],
                'automountMapName': [mapname],
            }))
        home_map_dn = self.automount_map_dn(AUTO_HOME)
        group_map_dn = self.automount_map_dn(AUTO_GROUP)
        for key, info in (
            ('/home', home_map_dn),
            ('/group', f'{group_map_dn} --ghost'),
        ):
            plan.append((self.dn.automount_dn(key, AUTO_MASTER), {
                'objectClass': ['automount', 'top'],
                'automountKey': [key],
                'automountInformation': [info],
            }))

        result: dict[str, str] = {}
        for dn, attrs in plan:
            result[dn] = await self._ensure_entry(dn, attrs)
        return result

    async def ensure_special_groups(
        self, groups: list[LDAPGroupRecord],
    ) -> dict[str, str]:
        """Create posixGroup entries for each record; idempotent.
        Returns `{groupname: 'created' | 'already_exists'}`."""
        out: dict[str, str] = {}
        for record in groups:
            attrs = group_to_entry_attrs(record)
            print(attrs)
            out[record.groupname] = await self._ensure_entry(
                self.group_dn(record.groupname), attrs,
            )
        return out
