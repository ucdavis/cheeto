"""Async LDAP wrapper around the bonsai client.

Replaces the v1 hand-written `cheeto/ldap.py` (ldap3-based, sync) with a
clean async layer. The shape mirrors `cheeto/iam_async.py`:

- `LDAPClientFactory` constructs configured `bonsai.LDAPClient` instances.
- `AsyncLDAPManager` is the public surface — owns an `AIOConnectionPool`,
  threads every operation through `_pooled_op` for stale-connection retry,
  exposes typed CRUD methods that return / accept pydantic record dataclasses.
- `DNBuilder` is a pure helper for site-scoped DN construction.
- Pure mapping functions (`entry_to_*`, `*_to_entry_attrs`) replace the
  marshmallow `LDAPRecord.from_entry` magic from v1; they're trivially
  unit-testable without a network.
- Wrapper exceptions translate bonsai's exception hierarchy into a smaller,
  semantic surface (`LDAPNotFound`, `LDAPAlreadyExists`, etc.).

The v1 `cheeto/ldap.py` and `cheeto/database/ldap.py` stay untouched until
cutover; both stacks coexist.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Iterable, Mapping, TypeVar

import bonsai
from bonsai import LDAPClient, LDAPEntry, LDAPSearchScope
from bonsai.asyncio import AIOConnectionPool

from .config import LDAPConfig

logger = logging.getLogger(__name__)

T = TypeVar('T')


# ---------------------------------------------------------------------------
# Exception hierarchy
# ---------------------------------------------------------------------------


class LDAPError(Exception):
    """Base for all LDAP wrapper exceptions."""


class LDAPNotFound(LDAPError):
    """Raised when an expected LDAP entry doesn't exist."""


class LDAPAlreadyExists(LDAPError):
    """Raised when attempting to create an entry whose DN is taken."""


class LDAPInvalidUser(LDAPError):
    """Raised when a username can't be resolved to a DN (e.g. for membership)."""


class LDAPCommitFailed(LDAPError):
    """Raised when an add/modify/delete fails for a non-classified reason."""


class LDAPTransientError(LDAPError):
    """Raised when an operation is retryable — network blip, server unavailable.

    Callers MUST NOT advance idempotency state on this; the operation should
    be retried later.
    """


class LDAPPruneAborted(LDAPError):
    """Raised when a prune operation would exceed its max_deletions safety cap.

    Carries the would-delete list as `.would_delete` so the caller can log
    it before exiting.
    """

    def __init__(self, message: str, would_delete: dict[str, list[str]]) -> None:
        super().__init__(message)
        self.would_delete = would_delete


def _translate_bonsai_error(exc: Exception) -> LDAPError:
    """Translate a bonsai exception into our semantic wrapper."""
    if isinstance(exc, bonsai.NoSuchObjectError):
        return LDAPNotFound(str(exc))
    if isinstance(exc, bonsai.AlreadyExists):
        return LDAPAlreadyExists(str(exc))
    if isinstance(exc, (bonsai.ConnectionError, asyncio.TimeoutError)):
        return LDAPTransientError(str(exc))
    if isinstance(exc, bonsai.LDAPError):
        return LDAPCommitFailed(str(exc))
    return LDAPCommitFailed(str(exc))


# ---------------------------------------------------------------------------
# Pure record dataclasses (no I/O, no pool, no connection)
# ---------------------------------------------------------------------------


@dataclass
class LDAPUserRecord:
    """In-memory representation of an LDAP user entry. Uses the canonical
    Python field names; the LDAP attribute names are looked up at
    serialization time via `LDAPConfig.user_attrs`."""

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
    """In-memory representation of an LDAP posixGroup entry."""

    groupname: str
    gid: int
    members: set[str] = field(default_factory=set)


@dataclass
class LDAPAutomountRecord:
    """In-memory representation of an LDAP automount entry. The LDAP attr
    `automountInformation` packs host, mount path, and options into one
    string; we keep them split here for ergonomics."""

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
    """Escape an RDN attribute value per RFC 4514. Bonsai's API leaves DN
    component escaping to the caller; this is the seam."""
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
    """Pure DN builder for a (config, sitename) pair. Memoizes the OU-level
    DNs at construction; per-call DNs are computed on demand."""

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
# Pure mapping: bonsai LDAPEntry  <->  record dataclass
# ---------------------------------------------------------------------------


# Field names in our records that are multi-valued at the LDAP layer.
_USER_SEQUENCE_FIELDS = frozenset({'ssh_keys'})


def _entry_value(entry: LDAPEntry, attr: str, multi: bool) -> Any:
    """Pull `attr` out of a bonsai entry, returning a list when `multi` is
    True or the first value (or None) when False."""
    values = entry.get(attr)
    if values is None:
        return [] if multi else None
    if multi:
        return list(values)
    return values[0] if values else None


def entry_to_user(
    entry: LDAPEntry,
    user_attrs: Mapping[str, str],
) -> LDAPUserRecord:
    """Map a bonsai LDAPEntry to an LDAPUserRecord using
    `LDAPConfig.user_attrs` (canonical_field -> ldap_attr)."""
    raw: dict[str, Any] = {}
    for py_field, ldap_attr in user_attrs.items():
        multi = py_field in _USER_SEQUENCE_FIELDS
        value = _entry_value(entry, ldap_attr, multi)
        if value is None:
            continue
        raw[py_field] = value
    # `user_attrs` uses 'username' but our dataclass uses the same field;
    # `int` coercion for uid/gid (bonsai returns strings sometimes).
    if 'uid' in raw and not isinstance(raw['uid'], int):
        raw['uid'] = int(raw['uid'])
    if 'gid' in raw and not isinstance(raw['gid'], int):
        raw['gid'] = int(raw['gid'])
    return LDAPUserRecord(**{
        # Defaults for any field not in user_attrs.
        'home_directory': raw.pop('home_directory', f'/home/{raw.get("username", "")}'),
        'shell': raw.pop('shell', '/usr/bin/bash'),
        **raw,
    })


def user_to_entry_attrs(
    record: LDAPUserRecord,
    user_attrs: Mapping[str, str],
    object_classes: list[str],
) -> dict[str, list[Any]]:
    """Project an LDAPUserRecord into the dict-of-lists shape bonsai accepts.

    Bonsai treats every attribute as a multi-valued list at the wire layer;
    we pass single-value attrs as one-element lists.
    """
    out: dict[str, list[Any]] = {
        'objectClass': list(object_classes),
        'cn': [record.fullname],
    }
    for py_field, ldap_attr in user_attrs.items():
        value = getattr(record, py_field, None)
        if value is None or value == '':
            continue
        if isinstance(value, (list, set)):
            if not value:
                continue
            out[ldap_attr] = list(value)
        else:
            out[ldap_attr] = [value]
    return out


def entry_to_group(
    entry: LDAPEntry,
    group_attrs: Mapping[str, str],
) -> LDAPGroupRecord:
    """Map a bonsai LDAPEntry to an LDAPGroupRecord using
    `LDAPConfig.group_attrs` (canonical_field -> ldap_attr)."""
    name_attr = group_attrs['groupname']
    gid_attr = group_attrs['gid']
    members_attr = group_attrs['members']

    name_v = _entry_value(entry, name_attr, multi=False)
    gid_v = _entry_value(entry, gid_attr, multi=False)
    members_v = _entry_value(entry, members_attr, multi=True)

    return LDAPGroupRecord(
        groupname=str(name_v) if name_v is not None else '',
        gid=int(gid_v) if gid_v is not None else 0,
        members=set(members_v) if members_v else set(),
    )


def group_to_entry_attrs(
    record: LDAPGroupRecord,
    group_attrs: Mapping[str, str],
    object_classes: list[str],
) -> dict[str, list[Any]]:
    out: dict[str, list[Any]] = {
        'objectClass': list(object_classes),
        group_attrs['groupname']: [record.groupname],
        group_attrs['gid']: [record.gid],
    }
    if record.members:
        out[group_attrs['members']] = list(record.members)
    return out


def automount_to_entry_attrs(record: LDAPAutomountRecord) -> dict[str, list[Any]]:
    """Build the bonsai attrs for an automount entry. The
    `automountInformation` value packs options + host:path."""
    info = (
        f'{record.options} {record.host}:{record.path}'.strip()
        if record.options
        else f'{record.host}:{record.path}'
    )
    return {
        'objectClass': ['automount'],
        'automountKey': [record.mountname],
        'automountInformation': [info],
    }


# ---------------------------------------------------------------------------
# Client factory
# ---------------------------------------------------------------------------


class LDAPClientFactory:
    """Builds configured bonsai LDAPClient instances for a given LDAPConfig.

    Pulled out as a separate class so test code can override (e.g., to
    inject a slapd-bound client without going through config).
    """

    def __init__(self, config: LDAPConfig) -> None:
        self.config = config

    def build_client(self) -> LDAPClient:
        # bonsai accepts only one URL per client; the config carries a list
        # for redundancy but we use the first today. Future iteration may
        # rotate through them.
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
# AsyncLDAPManager
# ---------------------------------------------------------------------------


class AsyncLDAPManager:
    """Site-scoped async LDAP CRUD wrapper.

    Use as a context manager:

        async with AsyncLDAPManager(config, sitename='farm') as mgr:
            await mgr.add_user(record)

    Owns an `AIOConnectionPool` and runs every operation through `_pooled_op`,
    which encapsulates the bounded retry loop required by bonsai's lack of
    stale-connection detection.
    """

    def __init__(
        self,
        config: LDAPConfig,
        *,
        sitename: str,
        client_factory: LDAPClientFactory | None = None,
    ) -> None:
        self.config = config
        self.sitename = sitename
        self._factory = client_factory or LDAPClientFactory(config)
        if not config.user_base:
            raise LDAPError(
                'LDAPConfig.user_base is required for the async manager'
            )
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

    # -----------------------------------------------------------------
    # Pooled-op helper
    # -----------------------------------------------------------------

    async def _pooled_op(
        self, op: Callable[[Any], Awaitable[T]],
    ) -> T:
        """Run `op` against a pooled connection with bounded retry on
        transient failures. Bonsai does not detect stale connections, so we
        catch ConnectionError/TimeoutError, close the connection (forces the
        pool to replace it), and try again up to `pool.idle_connection + 1`
        times before giving up with LDAPTransientError.

        Other LDAP errors (NoSuchObject, AlreadyExists, etc.) are translated
        once at the boundary and re-raised.
        """
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
            try:
                results = await conn.search(
                    dn, LDAPSearchScope.BASE, '(objectClass=*)',
                    attrlist=['objectClass'],
                    timeout=self.config.request_timeout_seconds,
                )
                return bool(results)
            except bonsai.NoSuchObjectError:
                return False
        return await self._pooled_op(_op)

    async def user_exists(self, username: str) -> bool:
        return await self.dn_exists(self.user_dn(username))

    async def group_exists(self, groupname: str) -> bool:
        return await self.dn_exists(self.group_dn(groupname))

    async def get_user(self, username: str) -> LDAPUserRecord | None:
        async def _op(conn):
            try:
                results = await conn.search(
                    self.user_dn(username), LDAPSearchScope.BASE,
                    '(objectClass=*)',
                    timeout=self.config.request_timeout_seconds,
                )
            except bonsai.NoSuchObjectError:
                return None
            if not results:
                return None
            return entry_to_user(results[0], self.config.user_attrs)
        return await self._pooled_op(_op)

    async def get_group(self, groupname: str) -> LDAPGroupRecord | None:
        if not self.config.group_attrs:
            raise LDAPError('LDAPConfig.group_attrs is required')

        async def _op(conn):
            try:
                results = await conn.search(
                    self.group_dn(groupname), LDAPSearchScope.BASE,
                    '(objectClass=*)',
                    timeout=self.config.request_timeout_seconds,
                )
            except bonsai.NoSuchObjectError:
                return None
            if not results:
                return None
            return entry_to_group(results[0], self.config.group_attrs)
        return await self._pooled_op(_op)

    async def list_users(self) -> list[LDAPUserRecord]:
        """All users under user_base. Useful for prune diff."""
        async def _op(conn):
            results = await conn.search(
                self.config.user_base, LDAPSearchScope.ONELEVEL,
                '(objectClass=posixAccount)',
                timeout=self.config.request_timeout_seconds,
            )
            return [entry_to_user(e, self.config.user_attrs) for e in results]
        return await self._pooled_op(_op)

    async def list_groups(self) -> list[LDAPGroupRecord]:
        """All groups under groups_ou for this site. Useful for prune diff."""
        if not self.config.group_attrs:
            raise LDAPError('LDAPConfig.group_attrs is required')

        async def _op(conn):
            try:
                results = await conn.search(
                    self.groups_ou_dn(), LDAPSearchScope.ONELEVEL,
                    '(objectClass=posixGroup)',
                    timeout=self.config.request_timeout_seconds,
                )
            except bonsai.NoSuchObjectError:
                return []
            return [entry_to_group(e, self.config.group_attrs) for e in results]
        return await self._pooled_op(_op)

    async def list_user_memberships(self, username: str) -> set[str]:
        """Set of cn= values where memberUid=<username> at this site."""
        if not self.config.group_attrs:
            raise LDAPError('LDAPConfig.group_attrs is required')

        async def _op(conn):
            try:
                results = await conn.search(
                    self.groups_ou_dn(), LDAPSearchScope.ONELEVEL,
                    f'({self.config.group_attrs["members"]}={username})',
                    attrlist=[self.config.group_attrs['groupname']],
                    timeout=self.config.request_timeout_seconds,
                )
            except bonsai.NoSuchObjectError:
                return set()
            cn_attr = self.config.group_attrs['groupname']
            return {
                e[cn_attr][0] for e in results
                if e.get(cn_attr)
            }
        return await self._pooled_op(_op)

    # -----------------------------------------------------------------
    # User CRUD
    # -----------------------------------------------------------------

    async def add_user(self, record: LDAPUserRecord) -> None:
        attrs = user_to_entry_attrs(
            record, self.config.user_attrs, self.config.user_classes,
        )
        dn = self.user_dn(record.username)

        async def _op(conn):
            entry = LDAPEntry(dn)
            for k, v in attrs.items():
                entry[k] = v
            await conn.add(entry, timeout=self.config.request_timeout_seconds)
        await self._pooled_op(_op)

    async def update_user(self, username: str, **fields: Any) -> None:
        """Patch attributes on an existing user. Field names are canonical
        Python names from `LDAPConfig.user_attrs`; only those listed in the
        config map are accepted (others are ignored with a warning)."""
        if not fields:
            return

        attr_map: dict[str, list[Any]] = {}
        for py_field, value in fields.items():
            ldap_attr = self.config.user_attrs.get(py_field)
            if ldap_attr is None:
                logger.warning(
                    'update_user(%s): field %r not in user_attrs map; ignoring',
                    username, py_field,
                )
                continue
            if value is None:
                attr_map[ldap_attr] = []
            elif isinstance(value, (list, set)):
                attr_map[ldap_attr] = list(value)
            else:
                attr_map[ldap_attr] = [value]

        if not attr_map:
            return

        dn = self.user_dn(username)

        async def _op(conn):
            try:
                results = await conn.search(
                    dn, LDAPSearchScope.BASE, '(objectClass=*)',
                    timeout=self.config.request_timeout_seconds,
                )
            except bonsai.NoSuchObjectError:
                raise LDAPNotFound(f'No such user: {username}')
            if not results:
                raise LDAPNotFound(f'No such user: {username}')
            entry = results[0]
            for k, v in attr_map.items():
                entry[k] = v
            await entry.modify()
        await self._pooled_op(_op)

    async def delete_user(self, username: str) -> None:
        """Delete the user dn. Idempotent — LDAPNotFound is swallowed."""
        dn = self.user_dn(username)

        async def _op(conn):
            try:
                await conn.delete(dn, timeout=self.config.request_timeout_seconds)
            except bonsai.NoSuchObjectError:
                pass
        await self._pooled_op(_op)

    # -----------------------------------------------------------------
    # Group CRUD
    # -----------------------------------------------------------------

    async def add_group(self, record: LDAPGroupRecord) -> None:
        if not self.config.group_classes or not self.config.group_attrs:
            raise LDAPError(
                'LDAPConfig.group_classes / group_attrs required for add_group'
            )
        attrs = group_to_entry_attrs(
            record, self.config.group_attrs, self.config.group_classes,
        )
        dn = self.group_dn(record.groupname)

        async def _op(conn):
            entry = LDAPEntry(dn)
            for k, v in attrs.items():
                entry[k] = v
            await conn.add(entry, timeout=self.config.request_timeout_seconds)
        await self._pooled_op(_op)

    async def delete_group(self, groupname: str) -> None:
        dn = self.group_dn(groupname)

        async def _op(conn):
            try:
                await conn.delete(dn, timeout=self.config.request_timeout_seconds)
            except bonsai.NoSuchObjectError:
                pass
        await self._pooled_op(_op)

    async def add_users_to_group(
        self, groupname: str, usernames: Iterable[str],
        *, verify_users: bool = True,
    ) -> None:
        """Idempotent: members already present are silently skipped."""
        if not self.config.group_attrs:
            raise LDAPError('LDAPConfig.group_attrs is required')
        usernames = list(usernames)
        if not usernames:
            return
        if verify_users:
            for u in usernames:
                if not await self.user_exists(u):
                    raise LDAPInvalidUser(f'User does not exist in LDAP: {u}')

        dn = self.group_dn(groupname)
        members_attr = self.config.group_attrs['members']

        async def _op(conn):
            try:
                results = await conn.search(
                    dn, LDAPSearchScope.BASE, '(objectClass=*)',
                    timeout=self.config.request_timeout_seconds,
                )
            except bonsai.NoSuchObjectError:
                raise LDAPNotFound(f'No such group: {groupname}')
            if not results:
                raise LDAPNotFound(f'No such group: {groupname}')
            entry = results[0]
            current = set(entry.get(members_attr) or [])
            new = current | set(usernames)
            if new == current:
                return
            entry[members_attr] = sorted(new)
            await entry.modify()
        await self._pooled_op(_op)

    async def remove_users_from_group(
        self, groupname: str, usernames: Iterable[str],
    ) -> None:
        if not self.config.group_attrs:
            raise LDAPError('LDAPConfig.group_attrs is required')
        usernames = set(usernames)
        if not usernames:
            return

        dn = self.group_dn(groupname)
        members_attr = self.config.group_attrs['members']

        async def _op(conn):
            try:
                results = await conn.search(
                    dn, LDAPSearchScope.BASE, '(objectClass=*)',
                    timeout=self.config.request_timeout_seconds,
                )
            except bonsai.NoSuchObjectError:
                raise LDAPNotFound(f'No such group: {groupname}')
            if not results:
                raise LDAPNotFound(f'No such group: {groupname}')
            entry = results[0]
            current = set(entry.get(members_attr) or [])
            new = current - usernames
            if new == current:
                return
            entry[members_attr] = sorted(new)
            await entry.modify()
        await self._pooled_op(_op)

    async def set_group_members(
        self, groupname: str, usernames: set[str],
    ) -> None:
        """Diff-and-patch convenience: replace the group's members with the
        given set."""
        if not self.config.group_attrs:
            raise LDAPError('LDAPConfig.group_attrs is required')

        dn = self.group_dn(groupname)
        members_attr = self.config.group_attrs['members']

        async def _op(conn):
            try:
                results = await conn.search(
                    dn, LDAPSearchScope.BASE, '(objectClass=*)',
                    timeout=self.config.request_timeout_seconds,
                )
            except bonsai.NoSuchObjectError:
                raise LDAPNotFound(f'No such group: {groupname}')
            if not results:
                raise LDAPNotFound(f'No such group: {groupname}')
            entry = results[0]
            current = set(entry.get(members_attr) or [])
            if current == usernames:
                return
            entry[members_attr] = sorted(usernames)
            await entry.modify()
        await self._pooled_op(_op)

    # -----------------------------------------------------------------
    # Automount CRUD
    # -----------------------------------------------------------------

    async def add_automount(self, record: LDAPAutomountRecord) -> None:
        attrs = automount_to_entry_attrs(record)
        dn = self.automount_dn(record.mountname, record.mapname)

        async def _op(conn):
            entry = LDAPEntry(dn)
            for k, v in attrs.items():
                entry[k] = v
            await conn.add(entry, timeout=self.config.request_timeout_seconds)
        await self._pooled_op(_op)

    async def delete_automount(self, mountname: str, mapname: str) -> None:
        dn = self.automount_dn(mountname, mapname)

        async def _op(conn):
            try:
                await conn.delete(dn, timeout=self.config.request_timeout_seconds)
            except bonsai.NoSuchObjectError:
                pass
        await self._pooled_op(_op)

    async def upsert_home_automount(
        self, username: str, host: str, path: str, options: str = '',
    ) -> None:
        """Create-or-replace automountKey=<username> in auto.home."""
        await self.delete_automount(username, 'auto.home')
        await self.add_automount(LDAPAutomountRecord(
            mountname=username, mapname='auto.home',
            host=host, path=path, options=options,
        ))

    async def upsert_group_automount(
        self, storagename: str, host: str, path: str, options: str = '',
    ) -> None:
        await self.delete_automount(storagename, 'auto.group')
        await self.add_automount(LDAPAutomountRecord(
            mountname=storagename, mapname='auto.group',
            host=host, path=path, options=options,
        ))

    async def list_subtree_dns(
        self, base: str | None = None, *,
        exclude_bases: Iterable[str] = (),
    ) -> list[str]:
        """All DNs under `base` (default: configured searchbase), excluding
        any DN that lies within one of `exclude_bases` (suffix match, case
        insensitive). Returned list is sorted leaf-first (deepest DNs
        first) so callers can delete in order without orphaning OUs.

        `exclude_bases` entries also exclude the base DNs themselves and
        any descendants. Missing bases return [].
        """
        base = base or self.config.searchbase
        excludes_lower = [b.lower() for b in exclude_bases]

        async def _op(conn):
            try:
                results = await conn.search(
                    base, LDAPSearchScope.SUB, '(objectClass=*)',
                    attrlist=['1.1'],  # 1.1 = no attributes, DNs only
                    timeout=self.config.request_timeout_seconds,
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
                # The base itself is included by SUB scope; we don't
                # delete it (it's the search root, often the searchbase).
                continue
            if any(
                dn_lower == eb or dn_lower.endswith(',' + eb)
                for eb in excludes_lower
            ):
                continue
            kept.append(dn)
        # Leaf-first: deeper DNs (more commas) before shallower so a
        # single linear pass can delete children before their parents.
        kept.sort(key=lambda d: -d.count(','))
        return kept

    async def delete_dn(self, dn: str, *, missing_ok: bool = True) -> bool:
        """Delete a single DN. Returns True if deleted, False when the DN
        was already absent and `missing_ok=True`."""
        async def _op(conn):
            try:
                await conn.delete(
                    dn, timeout=self.config.request_timeout_seconds,
                )
                return True
            except bonsai.NoSuchObjectError:
                if missing_ok:
                    return False
                raise
        return await self._pooled_op(_op)

    async def list_automounts(self, mapname: str) -> list[str]:
        """Return automountKey values under the given map. Useful for prune."""
        async def _op(conn):
            try:
                results = await conn.search(
                    self.automount_map_dn(mapname),
                    LDAPSearchScope.ONELEVEL, '(objectClass=automount)',
                    attrlist=['automountKey'],
                    timeout=self.config.request_timeout_seconds,
                )
            except bonsai.NoSuchObjectError:
                return []
            return [
                e['automountKey'][0] for e in results
                if e.get('automountKey')
            ]
        return await self._pooled_op(_op)

    # -----------------------------------------------------------------
    # Bootstrap helpers
    # -----------------------------------------------------------------

    async def ensure_site_tree(self) -> dict[str, str]:
        """Create the per-site OU tree and automount maps if absent.

        Returns `{dn: 'created' | 'already_exists'}` for caller-visible
        reporting. Idempotent.
        """
        result: dict[str, str] = {}

        async def _ensure_ou(dn: str, name: str) -> None:
            if await self.dn_exists(dn):
                result[dn] = 'already_exists'
                return

            async def _op(conn):
                entry = LDAPEntry(dn)
                entry['objectClass'] = ['organizationalUnit']
                entry['ou'] = [name]
                await conn.add(entry, timeout=self.config.request_timeout_seconds)
            await self._pooled_op(_op)
            result[dn] = 'created'

        async def _ensure_map(dn: str, mapname: str) -> None:
            if await self.dn_exists(dn):
                result[dn] = 'already_exists'
                return

            async def _op(conn):
                entry = LDAPEntry(dn)
                entry['objectClass'] = ['automountMap']
                entry['automountMapName'] = [mapname]
                await conn.add(entry, timeout=self.config.request_timeout_seconds)
            await self._pooled_op(_op)
            result[dn] = 'created'

        async def _ensure_automount_key(
            dn: str, key: str, info: str,
        ) -> None:
            if await self.dn_exists(dn):
                result[dn] = 'already_exists'
                return

            async def _op(conn):
                entry = LDAPEntry(dn)
                entry['objectClass'] = ['automount']
                entry['automountKey'] = [key]
                entry['automountInformation'] = [info]
                await conn.add(entry, timeout=self.config.request_timeout_seconds)
            await self._pooled_op(_op)
            result[dn] = 'created'

        await _ensure_ou(self.user_ou_dn(), 'users')
        await _ensure_ou(self.site_ou_dn(), self.sitename)
        await _ensure_ou(self.groups_ou_dn(), 'groups')
        await _ensure_ou(self.automount_ou_dn(), 'automount')

        master_dn = self.automount_map_dn('auto.master')
        home_map_dn = self.automount_map_dn('auto.home')
        group_map_dn = self.automount_map_dn('auto.group')
        await _ensure_map(master_dn, 'auto.master')
        await _ensure_map(home_map_dn, 'auto.home')
        await _ensure_map(group_map_dn, 'auto.group')

        # /home and /group keys in auto.master pointing at the corresponding maps.
        home_key_dn = self.dn.automount_dn('/home', 'auto.master')
        group_key_dn = self.dn.automount_dn('/group', 'auto.master')
        await _ensure_automount_key(home_key_dn, '/home', home_map_dn)
        await _ensure_automount_key(
            group_key_dn, '/group', f'{group_map_dn} --ghost',
        )
        return result

    async def ensure_special_groups(
        self, groups: list[LDAPGroupRecord],
    ) -> dict[str, str]:
        """Create LDAP groupOfMembers entries for each LDAPGroupRecord.

        Caller dedupes by `groupname`. Idempotent — already-present groups
        are skipped. Returns `{groupname: 'created' | 'already_exists'}`.
        """
        out: dict[str, str] = {}
        for record in groups:
            if await self.group_exists(record.groupname):
                out[record.groupname] = 'already_exists'
                continue
            await self.add_group(record)
            out[record.groupname] = 'created'
        return out
