"""Native v2 Slurm reconciliation core.

Self-contained, puppet-free building blocks for syncing a site's desired
Slurm accounting state (assembled from beanie in
`cheeto.queries.slurm.build_desired_slurm_state`) to a live controller via
`sacctmgr`:

- Normalized, equality-comparable value types (`TRESLimit`, `QOSState`,
  `AccountState`, `SlurmSyncState`) that are the diff currency for both the
  desired side and the current (parsed-from-`sacctmgr`) side.
- Parsers for `sacctmgr show -P qos` / `show -P associations` output.
- `reconcile()` — a native diff that emits ordered `sacctmgr` command
  batches (the dependency-safe ordering proven by the v1 implementation).
- `AsyncSAcctMgr` — an asyncio-friendly `sacctmgr` wrapper that reads state
  and dispatches commands via `sh`'s `_async=True`.

This module imports nothing from the v1 `cheeto.slurm` module or
`cheeto.puppet`; the only shared dependency is `size_to_megs`.
"""

from __future__ import annotations

import asyncio
import csv
import io
import os
import logging
from dataclasses import dataclass, field

import sh

from .utils import size_to_megs

logger = logging.getLogger(__name__)

class SlurmSyncAborted(Exception):
    """Raised when a reconcile plan exceeds the deletion safety cap. Carries
    the planned deletions so the caller can surface them."""

    def __init__(self, message: str, *, would_delete: list[str]) -> None:
        super().__init__(message)
        self.would_delete = would_delete


# sacctmgr's built-in QOS / the root account are never created or deleted by
# the sync; they're filtered out of the current state so they can't become
# reconciliation candidates.
PROTECTED_QOS = frozenset({'normal'})
PROTECTED_ACCOUNTS = frozenset({'root'})


# ---------------------------------------------------------------------------
# Normalized state types
# ---------------------------------------------------------------------------


def _int_or_none(token: str | None) -> int | None:
    token = (token or '').strip()
    if not token or token == '-1':
        return None
    return int(token)


def _mem_to_megs(token: str | None) -> int | None:
    """sacctmgr emits TRES mem either as a bare integer (megabytes, its
    default unit) or with a unit suffix; desired state always carries a
    suffix. Normalize both to an int number of megabytes; -1/empty → None."""
    token = (token or '').strip()
    if not token or token == '-1':
        return None
    if token[-1] in 'MmGgTtPp':
        return size_to_megs(token)
    return int(token)


@dataclass(frozen=True)
class TRESLimit:
    """A normalized TRES triple. None means 'unlimited' (rendered as -1)."""

    cpus: int | None = None
    gpus: int | None = None
    mem_megs: int | None = None

    @classmethod
    def from_tres_string(cls, s: str | None) -> 'TRESLimit':
        cpus = gpus = mem = None
        for tok in (s or '').split(','):
            if not tok.strip():
                continue
            resource, _, value = tok.partition('=')
            resource = resource.strip().removeprefix('gres/')
            resource, _, _ = resource.partition(':')  # drop gres type
            if resource == 'cpu':
                cpus = _int_or_none(value)
            elif resource == 'gpu':
                gpus = _int_or_none(value)
            elif resource == 'mem':
                mem = _mem_to_megs(value)
        return cls(cpus=cpus, gpus=gpus, mem_megs=mem)

    def to_tres_string(self) -> str:
        """Render as a sacctmgr TRES spec; unlimited fields emit -1."""
        cpu = -1 if self.cpus is None else self.cpus
        gpu = -1 if self.gpus is None else self.gpus
        mem = -1 if self.mem_megs is None else self.mem_megs
        return f'cpu={cpu},mem={mem},gres/gpu={gpu}'


EMPTY_TRES = TRESLimit()


@dataclass(frozen=True)
class QOSState:
    group: TRESLimit = EMPTY_TRES
    user: TRESLimit = EMPTY_TRES
    job: TRESLimit = EMPTY_TRES
    priority: int = 0
    flags: frozenset[str] = frozenset()


@dataclass(frozen=True)
class AccountState:
    max_user_jobs: int = -1
    max_group_jobs: int = -1
    max_submit_jobs: int = -1
    max_job_length: str = '-1'


# An association is keyed by (user, account, partition) -> qos name.
AssocKey = tuple[str, str, str]


@dataclass
class SlurmSyncState:
    """A full snapshot of the reconcilable Slurm state for one site/cluster."""

    qos: dict[str, QOSState] = field(default_factory=dict)
    accounts: dict[str, AccountState] = field(default_factory=dict)
    associations: dict[AssocKey, str] = field(default_factory=dict)
    # Per-user default account (sacctmgr's user-level DefaultAccount), keyed
    # by username. Empty when the site has no configured default account.
    default_accounts: dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Current-state parsers (sacctmgr show -P output)
# ---------------------------------------------------------------------------


def parse_qos_state(text: str) -> dict[str, QOSState]:
    """Parse `sacctmgr show -P qos` output. The built-in `normal` QOS is
    skipped so it's never a deletion candidate."""
    out: dict[str, QOSState] = {}
    for row in csv.DictReader(io.StringIO(text), delimiter='|'):
        name = (row.get('Name') or '').strip()
        if not name or name in PROTECTED_QOS:
            continue
        flags = frozenset(
            f for f in (row.get('Flags') or '').split(',') if f.strip()
        )
        out[name] = QOSState(
            group=TRESLimit.from_tres_string(row.get('GrpTRES')),
            user=TRESLimit.from_tres_string(row.get('MaxTRESPU')),
            job=TRESLimit.from_tres_string(row.get('MaxTRES')),
            priority=int((row.get('Priority') or '0').strip() or 0),
            flags=flags,
        )
    return out


def parse_association_state(
    text: str,
) -> tuple[dict[AssocKey, str], dict[str, AccountState]]:
    """Parse `sacctmgr show -P associations` output into
    (associations, accounts). Account-level rows (no Partition) carry the
    account limits; user rows carry the (user, account, partition) -> QOS
    mapping. The root account is skipped."""
    associations: dict[AssocKey, str] = {}
    accounts: dict[str, AccountState] = {}
    for row in csv.DictReader(io.StringIO(text), delimiter='|'):
        account = (row.get('Account') or '').strip()
        if not account or account in PROTECTED_ACCOUNTS:
            continue
        partition = (row.get('Partition') or '').strip()
        user = (row.get('User') or '').strip()
        if not partition:
            # Account-level definition row.
            accounts[account] = AccountState(
                max_user_jobs=int((row.get('MaxJobs') or '-1').strip() or -1),
                max_group_jobs=int((row.get('GrpJobs') or '-1').strip() or -1),
                max_submit_jobs=int((row.get('MaxSubmit') or '-1').strip() or -1),
                max_job_length=(row.get('MaxWall') or '-1').strip() or '-1',
            )
        elif user:
            associations[(user, account, partition)] = (row.get('QOS') or '').strip()
    return associations, accounts


def parse_user_default_accounts(text: str) -> dict[str, str]:
    """Parse `sacctmgr show -P user format=User,DefaultAccount` into a
    {user -> default account} map.

    Read by column index rather than header name: sacctmgr abbreviates the
    DefaultAccount header (e.g. `Def Acct`), but the `format=` order is fixed
    (User, then DefaultAccount), so positions are stable. The header line is
    skipped (detected by a first cell of `User`)."""
    out: dict[str, str] = {}
    for row in csv.reader(io.StringIO(text), delimiter='|'):
        if len(row) < 2:
            continue
        user = row[0].strip()
        if not user or user == 'User':  # header row
            continue
        default = row[1].strip()
        if default:
            out[user] = default
    return out


# ---------------------------------------------------------------------------
# Command specs + sacctmgr argument rendering
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CommandSpec:
    """One sacctmgr invocation, rendered either to a preview string or to a
    baked `sh.Command` (via `AsyncSAcctMgr.render`)."""

    verb: str        # 'add' | 'modify' | 'remove'
    entity: str      # 'qos' | 'account' | 'user'
    args: tuple[str, ...] = ()

    def tokens(self) -> list[str]:
        return [self.verb, self.entity, *self.args]

    def __str__(self) -> str:
        return 'sacctmgr -iQ ' + ' '.join(self.tokens())


@dataclass
class CommandBatch:
    label: str
    specs: list[CommandSpec]
    is_deletion: bool = False


def _qos_set_args(q: QOSState, *, modify: bool) -> list[str]:
    args = [
        f'GrpTRES={q.group.to_tres_string()}',
        f'MaxTRESPerUser={q.user.to_tres_string()}',
        f'MaxTRESPerJob={q.job.to_tres_string()}',
    ]
    if q.flags:
        args.append(f'Flags={",".join(sorted(q.flags))}')
    elif modify:
        # Clear flags on modify when none are desired.
        args.append('Flags=-1')
    args.append(f'Priority={q.priority}')
    return args


def _account_set_args(a: AccountState) -> list[str]:
    return [
        f'MaxJobs={a.max_user_jobs}',
        f'GrpJobs={a.max_group_jobs}',
        f'MaxWall={a.max_job_length}',
        f'MaxSubmit={a.max_submit_jobs}',
    ]


def _add_qos(name: str, q: QOSState) -> CommandSpec:
    return CommandSpec('add', 'qos', (name, *_qos_set_args(q, modify=False)))


def _modify_qos(name: str, q: QOSState) -> CommandSpec:
    return CommandSpec('modify', 'qos', (name, 'set', *_qos_set_args(q, modify=True)))


def _remove_qos(name: str) -> CommandSpec:
    return CommandSpec('remove', 'qos', (name,))


def _add_account(name: str, a: AccountState) -> CommandSpec:
    return CommandSpec('add', 'account', (name, *_account_set_args(a)))


def _modify_account(name: str, a: AccountState) -> CommandSpec:
    return CommandSpec('modify', 'account', (name, 'set', *_account_set_args(a)))


def _remove_account(name: str) -> CommandSpec:
    return CommandSpec('remove', 'account', (name,))


def _add_user(key: AssocKey, qos: str) -> CommandSpec:
    user, account, partition = key
    return CommandSpec('add', 'user', (
        f'user={user}', f'account={account}',
        f'partition={partition}', f'qos={qos}',
    ))


def _modify_user(key: AssocKey, qos: str) -> CommandSpec:
    user, account, partition = key
    return CommandSpec('modify', 'user', (
        'set', f'qos={qos}', 'defaultqos=-1', 'where',
        f'user={user}', f'account={account}', f'partition={partition}',
    ))


def _remove_user(key: AssocKey) -> CommandSpec:
    user, account, partition = key
    return CommandSpec('remove', 'user', (
        f'user={user}', f'account={account}', f'partition={partition}',
    ))


def _set_user_default_account(user: str, account: str) -> CommandSpec:
    # DefaultAccount is a user-level attribute; scope the `where` to the user
    # only (not a specific association).
    return CommandSpec('modify', 'user', (
        'set', f'defaultaccount={account}', 'where', f'user={user}',
    ))


# ---------------------------------------------------------------------------
# Reconcile
# ---------------------------------------------------------------------------


def _diff(desired: dict, current: dict):
    """Return (additions, updates, deletions) as lists of keys. An update is
    a key present in both whose value differs."""
    additions = [k for k in desired if k not in current]
    updates = [k for k in desired if k in current and desired[k] != current[k]]
    deletions = [k for k in current if k not in desired]
    return additions, updates, deletions


def reconcile(
    desired: SlurmSyncState, current: SlurmSyncState,
) -> list[CommandBatch]:
    """Diff desired vs current and emit ordered sacctmgr command batches.

    Ordering is dependency-safe: emit all creates, then re-point user default
    accounts, then all deletes. Two constraints drive it:
      - A user's default account must point at a surviving account before its
        stale associations are deleted (sacctmgr refuses to delete a user's
        current default association). So 'Set user default accounts' runs
        before 'Delete user associations', and after 'Add user associations'
        so the (sticky) default-account association exists to point at.
      - Add-sets and delete-sets are disjoint by name/key (a same-named change
        is a modify, never delete+add), so doing all adds before all deletes
        introduces no collisions.
    """
    qos_add, qos_upd, qos_del = _diff(desired.qos, current.qos)
    acct_add, acct_upd, acct_del = _diff(desired.accounts, current.accounts)
    user_add, user_upd, user_del = _diff(desired.associations, current.associations)

    default_changes = [
        u for u, acct in desired.default_accounts.items()
        if current.default_accounts.get(u) != acct
    ]

    return [
        CommandBatch('Add QOS', [_add_qos(n, desired.qos[n]) for n in qos_add]),
        CommandBatch('Modify QOS', [_modify_qos(n, desired.qos[n]) for n in qos_upd]),
        CommandBatch('Add accounts', [_add_account(n, desired.accounts[n]) for n in acct_add]),
        CommandBatch('Modify accounts', [_modify_account(n, desired.accounts[n]) for n in acct_upd]),
        CommandBatch('Add user associations',
                     [_add_user(k, desired.associations[k]) for k in user_add]),
        CommandBatch('Modify user associations',
                     [_modify_user(k, desired.associations[k]) for k in user_upd]),
        CommandBatch('Set user default accounts',
                     [_set_user_default_account(u, desired.default_accounts[u])
                      for u in default_changes]),
        CommandBatch('Delete user associations',
                     [_remove_user(k) for k in user_del], is_deletion=True),
        CommandBatch('Delete QOS', [_remove_qos(n) for n in qos_del], is_deletion=True),
        CommandBatch('Delete accounts',
                     [_remove_account(n) for n in acct_del], is_deletion=True),
    ]


def count_deletions(batches: list[CommandBatch]) -> int:
    return sum(len(b.specs) for b in batches if b.is_deletion)


def parse_state(
    qos_text: str, associations_text: str, user_text: str = '',
) -> SlurmSyncState:
    """Assemble a `SlurmSyncState` from raw `sacctmgr show -P` output for qos,
    associations, and (optionally) users. Shared by the live and dump
    readers."""
    associations, accounts = parse_association_state(associations_text)
    return SlurmSyncState(
        qos=parse_qos_state(qos_text),
        accounts=accounts,
        associations=associations,
        default_accounts=parse_user_default_accounts(user_text),
    )


# ---------------------------------------------------------------------------
# AsyncSAcctMgr / DumpSAcctMgr
# ---------------------------------------------------------------------------


class AsyncSAcctMgr:
    """asyncio-friendly `sacctmgr` wrapper. Reads state and dispatches
    commands via `sh`'s `_async=True` so the controller round-trips don't
    block the event loop. See the `sh` reference rule for `_async` usage.

    `exec_prefix` runs `sacctmgr` through another command rather than on the
    host — e.g. `exec_prefix=['docker', 'exec', 'slurmctld']` to drive a
    containerized controller (used by the CI integration test). When set, the
    host is not probed for a `sacctmgr` binary; `sacctmgr_path` (default
    `'sacctmgr'`) is resolved inside the target instead.
    """

    def __init__(
        self,
        *,
        sudo: bool = False,
        sacctmgr_path: str | None = None,
        exec_prefix: list[str] | None = None,
    ) -> None:
        self.exec_prefix = list(exec_prefix) if exec_prefix else None
        if self.exec_prefix:
            # sacctmgr lives inside the target (e.g. a container); don't
            # resolve or stat it on the host.
            self.path = sacctmgr_path or 'sacctmgr'
            runner = sh.Command(self.exec_prefix[0])
            self._base = runner.bake(
                *self.exec_prefix[1:], self.path, '-iQ',
            )
        else:
            path = sacctmgr_path or str(sh.which('sacctmgr')).strip()
            if not path or not os.path.exists(path):
                raise RuntimeError(f'sacctmgr not found (resolved to {path!r})')
            self.path = path
            if sudo:
                self._base = sh.sudo.bake(path, '-iQ')
            else:
                self._base = sh.Command(path).bake('-iQ')
        self._show = self._base.bake('show', '-P')

    async def show_qos(self) -> str:
        return str(await self._show.bake('qos')(_async=True))

    async def show_associations(self) -> str:
        return str(await self._show.bake('associations')(_async=True))

    async def show_user(self) -> str:
        return str(await self._show.bake(
            'user', 'format=User,DefaultAccount',
        )(_async=True))

    def render(self, spec: CommandSpec) -> sh.Command:
        return self._base.bake(*spec.tokens())

    async def dispatch(self, spec: CommandSpec):
        """Execute one CommandSpec; raises sh.ErrorReturnCode on failure."""
        return await self.render(spec)(_async=True)

    async def read_current_state(self) -> SlurmSyncState:
        # The three reads are independent; fetch them concurrently.
        qos_text, assoc_text, user_text = await asyncio.gather(
            self.show_qos(), self.show_associations(), self.show_user(),
        )
        return parse_state(qos_text, assoc_text, user_text)


class DumpSAcctMgr:
    """An `AsyncSAcctMgr`-compatible reader backed by captured
    `sacctmgr show -P` text instead of a live controller.

    For testing and offline planning: feed it the saved output of
    `sacctmgr show -P qos` / `show -P associations` / `show -P user
    format=User,DefaultAccount` and run a `SyncSlurm` against it without a
    Slurm controller. `dispatch` performs no controller I/O — it records the
    command it would have run on `self.dispatched`, so the apply path's
    batching/tally can be exercised offline (it never mutates anything).
    """

    def __init__(
        self, *, qos_text: str = '', associations_text: str = '',
        user_text: str = '',
    ) -> None:
        self._qos_text = qos_text
        self._associations_text = associations_text
        self._user_text = user_text
        self.dispatched: list[str] = []

    @classmethod
    def from_files(
        cls,
        qos_path: str | os.PathLike | None = None,
        associations_path: str | os.PathLike | None = None,
        user_path: str | os.PathLike | None = None,
    ) -> 'DumpSAcctMgr':
        def _read(p) -> str:
            if p is None:
                return ''
            with open(p) as fp:
                return fp.read()
        return cls(
            qos_text=_read(qos_path),
            associations_text=_read(associations_path),
            user_text=_read(user_path),
        )

    def render(self, spec: CommandSpec) -> str:
        return str(spec)

    async def dispatch(self, spec: CommandSpec):
        """Record the command instead of running it; no controller side
        effects."""
        self.dispatched.append(str(spec))
        return None

    async def read_current_state(self) -> SlurmSyncState:
        return parse_state(
            self._qos_text, self._associations_text, self._user_text,
        )
