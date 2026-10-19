"""Native v2 Slurm reconciliation core.

Self-contained, puppet-free building blocks for syncing a site's desired
Slurm accounting state (assembled from beanie in
`cheeto.queries.slurm.build_desired_slurm_state`) to a live controller via
`sacctmgr`:

- Normalized, equality-comparable value types (`TRESLimit`, `QOSState`,
  `AccountState`, `SlurmSyncState`) that are the diff currency for both the
  desired side and the current (parsed-from-`sacctmgr`) side.
- Parsers for `sacctmgr show -P qos` / `show -P associations` /
  `show -P account withcoord` output.
- `reconcile()` — a native diff that emits ordered `sacctmgr` command
  batches (the dependency-safe ordering proven by the v1 implementation).
- `AsyncSAcctMgr` — an asyncio-friendly `sacctmgr` wrapper that reads state
  and dispatches commands via `sh`'s `_async=True`.

This module imports nothing from `cheeto.puppet`; the only shared
dependency is `size_to_megs`.
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
# The root user is never removed as a whole Slurm user. The users seen on a
# protected account and root's coordinators join it; see
# `SlurmSyncState.protected_users`.
PROTECTED_USERS = frozenset({'root'})


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

# A coordinator seat is keyed by (account, user).
CoordKey = tuple[str, str]


@dataclass
class SlurmSyncState:
    """A full snapshot of the reconcilable Slurm state for one site/cluster."""

    qos: dict[str, QOSState] = field(default_factory=dict)
    accounts: dict[str, AccountState] = field(default_factory=dict)
    associations: dict[AssocKey, str] = field(default_factory=dict)
    # MaxJobs on each user association (-1 is unlimited). Desired side: the
    # per-status value from `SLURM_STATUS_MAX_JOBS`, uniform across a user's
    # associations (0 for `disabled` users). Current side: parsed per row.
    # Reconciled per *user* with one `modify user ... set MaxJobs=` because
    # sacctmgr applies that to every association of the user, and because it
    # succeeds while the user has running jobs (unlike `remove user`).
    association_max_jobs: dict[AssocKey, int] = field(default_factory=dict)
    # Per-user default account (sacctmgr's user-level DefaultAccount), keyed
    # by username. Empty when the site has no configured default account.
    default_accounts: dict[str, str] = field(default_factory=dict)
    # Account coordinator seats as (account, user) pairs.
    coordinators: set[CoordKey] = field(default_factory=set)
    # Coordinators of the protected root account. Coordinator rights are
    # inherited down the account tree, so sacctmgr reports these on every
    # account. They are managed outside cheeto; `reconcile` masks them from
    # the diff so they're never added or removed here.
    root_coordinators: frozenset[str] = frozenset()
    # Users holding an association on a protected account (root). Those rows
    # are not parsed into `associations`, and `reconcile` never removes such
    # a user as a whole Slurm user. Managed outside cheeto.
    protected_users: frozenset[str] = frozenset()


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
) -> tuple[
    dict[AssocKey, str], dict[str, AccountState], dict[AssocKey, int],
    frozenset[str],
]:
    """Parse `sacctmgr show -P associations` output into
    (associations, accounts, association_max_jobs, protected_users).
    Account-level rows (no User) carry the account limits; partition-scoped
    user rows carry the (user, account, partition) -> QOS mapping and that
    association's MaxJobs (empty -> -1). A user row without a Partition is
    the user's parent association, which this sync does not manage: it is
    skipped rather than read as an account row. Rows on protected accounts
    (root) are skipped, but the users seen on them are returned as
    `protected_users` so `reconcile` never removes them as whole users."""
    associations: dict[AssocKey, str] = {}
    accounts: dict[str, AccountState] = {}
    association_max_jobs: dict[AssocKey, int] = {}
    protected_users: set[str] = set()
    for row in csv.DictReader(io.StringIO(text), delimiter='|'):
        account = (row.get('Account') or '').strip()
        if not account:
            continue
        partition = (row.get('Partition') or '').strip()
        user = (row.get('User') or '').strip()
        if account in PROTECTED_ACCOUNTS:
            if user:
                protected_users.add(user)
            continue
        if user:
            if partition:
                key = (user, account, partition)
                associations[key] = (row.get('QOS') or '').strip()
                association_max_jobs[key] = int(
                    (row.get('MaxJobs') or '-1').strip() or -1
                )
            continue
        # Account-level definition row.
        accounts[account] = AccountState(
            max_user_jobs=int((row.get('MaxJobs') or '-1').strip() or -1),
            max_group_jobs=int((row.get('GrpJobs') or '-1').strip() or -1),
            max_submit_jobs=int((row.get('MaxSubmit') or '-1').strip() or -1),
            max_job_length=(row.get('MaxWall') or '-1').strip() or '-1',
        )
    return (
        associations, accounts, association_max_jobs, frozenset(protected_users),
    )


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


def parse_account_coordinators(text: str) -> dict[str, frozenset[str]]:
    """Parse `sacctmgr show -P account withcoord format=Account,Coordinators`
    into an {account -> coordinator usernames} map. Accounts with no
    coordinators are omitted. Protected accounts (root) are kept so the
    caller can split their inherited coordinators out.

    Read by column index for the same reason as `parse_user_default_accounts`:
    the `format=` order is fixed (Account, then Coordinators) while the
    header text may be abbreviated. The header line is skipped (detected by
    a first cell of `Account`)."""
    out: dict[str, frozenset[str]] = {}
    for row in csv.reader(io.StringIO(text), delimiter='|'):
        if len(row) < 2:
            continue
        account = row[0].strip()
        if not account or account == 'Account':  # header row
            continue
        users = frozenset(u.strip() for u in row[1].split(',') if u.strip())
        if users:
            out[account] = users
    return out


# ---------------------------------------------------------------------------
# Command specs + sacctmgr argument rendering
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CommandSpec:
    """One sacctmgr invocation, rendered either to a preview string or to a
    baked `sh.Command` (via `AsyncSAcctMgr.render`)."""

    verb: str        # 'add' | 'modify' | 'remove'
    entity: str      # 'qos' | 'account' | 'user' | 'coordinator'
    args: tuple[str, ...] = ()
    # Managed association rows this command removes; summed by
    # `count_deletions` over the deletion batches. 1 for everything except
    # the user- and account-scoped removals, which drop several rows at once.
    deletions: int = 1

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


def _remove_user_account(user: str, account: str, *, rows: int) -> CommandSpec:
    # No partition filter: removes every row of the user on the account,
    # including the partition-less parent row that per-partition removal
    # leaves behind and that slurmctld falls back to for partitions without
    # a specific association.
    return CommandSpec('remove', 'user', (
        f'user={user}', f'account={account}',
    ), deletions=rows)


def _remove_whole_user(user: str, *, rows: int) -> CommandSpec:
    # No account/partition filter: removes the user record, its parent rows
    # and every partition row on the cluster. This is the only form sacctmgr
    # accepts for rows on the user's default account.
    return CommandSpec('remove', 'user', (f'user={user}',), deletions=rows)


def _add_coordinator(key: CoordKey) -> CommandSpec:
    account, user = key
    return CommandSpec('add', 'coordinator', (
        f'account={account}', f'names={user}',
    ))


def _remove_coordinator(key: CoordKey) -> CommandSpec:
    account, user = key
    return CommandSpec('remove', 'coordinator', (
        f'account={account}', f'names={user}',
    ))


def _set_user_default_account(user: str, account: str) -> CommandSpec:
    # DefaultAccount is a user-level attribute; scope the `where` to the user
    # only (not a specific association).
    return CommandSpec('modify', 'user', (
        'set', f'defaultaccount={account}', 'where', f'user={user}',
    ))


def _set_user_max_jobs(user: str, max_jobs: int) -> CommandSpec:
    # MaxJobs is an association limit. A `where user=` without account or
    # partition reaches every association of the user, including the
    # partition-less parent row this sync does not otherwise track. Unlike
    # `remove user`, sacctmgr accepts this while the user has running jobs.
    return CommandSpec('modify', 'user', (
        'set', f'MaxJobs={max_jobs}', 'where', f'user={user}',
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


def _plan_user_removals(
    desired: SlurmSyncState, current: SlurmSyncState, user_del: list[AssocKey],
) -> tuple[list[CommandSpec], list[AssocKey]]:
    """Split the stale association keys into whole-user removals and the
    residual keys. A user with rows to delete and no desired row at the site
    is removed outright: sacctmgr refuses to delete any row on the user's
    default account, and the unscoped `remove user` is the only form it
    accepts for those. Protected users (root, anyone with a row on a
    protected account, root's coordinators) keep scoped removal. Candidates
    come from the stale keys only, never from `show user`, so a user with no
    managed row is never touched."""
    desired_users = {u for (u, _a, _p) in desired.associations}
    protected = (
        PROTECTED_USERS | current.protected_users | current.root_coordinators
    )
    rows: dict[str, int] = {}
    for (u, _a, _p) in user_del:
        rows[u] = rows.get(u, 0) + 1
    whole = sorted(
        u for u in rows if u not in desired_users and u not in protected
    )
    removed = set(whole)
    specs = [_remove_whole_user(u, rows=rows[u]) for u in whole]
    residual = [k for k in user_del if k[0] not in removed]
    return specs, residual


def _plan_association_removals(
    desired: SlurmSyncState, residual: list[AssocKey],
) -> list[CommandSpec]:
    """Render the residual stale keys. When the user keeps no desired row on
    an account, one account-scoped `remove user` drops every row there,
    including the partition-less parent row that slurmctld falls back to
    for partitions without a specific association. Otherwise each stale
    partition row is removed on its own."""
    desired_pairs = {(u, a) for (u, a, _p) in desired.associations}
    by_pair: dict[tuple[str, str], list[AssocKey]] = {}
    for key in sorted(residual):
        by_pair.setdefault((key[0], key[1]), []).append(key)
    specs: list[CommandSpec] = []
    for (user, account), keys in by_pair.items():
        if (user, account) in desired_pairs:
            specs.extend(_remove_user(k) for k in keys)
        else:
            specs.append(_remove_user_account(user, account, rows=len(keys)))
    return specs


def _default_account_repoints(
    desired: SlurmSyncState, current: SlurmSyncState, residual: list[AssocKey],
) -> dict[str, str]:
    """Default accounts that must move before the residual rows can be
    deleted. sacctmgr refuses to delete any row on a user's current default
    account, so a user losing rows there is re-pointed to a desired account
    first: the lowest-sorted one with no deletions this run, else the
    lowest-sorted one other than the current default. A user with a desired
    default (the site has one configured) is governed by that comparison
    instead. With no other account to move to the rows cannot be deleted; a
    warning is logged and the delete is left to fail as before."""
    deleting: dict[str, set[str]] = {}
    for (u, a, _p) in residual:
        deleting.setdefault(u, set()).add(a)
    keeping: dict[str, set[str]] = {}
    for (u, a, _p) in desired.associations:
        keeping.setdefault(u, set()).add(a)
    out: dict[str, str] = {}
    for u in sorted(deleting):
        if u in desired.default_accounts:
            continue
        cur = current.default_accounts.get(u)
        if cur is None or cur not in deleting[u]:
            continue
        have = keeping.get(u, set())
        choice = sorted(have - deleting[u]) or sorted(have - {cur})
        if not choice:
            logger.warning(
                'Slurm user %s: rows on default account %s are stale but no '
                'other account can take the default; sacctmgr will refuse '
                'to delete them', u, cur,
            )
            continue
        out[u] = choice[0]
    return out


def reconcile(
    desired: SlurmSyncState, current: SlurmSyncState,
) -> list[CommandBatch]:
    """Diff desired vs current and emit ordered sacctmgr command batches.

    Ordering is dependency-safe: emit all creates, then re-point user default
    accounts, then all deletes. The constraints:
      - sacctmgr refuses to delete any association row on a user's current
        default account. A user with no desired row at the site is removed
        as a whole ('Delete users'), the only form sacctmgr accepts for those
        rows. A user keeping rows elsewhere has the default re-pointed first:
        'Set user default accounts' runs before 'Delete user associations',
        and after 'Add user associations' so a new target row exists. The
        desired default is the site default when one is configured, else the
        pick made by `_default_account_repoints`.
      - A user who keeps no desired row on an account loses every row there
        with one account-scoped remove, parent row included; otherwise stale
        partition rows are removed one by one.
      - Add-sets and delete-sets are disjoint by name/key (a same-named change
        is a modify, never delete+add), so doing all adds before all deletes
        introduces no collisions.
      - Per-user MaxJobs is set after user associations are added/modified
        so a new `disabled` user's fresh rows are capped in the same run.
        Only desired associations are compared (a stale row about to be
        deleted never triggers a modify); a row missing from the current
        side is a new association, which sacctmgr creates unlimited (-1).
      - Coordinator seats are added after user associations (slurmctld only
        honors a seat for a user it knows) and removed before accounts are
        deleted (the account must still exist for the removal to apply).
        Seats are only removed on accounts in the desired state: an account
        being deleted takes its seats with it, and an account absent from
        the desired state isn't managed here. Root's coordinators are
        inherited by every account and are masked from both sides.
        'Remove coordinators' also precedes 'Delete users' so a removed
        user's seats go explicitly even when the user removal fails on
        running jobs, and 'Delete users' precedes 'Delete accounts' because
        it empties accounts that are deleted in the same run.
    """
    qos_add, qos_upd, qos_del = _diff(desired.qos, current.qos)
    acct_add, acct_upd, acct_del = _diff(desired.accounts, current.accounts)
    user_add, user_upd, user_del = _diff(desired.associations, current.associations)
    remove_users, residual = _plan_user_removals(desired, current, user_del)
    remove_assocs = _plan_association_removals(desired, residual)

    default_changes: dict[str, str] = {
        u: acct for u, acct in desired.default_accounts.items()
        if current.default_accounts.get(u) != acct
    }
    default_changes.update(_default_account_repoints(desired, current, residual))

    # The desired value is uniform per user (build_desired_slurm_state derives
    # it from the user's status), so one user-wide command per drifted user.
    max_jobs_by_user = {
        key[0]: v for key, v in desired.association_max_jobs.items()
    }
    max_jobs_changes = sorted({
        key[0] for key, want in desired.association_max_jobs.items()
        if current.association_max_jobs.get(key, -1) != want
    })

    mask = current.root_coordinators
    desired_coords = {k for k in desired.coordinators if k[1] not in mask}
    current_coords = {k for k in current.coordinators if k[1] not in mask}
    coord_add = sorted(desired_coords - current_coords)
    coord_del = sorted(
        k for k in current_coords - desired_coords if k[0] in desired.accounts
    )

    return [
        CommandBatch('Add QOS', [_add_qos(n, desired.qos[n]) for n in qos_add]),
        CommandBatch('Modify QOS', [_modify_qos(n, desired.qos[n]) for n in qos_upd]),
        CommandBatch('Add accounts', [_add_account(n, desired.accounts[n]) for n in acct_add]),
        CommandBatch('Modify accounts', [_modify_account(n, desired.accounts[n]) for n in acct_upd]),
        CommandBatch('Add user associations',
                     [_add_user(k, desired.associations[k]) for k in user_add]),
        CommandBatch('Modify user associations',
                     [_modify_user(k, desired.associations[k]) for k in user_upd]),
        CommandBatch('Set user MaxJobs',
                     [_set_user_max_jobs(u, max_jobs_by_user[u])
                      for u in max_jobs_changes]),
        CommandBatch('Add coordinators', [_add_coordinator(k) for k in coord_add]),
        CommandBatch('Set user default accounts',
                     [_set_user_default_account(u, acct)
                      for u, acct in sorted(default_changes.items())]),
        CommandBatch('Remove coordinators',
                     [_remove_coordinator(k) for k in coord_del], is_deletion=True),
        CommandBatch('Delete users', remove_users, is_deletion=True),
        CommandBatch('Delete user associations', remove_assocs, is_deletion=True),
        CommandBatch('Delete QOS', [_remove_qos(n) for n in qos_del], is_deletion=True),
        CommandBatch('Delete accounts',
                     [_remove_account(n) for n in acct_del], is_deletion=True),
    ]


def count_deletions(batches: list[CommandBatch]) -> int:
    """Association rows (and coordinator seats) removed by the deletion
    batches. Each spec counts its `deletions`, so the cap in `SyncSlurm`
    measures rows whichever form the removal takes."""
    return sum(s.deletions for b in batches if b.is_deletion for s in b.specs)


def parse_state(
    qos_text: str, associations_text: str, user_text: str = '',
    coordinators_text: str = '',
) -> SlurmSyncState:
    """Assemble a `SlurmSyncState` from raw `sacctmgr show -P` output for qos,
    associations, and (optionally) users and account coordinators. Shared by
    the live and dump readers."""
    associations, accounts, association_max_jobs, protected_users = (
        parse_association_state(associations_text)
    )
    coords_by_account = parse_account_coordinators(coordinators_text)
    root_coordinators: set[str] = set()
    for name in PROTECTED_ACCOUNTS:
        root_coordinators |= coords_by_account.pop(name, frozenset())
    coordinators = {
        (account, user)
        for account, users in coords_by_account.items()
        for user in users
    }
    return SlurmSyncState(
        qos=parse_qos_state(qos_text),
        accounts=accounts,
        associations=associations,
        association_max_jobs=association_max_jobs,
        default_accounts=parse_user_default_accounts(user_text),
        coordinators=coordinators,
        root_coordinators=frozenset(root_coordinators),
        protected_users=protected_users,
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

    async def show_account_coordinators(self) -> str:
        return str(await self._show.bake(
            'account', 'withcoord', 'format=Account,Coordinators',
        )(_async=True))

    def render(self, spec: CommandSpec) -> sh.Command:
        return self._base.bake(*spec.tokens())

    async def dispatch(self, spec: CommandSpec):
        """Execute one CommandSpec; raises sh.ErrorReturnCode on failure."""
        return await self.render(spec)(_async=True)

    async def read_current_state(self) -> SlurmSyncState:
        # The four reads are independent; fetch them concurrently.
        qos_text, assoc_text, user_text, coord_text = await asyncio.gather(
            self.show_qos(), self.show_associations(), self.show_user(),
            self.show_account_coordinators(),
        )
        return parse_state(qos_text, assoc_text, user_text, coord_text)


class DumpSAcctMgr:
    """An `AsyncSAcctMgr`-compatible reader backed by captured
    `sacctmgr show -P` text instead of a live controller.

    For testing and offline planning: feed it the saved output of
    `sacctmgr show -P qos` / `show -P associations` / `show -P user
    format=User,DefaultAccount` / `show -P account withcoord
    format=Account,Coordinators` and run a `SyncSlurm` against it without a
    Slurm controller. `dispatch` performs no controller I/O — it records the
    command it would have run on `self.dispatched`, so the apply path's
    batching/tally can be exercised offline (it never mutates anything).
    """

    def __init__(
        self, *, qos_text: str = '', associations_text: str = '',
        user_text: str = '', coordinators_text: str = '',
    ) -> None:
        self._qos_text = qos_text
        self._associations_text = associations_text
        self._user_text = user_text
        self._coordinators_text = coordinators_text
        self.dispatched: list[str] = []

    @classmethod
    def from_files(
        cls,
        qos_path: str | os.PathLike | None = None,
        associations_path: str | os.PathLike | None = None,
        user_path: str | os.PathLike | None = None,
        coordinators_path: str | os.PathLike | None = None,
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
            coordinators_text=_read(coordinators_path),
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
            self._coordinators_text,
        )
