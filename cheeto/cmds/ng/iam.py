"""`cheeto ng iam` — sync UC Davis IAM data into beanie User records.

Wraps the SyncUserIAM / SyncAllUsersIAM / ReapOffboardedUsers operations.
The hand-written v1 client at `cheeto/iam.py` keeps working in parallel; this
namespace is the new async/beanie path.
"""

from argparse import Namespace
from datetime import datetime, timedelta, timezone

from ponderosa import ArgParser
from rich.panel import Panel
from rich.table import Table

from .. import commands
from ...constants import IAM_SYNCABLE_USER_TYPES
from ...iam_async import AsyncIAMAPI
from ...log import Console
from ...models.user import User
from ...operations import (
    ReapOffboardedUsers,
    SyncAllUsersIAM,
    SyncUserIAM,
)
from ._args import user_args


@commands.register('ng', 'iam',
                   help='UC Davis IAM sync operations (async/beanie)')
def iam_cmd(args: Namespace):
    pass


# ---------------------------------------------------------------------------
# `ng iam sync --user NAME`
# ---------------------------------------------------------------------------


@user_args.apply(required=True)
@commands.register('ng', 'iam', 'sync',
                   help='Sync a single user against the IAM API')
async def iam_sync_cmd(args: Namespace):
    console = Console()
    cfg = args.config.ucdiam
    grace = args.grace_days if args.grace_days is not None else cfg.grace_days
    offset = (
        args.expiry_offset_days if args.expiry_offset_days is not None
        else cfg.expiry_offset_days
    )
    async with AsyncIAMAPI(cfg) as iam_api:
        try:
            result = await SyncUserIAM.run(
                args.db, args.author,
                username=args.user,
                iam_api=iam_api,
                grace_days=grace,
                expiry_offset_days=offset,
            )
        except ValueError as e:
            console.print(f'[red]{e}[/]')
            return 1

    style = _outcome_style(result.outcome)
    console.print(
        f'[bold]{result.username}[/] -> [{style}]{result.outcome}[/] '
        f'(status={result.status}, '
        f'expires_at={result.expires_at or "—"})'
    )


@iam_sync_cmd.args()
def _(parser: ArgParser):
    parser.add_argument('--grace-days', type=int, default=None,
                        help='Override IAMConfig.grace_days for this run')
    parser.add_argument('--expiry-offset-days', type=int, default=None,
                        help='Override IAMConfig.expiry_offset_days for this run')


# ---------------------------------------------------------------------------
# `ng iam sync-all`
# ---------------------------------------------------------------------------


@commands.register('ng', 'iam', 'sync-all',
                   help='Sync all eligible users against the IAM API')
async def iam_sync_all_cmd(args: Namespace):
    console = Console()
    cfg = args.config.ucdiam
    grace = args.grace_days if args.grace_days is not None else cfg.grace_days
    offset = (
        args.expiry_offset_days if args.expiry_offset_days is not None
        else cfg.expiry_offset_days
    )
    types = args.type or list(IAM_SYNCABLE_USER_TYPES)

    async with AsyncIAMAPI(cfg) as iam_api:
        tally = await SyncAllUsersIAM.run(
            args.db, args.author,
            iam_api=iam_api,
            grace_days=grace,
            expiry_offset_days=offset,
            types=types,
            max_users=args.max_users,
            concurrency=args.concurrency,
        )

    table = Table(title='IAM sync summary', show_header=True)
    table.add_column('outcome', style='cyan')
    table.add_column('count', justify='right')
    for key in sorted(tally.keys()):
        table.add_row(key, str(tally[key]))
    console.print(table)


@iam_sync_all_cmd.args()
def _(parser: ArgParser):
    parser.add_argument('--type', action='append', default=None,
                        choices=list(IAM_SYNCABLE_USER_TYPES),
                        help='Restrict to user.type (repeatable; default: all '
                             'IAM-syncable types)')
    parser.add_argument('--max-users', type=int, default=None,
                        help='Cap the number of users synced (for testing)')
    parser.add_argument('--concurrency', type=int, default=1,
                        help='Concurrent IAM lookups (default: 1)')
    parser.add_argument('--grace-days', type=int, default=None,
                        help='Override IAMConfig.grace_days for this run')
    parser.add_argument('--expiry-offset-days', type=int, default=None,
                        help='Override IAMConfig.expiry_offset_days for this run')


# ---------------------------------------------------------------------------
# `ng iam show --user NAME` — read-only inspection
# ---------------------------------------------------------------------------


@user_args.apply(required=True)
@commands.register('ng', 'iam', 'show',
                   help='Show stored IAM bookkeeping for a user (no IAM call)')
async def iam_show_cmd(args: Namespace):
    console = Console()
    user = await User.find_one(User.name == args.user)
    if user is None:
        console.print(f'[red]User {args.user} not found[/]')
        return 1

    cfg = args.config.ucdiam
    table = Table(show_header=False, box=None, pad_edge=False, padding=(0, 1))
    table.add_column(style='bold cyan', no_wrap=True)
    table.add_column()
    table.add_row('name', user.name)
    table.add_row('type', user.type)
    table.add_row('status', user.status)
    table.add_row('expires_at', str(user.expires_at) if user.expires_at else '—')

    if user.iam is None:
        table.add_row('iam', '[dim](no IAM data)[/]')
    else:
        iam = user.iam
        status_style = 'red' if iam.iam_status == 'missing' else 'green'
        table.add_row('iam_status', f'[{status_style}]{iam.iam_status}[/]')
        if iam.person is not None:
            table.add_row('iam_id', str(iam.person.iam_id))
            table.add_row('mothra_id', str(iam.person.mothra_id))
            table.add_row(
                'user_types',
                ', '.join(iam.person.user_types) or '[dim](none)[/]',
            )
        else:
            table.add_row('iam_id', '[dim](no snapshot captured yet)[/]')
        table.add_row('iam_synced_at', str(iam.iam_synced_at) if iam.iam_synced_at else '—')
        table.add_row('last_seen_at', str(iam.last_seen_at) if iam.last_seen_at else '—')
        table.add_row('first_missing_at', str(iam.first_missing_at) if iam.first_missing_at else '—')

        # Project the next state transition based on current bookkeeping.
        if iam.first_missing_at is not None:
            now = datetime.now(timezone.utc)
            # Some test datasets store naive datetimes; normalize for the math.
            fma = iam.first_missing_at
            if fma.tzinfo is None:
                fma = fma.replace(tzinfo=timezone.utc)
            elapsed = now - fma
            grace_remaining = timedelta(days=cfg.grace_days) - elapsed
            if grace_remaining.total_seconds() > 0:
                table.add_row(
                    'grace remaining',
                    f'{grace_remaining.days}d {grace_remaining.seconds // 3600}h',
                )
                projected = now + grace_remaining + timedelta(
                    days=cfg.expiry_offset_days
                )
                table.add_row('would expire on', str(projected))
            else:
                table.add_row(
                    'grace remaining',
                    '[red]past — next sync will set expires_at[/]',
                )

        if iam.person is not None and iam.person.associations:
            sub = Table(box=None, pad_edge=False, padding=(0, 1))
            sub.add_column('dept')
            sub.add_column('title')
            sub.add_column('class')
            for a in iam.person.associations:
                sub.add_row(a.dept_name, a.title, a.title_type)
            table.add_row('associations', sub)

    console.print(Panel(table, title=f'[bold]IAM:[/] [green]{user.name}[/]',
                        border_style='cyan', expand=False))


# ---------------------------------------------------------------------------
# `ng iam reap` — flip offboarding -> inactive when expires_at has passed
# ---------------------------------------------------------------------------


@commands.register('ng', 'iam', 'reap',
                   help='Flip offboarding users whose expires_at has passed '
                        'to inactive')
async def iam_reap_cmd(args: Namespace):
    console = Console()
    if args.dry_run:
        # Read-only preview: do the same query the op would.
        from beanie.operators import LTE
        users = await User.find(
            User.status == 'offboarding',
            LTE(User.expires_at, datetime.now(timezone.utc)),
        ).to_list()
        if not users:
            console.print('[dim](no users to reap)[/]')
            return
        console.print(f'[yellow]Would reap {len(users)} user(s):[/]')
        for u in users:
            console.print(f'  [yellow]{u.name}[/] (expires_at={u.expires_at})')
        return

    reaped = await ReapOffboardedUsers.run(args.db, args.author)
    if not reaped:
        console.print('[dim](no users to reap)[/]')
        return
    console.print(f'[green]Reaped {len(reaped)} user(s):[/]')
    for name in reaped:
        console.print(f'  [green]{name}[/]')


@iam_reap_cmd.args()
def _(parser: ArgParser):
    parser.add_argument('--dry-run', action='store_true', default=False,
                        help='Show users that would be reaped, but do not '
                             'flip their status')


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _outcome_style(outcome: str) -> str:
    if outcome.startswith('hit'):
        return 'green'
    if outcome.startswith('miss_offboarding'):
        return 'red'
    if outcome.startswith('miss'):
        return 'yellow'
    return 'magenta'
