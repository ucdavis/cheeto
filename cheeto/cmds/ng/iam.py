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
from ...hippo import email_notifier
from ...iam_async import AsyncIAMAPI
from ...log import Console
from ...models.user import User
from ...operations import (
    ReapOffboardedUsers,
    SyncAllUsersIAM,
    SyncUserIAM,
)
from ...operations.iam import maybe_notify_offboarding, maybe_notify_restored
from ...queries import find_restorable_users, resolve_status_name
from ...yaml import print_yaml
from . import parent
from ._args import user_args, yaml_args


parent('ng', 'iam', help='UC Davis IAM sync operations (async/beanie)')
parent('ng', 'iam', 'list', help='IAM reconciliation reports')


# ---------------------------------------------------------------------------
# `ng iam sync --user NAME`
# ---------------------------------------------------------------------------


@user_args.apply(multiple=True)
@commands.register('ng', 'iam', 'sync',
                   help='Sync users against the IAM API '
                        '(-u USER... or --all)')
async def iam_sync_cmd(args: Namespace):
    console = Console()
    if bool(args.user) == bool(args.all):
        console.print('[red]Specify either -u/--user or --all[/]')
        return 1

    cfg = args.config.ucdiam
    grace = args.grace_days if args.grace_days is not None else cfg.grace_days
    offset = (
        args.expiry_offset_days if args.expiry_offset_days is not None
        else cfg.expiry_offset_days
    )

    if args.all:
        types = args.type or list(IAM_SYNCABLE_USER_TYPES)
        async with AsyncIAMAPI(cfg) as iam_api:
            with email_notifier(args.config.hippo, db=args.db,
                                author=args.author,
                                enabled=args.notify) as notify:
                tally = await SyncAllUsersIAM.run(
                    args.db, args.author,
                    iam_api=iam_api,
                    grace_days=grace,
                    expiry_offset_days=offset,
                    types=types,
                    max_users=args.max_users,
                    concurrency=args.concurrency,
                    notifier=notify,
                )
        table = Table(title='IAM sync summary', show_header=True)
        table.add_column('outcome', style='cyan')
        table.add_column('count', justify='right')
        for key in sorted(tally.keys()):
            table.add_row(key, str(tally[key]))
        console.print(table)
        return 0

    failed: list[str] = []
    async with AsyncIAMAPI(cfg) as iam_api:
        with email_notifier(args.config.hippo, db=args.db, author=args.author,
                            enabled=args.notify) as notify:
            for username in args.user:
                try:
                    result = await SyncUserIAM.run(
                        args.db, args.author,
                        username=username,
                        iam_api=iam_api,
                        grace_days=grace,
                        expiry_offset_days=offset,
                    )
                except ValueError as e:
                    console.print(f'  [red]{username}: {e}[/]')
                    failed.append(username)
                    continue
                if await maybe_notify_offboarding(result, notify):
                    console.print('[dim]sent offboarding notification[/]')
                if await maybe_notify_restored(result, notify):
                    console.print('[dim]sent restoration notification[/]')
                style = _outcome_style(result.outcome)
                console.print(
                    f'  [bold]{result.username}[/] -> '
                    f'[{style}]{result.outcome}[/] '
                    f'(status={result.status}, '
                    f'expires_at={result.expires_at or "—"})'
                )
    if failed:
        console.print(
            f'[red]{len(failed)}/{len(args.user)} failed:[/] '
            f'{", ".join(failed)}'
        )
        return 1
    return 0


@iam_sync_cmd.args()
def _(parser: ArgParser):
    parser.add_argument('--all', action='store_true', default=False,
                        help='Sync every IAM-syncable user (bulk mode)')
    parser.add_argument('--type', action='append', default=None,
                        choices=list(IAM_SYNCABLE_USER_TYPES),
                        help='With --all: restrict to user.type (repeatable; '
                             'default: all IAM-syncable types)')
    parser.add_argument('--max-users', type=int, default=None,
                        help='With --all: cap the number of users synced')
    parser.add_argument('--concurrency', type=int, default=1,
                        help='With --all: concurrent IAM lookups (default: 1)')
    parser.add_argument('--grace-days', type=int, default=None,
                        help='Override IAMConfig.grace_days for this run')
    parser.add_argument('--expiry-offset-days', type=int, default=None,
                        help='Override IAMConfig.expiry_offset_days for this run')
    parser.add_argument('--notify', action='store_true', default=False,
                        help='Email users this sync moves into offboarding '
                             'or restores from it')


# ---------------------------------------------------------------------------
# `ng iam show --user NAME` — read-only inspection
# ---------------------------------------------------------------------------


@user_args.apply(required=True)
@yaml_args.apply()
@commands.register('ng', 'iam', 'show',
                   help='Show stored IAM bookkeeping for a user (no IAM call)')
async def iam_show_cmd(args: Namespace):
    console = Console()
    user = await User.find_one(User.name == args.user)
    if user is None:
        console.print(f'[red]User {args.user} not found[/]')
        return 1

    cfg = args.config.ucdiam

    if args.yaml:
        iam_data = None
        if user.iam is not None:
            iam = user.iam
            person = None
            if iam.person is not None:
                person = {
                    'iam_id': iam.person.iam_id,
                    'mothra_id': iam.person.mothra_id,
                    'user_types': list(iam.person.user_types),
                    'associations': [
                        {'dept': a.dept_name, 'title': a.title,
                         'class': a.title_type}
                        for a in iam.person.associations
                    ],
                }
            iam_data = {
                'iam_status': iam.iam_status,
                'iam_synced_at': iam.iam_synced_at,
                'last_seen_at': iam.last_seen_at,
                'first_missing_at': iam.first_missing_at,
                'person': person,
            }
        print_yaml({
            'name': user.name,
            'type': user.type,
            'status': await resolve_status_name(user.status),
            'expires_at': user.expires_at,
            'iam': iam_data,
        })
        return 0

    table = Table(show_header=False, box=None, pad_edge=False, padding=(0, 1))
    table.add_column(style='bold cyan', no_wrap=True)
    table.add_column()
    table.add_row('name', user.name)
    table.add_row('type', user.type)
    table.add_row(
        'status', await resolve_status_name(user.status) or '[dim](none)[/]',
    )
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

    with email_notifier(args.config.hippo, db=args.db, author=args.author,
                        enabled=args.notify) as notify:
        reaped = await ReapOffboardedUsers.run(args.db, args.author,
                                               notifier=notify)
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
    parser.add_argument('--notify', action='store_true', default=False,
                        help='Email each user when their account is '
                             'deactivated')


# ---------------------------------------------------------------------------
# `ng iam restorable` — inactive/disabled accounts whose IAM record is back
# ---------------------------------------------------------------------------


@yaml_args.apply()
@commands.register('ng', 'iam', 'list', 'restorable',
                   help='List users whose IAM record is valid again but '
                        'whose status was never restored (e.g. returned '
                        'after the offboarding window)')
async def iam_restorable_cmd(args: Namespace):
    console = Console()
    users = await find_restorable_users(status_names=tuple(args.status))

    if args.yaml:
        print_yaml([
            {
                'name': u.name,
                'email': u.email,
                'type': u.type,
                'status': await resolve_status_name(u.status),
                'iam_id': (
                    u.iam.person.iam_id
                    if u.iam and u.iam.person else None
                ),
                'last_seen_at': u.iam.last_seen_at if u.iam else None,
                'iam_synced_at': u.iam.iam_synced_at if u.iam else None,
                'expires_at': u.expires_at,
            }
            for u in users
        ])
        return 0

    if not users:
        console.print('[dim](no restorable users)[/]')
        return

    table = Table(box=None, pad_edge=False, padding=(0, 1))
    table.add_column('user', style='bold')
    table.add_column('email')
    table.add_column('type', style='dim')
    table.add_column('status', style='yellow')
    table.add_column('iam_id')
    table.add_column('last_seen_at', style='green')
    table.add_column('iam_synced_at', style='dim')
    table.add_column('expired_at')
    for u in users:
        iam = u.iam
        table.add_row(
            u.name, u.email, u.type,
            await resolve_status_name(u.status) or '—',
            str(iam.person.iam_id) if iam and iam.person else '—',
            str(iam.last_seen_at) if iam and iam.last_seen_at else '—',
            str(iam.iam_synced_at) if iam and iam.iam_synced_at else '—',
            str(u.expires_at) if u.expires_at else '—',
        )
    console.print(Panel(
        table,
        title=f'[bold]Restorable users[/] ({len(users)})',
        border_style='cyan', expand=False,
    ))
    console.print(
        '[dim]Reactivate with: '
        'cheeto ng user set status -u <name> --status active --reason ...[/]'
    )


@iam_restorable_cmd.args()
def _(parser: ArgParser):
    parser.add_argument('--status', nargs='+',
                        choices=('inactive', 'disabled', 'offboarding'),
                        default=['inactive'],
                        help='Which cheeto statuses to scan '
                             '(default: inactive)')


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
