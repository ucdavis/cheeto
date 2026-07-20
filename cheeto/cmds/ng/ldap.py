"""`cheeto ng ldap` — async LDAP sync commands.

Wraps the BootstrapLDAPSite / SyncSiteLDAP / SyncUserToLDAP /
SyncGroupToLDAP / PruneSiteLDAP operations behind ergonomic CLI flags.

Mirrors the IAM CLI shape in `cheeto/cmds/ng/iam.py`. The async LDAP
manager is constructed inside each handler via `async with` so the pool
opens/closes cleanly per invocation.
"""

from __future__ import annotations

import secrets
from argparse import Namespace

from ponderosa import ArgParser
from rich.panel import Panel
from rich.table import Table

from .. import commands
from ...ldap_async import (
    AUTO_GROUP,
    AUTO_HOME,
    AUTO_MASTER,
    AsyncLDAPManager,
    LDAPPruneAborted,
)
from ...log import Console
from ...models.user import User
from ...operations import (
    BackfillLDAPInfo,
    BootstrapLDAPSite,
    ClearLDAPTree,
    PruneSiteLDAP,
    SyncGroupToLDAP,
    SyncSiteLDAP,
    SyncUserToLDAP,
)
from ...yaml import print_yaml
from ._args import group_args, site_args, user_args, yaml_args


@commands.register('ng', 'ldap',
                   help='Async LDAP sync (bonsai)')
def ldap_cmd(args: Namespace):
    pass


# ---------------------------------------------------------------------------
# `ng ldap bootstrap`
# ---------------------------------------------------------------------------


@site_args.apply(required=True)
@commands.register('ng', 'ldap', 'bootstrap',
                   help='Create the LDAP tree skeleton + special groups for a site')
async def ldap_bootstrap_cmd(args: Namespace):
    console = Console()
    cfg = args.config.ldap
    async with AsyncLDAPManager(cfg, sitename=args.site) as ldap:
        result = await BootstrapLDAPSite.run(
            args.db, args.author,
            sitename=args.site, ldap=ldap,
        )

    table = Table(title=f'Bootstrap result for {args.site}')
    table.add_column('phase', style='cyan')
    table.add_column('dn / name', style='green')
    table.add_column('status')
    for dn, status in result['tree'].items():
        style = 'yellow' if status == 'created' else 'dim'
        table.add_row('tree', dn, f'[{style}]{status}[/]')
    for name, status in result['special_groups'].items():
        style = 'yellow' if status == 'created' else 'dim'
        table.add_row('special_group', name, f'[{style}]{status}[/]')
    console.print(table)


# ---------------------------------------------------------------------------
# `ng ldap backfill`
# ---------------------------------------------------------------------------


@commands.register('ng', 'ldap', 'backfill',
                   help='Persist LDAP dirty-tracking state on documents '
                        'that predate the LDAPSyncable field')
async def ldap_backfill_cmd(args: Namespace):
    console = Console()
    result = await BackfillLDAPInfo.run(args.db, args.author)
    table = Table(title='LDAP dirty-tracking backfill')
    table.add_column('collection', style='cyan')
    table.add_column('backfilled', justify='right', style='green')
    for label, count in result.items():
        table.add_row(label, str(count))
    console.print(table)


# ---------------------------------------------------------------------------
# `ng ldap sync-site`
# ---------------------------------------------------------------------------


@site_args.apply(required=True)
@commands.register('ng', 'ldap', 'sync-site',
                   help='Sync all users, groups, and automounts for a site')
async def ldap_sync_site_cmd(args: Namespace):
    console = Console()
    cfg = args.config.ldap
    scope = (
        [s.strip() for s in args.scope.split(',') if s.strip()]
        if args.scope else None
    )
    async with AsyncLDAPManager(cfg, sitename=args.site) as ldap:
        result = await SyncSiteLDAP.run(
            args.db, args.author,
            sitename=args.site, ldap=ldap,
            force=args.force,
            full=args.full,
            concurrency=args.concurrency,
            scope=scope,
            prune=not args.no_prune,
            max_deletions=(
                None if args.max_deletions is not None and args.max_deletions < 0
                else args.max_deletions
            ),
            dry_run=args.dry_run,
        )

    _print_sync_site_result(console, args.site, result)


def _print_sync_site_result(console: Console, sitename: str, result: dict):
    """Render the SyncSiteLDAP result dict as Rich tables."""
    panel_table = Table.grid(padding=(0, 1))
    panel_table.add_column(style='bold cyan', no_wrap=True)
    panel_table.add_column()

    users = result.get('users') or {}
    groups = result.get('groups') or {}
    automounts = result.get('automounts') or {}
    pruned = result.get('pruned') or {}

    panel_table.add_row(
        'users',
        ', '.join(f'{k}={v}' for k, v in users.items() if v) or '(none)',
    )
    panel_table.add_row(
        'groups',
        ', '.join(f'{k}={v}' for k, v in groups.items() if v) or '(none)',
    )
    panel_table.add_row(
        'automounts',
        ', '.join(f'{k}={v}' for k, v in automounts.items()) or '(none)',
    )
    if pruned:
        if 'aborted' in pruned:
            wd = pruned.get('would_delete') or {}
            panel_table.add_row(
                'pruned',
                f'[red]aborted[/]: would delete '
                + ', '.join(f'{k}={len(v)}' for k, v in wd.items()),
            )
        else:
            panel_table.add_row(
                'pruned',
                ', '.join(
                    f'{k}={len(v) if isinstance(v, list) else v}'
                    for k, v in pruned.items()
                ) or '(none)',
            )
    console.print(Panel(panel_table,
                        title=f'[bold]ng ldap sync-site:[/] [green]{sitename}[/]',
                        border_style='green', expand=False))


@ldap_sync_site_cmd.args()
def _(parser: ArgParser):
    parser.add_argument('--force', action='store_true', default=False,
                        help='Delete-and-recreate user/group dns instead of '
                             'patching in place')
    parser.add_argument('--full', action='store_true', default=False,
                        help='Sync all records, not just changed ones '
                             '(normal upsert; unlike --force, no '
                             'delete-recreate)')
    parser.add_argument('--concurrency', type=int, default=1,
                        help='Concurrent per-user syncs (default 1)')
    parser.add_argument('--scope', default=None,
                        help='Comma-separated subset of '
                             "users,groups,automounts,prune")
    parser.add_argument('--no-prune', action='store_true', default=False,
                        help='Skip the prune phase entirely')
    parser.add_argument('--max-deletions', type=int, default=50,
                        help='Abort prune if it would delete more than N '
                             'entries (default 50; pass -1 to disable)')
    parser.add_argument('--dry-run', action='store_true', default=False,
                        help='Show would-delete list for prune; no LDAP writes')


# ---------------------------------------------------------------------------
# `ng ldap sync-user`
# ---------------------------------------------------------------------------


@site_args.apply(required=True)
@user_args.apply(required=True)
@commands.register('ng', 'ldap', 'sync-user',
                   help='Sync one user to LDAP (upsert dn + reconcile groups)')
async def ldap_sync_user_cmd(args: Namespace):
    console = Console()
    cfg = args.config.ldap
    async with AsyncLDAPManager(cfg, sitename=args.site) as ldap:
        result = await SyncUserToLDAP.run(
            args.db, args.author,
            username=args.user, sitename=args.site,
            ldap=ldap, force=args.force, full=args.full,
        )

    style = {
        'created': 'green', 'updated': 'cyan', 'recreated': 'yellow',
        'memberships_only': 'cyan', 'no_op': 'dim',
    }.get(result.outcome, 'magenta')
    added = result.extra.get('added_groups') or []
    removed = result.extra.get('removed_groups') or []
    console.print(
        f'[bold]{result.name}[/] -> [{style}]{result.outcome}[/]',
    )
    if added:
        console.print(f'  [green]+ groups:[/] {", ".join(added)}')
    if removed:
        console.print(f'  [red]- groups:[/] {", ".join(removed)}')


@ldap_sync_user_cmd.args()
def _(parser: ArgParser):
    parser.add_argument('--force', action='store_true', default=False,
                        help='Delete-and-recreate the user dn')
    parser.add_argument('--full', action='store_true', default=False,
                        help='Sync even if unchanged (normal upsert; '
                             'unlike --force, no delete-recreate)')


# ---------------------------------------------------------------------------
# `ng ldap sync-group`
# ---------------------------------------------------------------------------


@site_args.apply(required=True)
@group_args.apply(required=True)
@commands.register('ng', 'ldap', 'sync-group',
                   help='Sync one beanie Group to LDAP (regular groups only)')
async def ldap_sync_group_cmd(args: Namespace):
    console = Console()
    cfg = args.config.ldap
    async with AsyncLDAPManager(cfg, sitename=args.site) as ldap:
        result = await SyncGroupToLDAP.run(
            args.db, args.author,
            groupname=args.group, sitename=args.site,
            ldap=ldap, force=args.force, full=args.full,
        )

    console.print(
        f'[bold]{result.name}[/] -> [cyan]{result.outcome}[/] '
        f'(members={result.extra.get("member_count", 0)})',
    )


@ldap_sync_group_cmd.args()
def _(parser: ArgParser):
    parser.add_argument('--force', action='store_true', default=False,
                        help='Delete-and-recreate the group dn')
    parser.add_argument('--full', action='store_true', default=False,
                        help='Sync even if unchanged (normal upsert; '
                             'unlike --force, no delete-recreate)')


# ---------------------------------------------------------------------------
# `ng ldap prune-site`
# ---------------------------------------------------------------------------


@site_args.apply(required=True)
@commands.register('ng', 'ldap', 'prune-site',
                   help='Delete LDAP entries that have no beanie record')
async def ldap_prune_site_cmd(args: Namespace):
    console = Console()
    cfg = args.config.ldap
    scope = (
        [s.strip() for s in args.scope.split(',') if s.strip()]
        if args.scope else None
    )
    async with AsyncLDAPManager(cfg, sitename=args.site) as ldap:
        try:
            result = await PruneSiteLDAP.run(
                args.db, args.author,
                sitename=args.site, ldap=ldap,
                scope=scope,
                max_deletions=(
                    None if args.max_deletions is not None
                    and args.max_deletions < 0
                    else args.max_deletions
                ),
                dry_run=args.dry_run,
            )
        except LDAPPruneAborted as e:
            console.print(f'[red]Prune aborted:[/] {e}')
            for cat, items in e.would_delete.items():
                if items:
                    console.print(f'  [yellow]{cat}[/] ({len(items)}):')
                    for item in items:
                        console.print(f'    {item}')
            return 1

    title = 'Prune (dry-run)' if args.dry_run else 'Pruned'
    table = Table(title=f'{title}: {args.site}')
    table.add_column('phase', style='cyan')
    table.add_column('count', justify='right')
    table.add_column('entries', style='dim')
    for phase, items in result.items():
        table.add_row(phase, str(len(items)), ', '.join(items[:5]) + (
            ' ...' if len(items) > 5 else ''
        ))
    console.print(table)


@ldap_prune_site_cmd.args()
def _(parser: ArgParser):
    parser.add_argument('--scope', default=None,
                        help='Comma-separated subset of users,groups,automounts')
    parser.add_argument('--max-deletions', type=int, default=50,
                        help='Abort if would delete more than N entries '
                             '(default 50; pass -1 to disable)')
    parser.add_argument('--dry-run', action='store_true', default=False,
                        help='Show would-delete list without writing')


# ---------------------------------------------------------------------------
# `ng ldap clear-tree`
# ---------------------------------------------------------------------------


def _confirm_clear_tree(
    console: Console, base: str, exclude_bases: list[str], count: int,
) -> bool:
    """Typed 6-digit confirmation before a destructive clear. Mirrors
    the migrate --drop confirm flow. Returns True only on exact match."""
    code = f'{secrets.randbelow(1_000_000):06d}'
    console.print()
    console.rule(
        '[bold red]DANGER: clear-tree will PERMANENTLY DELETE LDAP entries[/]',
        style='red',
    )
    console.print(f'[bold red]Base:[/] [yellow]{base}[/]')
    console.print(f'[bold red]Entries to delete:[/] [yellow]{count}[/]')
    if exclude_bases:
        console.print('[bold red]Excluded subtrees:[/]')
        for eb in exclude_bases:
            console.print(f'  [green]•[/] [dim]{eb}[/]')
    console.print(
        '\n[bold red]This action is IRREVERSIBLE.[/]\n'
        f'To confirm, re-type this code exactly: [bold yellow]{code}[/]'
    )
    try:
        entered = input('Confirmation code: ').strip()
    except (EOFError, KeyboardInterrupt):
        console.print('\n[red]Aborted: no confirmation received.[/]')
        return False
    if entered != code:
        console.print('[red]Aborted: confirmation code did not match.[/]')
        return False
    console.print('[green]Confirmed. Proceeding.[/]\n')
    return True


@commands.register(
    'ng', 'ldap', 'clear-tree',
    help='Delete every LDAP entry under the searchbase (excluding Services)',
)
async def ldap_clear_tree_cmd(args: Namespace):
    console = Console()
    cfg = args.config.ldap
    exclude_bases = (
        [b.strip() for b in args.exclude.split(',') if b.strip()]
        if args.exclude else None
    )
    max_deletions = (
        None if args.max_deletions is not None and args.max_deletions < 0
        else args.max_deletions
    )
    # sitename is unused for clear-tree but required by the manager.
    sitename = args.site or '__clear__'
    async with AsyncLDAPManager(cfg, sitename=sitename) as ldap:
        try:
            preview = await ClearLDAPTree.run(
                args.db, args.author,
                ldap=ldap,
                base=args.base,
                exclude_bases=exclude_bases,
                max_deletions=max_deletions,
                dry_run=True,
            )
        except LDAPPruneAborted as e:
            console.print(f'[red]Aborted:[/] {e}')
            for cat, items in e.would_delete.items():
                console.print(f'  [yellow]{cat}[/] ({len(items)} entries):')
                for dn in items[:25]:
                    console.print(f'    {dn}')
                if len(items) > 25:
                    console.print(f'    ... {len(items) - 25} more')
            return 1

        will_delete = preview['would_delete']
        if not will_delete:
            console.print('[green]Nothing to delete.[/]')
            return

        for dn in will_delete[:25]:
            console.print(f'  [red]-[/] {dn}')
        if len(will_delete) > 25:
            console.print(f'  ... {len(will_delete) - 25} more')

        if args.dry_run:
            console.print(
                f'[yellow]dry-run:[/] {len(will_delete)} DNs would be deleted',
            )
            return

        base = args.base or cfg.searchbase
        if not _confirm_clear_tree(
            console, base,
            exclude_bases or [f'ou=Services,{cfg.searchbase}'],
            len(will_delete),
        ):
            return 1

        result = await ClearLDAPTree.run(
            args.db, args.author,
            ldap=ldap,
            base=args.base,
            exclude_bases=exclude_bases,
            max_deletions=max_deletions,
            dry_run=False,
        )

    console.print(
        f'[green]Deleted[/] {result["count"]} LDAP entries',
    )


@ldap_clear_tree_cmd.args()
def _(parser: ArgParser):
    parser.add_argument(
        '--base', default=None,
        help='Base DN to clear (default: LDAPConfig.searchbase)',
    )
    parser.add_argument(
        '--exclude', default=None,
        help='Comma-separated DNs to exclude (subtree match). Default: '
             'ou=Services,<searchbase>',
    )
    parser.add_argument(
        '--site', default=None,
        help='Sitename for the LDAP manager (cosmetic; clear-tree is '
             'site-agnostic). Default: synthetic placeholder.',
    )
    parser.add_argument(
        '--max-deletions', type=int, default=200,
        help='Abort if more than N entries would be deleted '
             '(default 200; pass -1 to disable)',
    )
    parser.add_argument(
        '--dry-run', action='store_true', default=False,
        help='Show what would be deleted without prompting or writing',
    )


# ---------------------------------------------------------------------------
# `ng ldap show ...`
# ---------------------------------------------------------------------------


@commands.register('ng', 'ldap', 'show',
                   help='Read-only inspection of LDAP-side state')
def ldap_show_cmd(args: Namespace):
    pass


def _password_display(password: str | None) -> str:
    """Presence + scheme only — the hash itself must never be printed."""
    if not password:
        return 'not set'
    if password.startswith('{') and '}' in password:
        return f'set ({password[:password.index("}") + 1]})'
    return 'set (no scheme!)'


@site_args.apply(required=True)
@user_args.apply(required=True)
@yaml_args.apply()
@commands.register('ng', 'ldap', 'show', 'user',
                   help='Show LDAP record for a user')
async def ldap_show_user_cmd(args: Namespace):
    console = Console()
    cfg = args.config.ldap
    async with AsyncLDAPManager(cfg, sitename=args.site) as ldap:
        record = await ldap.get_user(args.user)
        memberships = (
            await ldap.list_user_memberships(args.user)
            if record is not None else set()
        )
    if record is None:
        console.print(f'[red]No LDAP entry for {args.user}[/]')
        return 1
    if args.yaml:
        print_yaml({
            'username': record.username,
            'uid': record.uid,
            'gid': record.gid,
            'email': record.email,
            'home_directory': record.home_directory,
            'shell': record.shell,
            'password': _password_display(record.password),
            'ssh_keys': list(record.ssh_keys),
            'memberships': sorted(memberships),
        })
        return 0
    table = Table.grid(padding=(0, 1))
    table.add_column(style='bold cyan', no_wrap=True)
    table.add_column()
    table.add_row('username', record.username)
    table.add_row('uid', str(record.uid))
    table.add_row('gid', str(record.gid))
    table.add_row('email', record.email)
    table.add_row('home', record.home_directory)
    table.add_row('shell', record.shell)
    table.add_row(
        'password',
        _password_display(record.password) if record.password
        else '[dim]not set[/]',
    )
    table.add_row(
        'ssh_keys',
        '\n'.join(record.ssh_keys) if record.ssh_keys else '(none)',
    )
    table.add_row(
        'memberships',
        ', '.join(sorted(memberships)) if memberships else '(none)',
    )
    console.print(Panel(table, title=f'[bold]LDAP user:[/] [green]{args.user}[/]',
                        border_style='green', expand=False))


@site_args.apply(required=True)
@group_args.apply(required=True)
@yaml_args.apply()
@commands.register('ng', 'ldap', 'show', 'group',
                   help='Show LDAP record for a group')
async def ldap_show_group_cmd(args: Namespace):
    console = Console()
    cfg = args.config.ldap
    async with AsyncLDAPManager(cfg, sitename=args.site) as ldap:
        record = await ldap.get_group(args.group)
    if record is None:
        console.print(f'[red]No LDAP entry for {args.group}[/]')
        return 1
    if args.yaml:
        print_yaml({
            'groupname': record.groupname,
            'gid': record.gid,
            'members': sorted(record.members),
        })
        return 0
    console.print(
        f'[bold]{record.groupname}[/] gid={record.gid} '
        f'members=[dim]{", ".join(sorted(record.members)) or "(none)"}[/]',
    )


@site_args.apply(required=True)
@yaml_args.apply()
@commands.register('ng', 'ldap', 'show', 'site',
                   help='Probe site OU tree + special-group presence')
async def ldap_show_site_cmd(args: Namespace):
    console = Console()
    cfg = args.config.ldap
    from ...models.group import AccessGroup, StatusGroup
    async with AsyncLDAPManager(cfg, sitename=args.site) as ldap:
        site_ou = ldap.site_ou_dn()
        groups_ou = ldap.groups_ou_dn()
        automount_ou = ldap.automount_ou_dn()
        statuses = {
            'site_ou': await ldap.dn_exists(site_ou),
            'groups_ou': await ldap.dn_exists(groups_ou),
            'automount_ou': await ldap.dn_exists(automount_ou),
            AUTO_MASTER: await ldap.dn_exists(ldap.automount_map_dn(AUTO_MASTER)),
            AUTO_HOME: await ldap.dn_exists(ldap.automount_map_dn(AUTO_HOME)),
            AUTO_GROUP: await ldap.dn_exists(ldap.automount_map_dn(AUTO_GROUP)),
        }
        access_records = await AccessGroup.find_all().to_list()
        status_records = await StatusGroup.find_all().to_list()
        for record in access_records + status_records:
            statuses[f'group:{record.name}'] = await ldap.group_exists(record.name)

    if args.yaml:
        print_yaml(statuses)
        return 0

    table = Table(title=f'LDAP site state: {args.site}')
    table.add_column('check', style='cyan')
    table.add_column('present')
    for name, present in statuses.items():
        marker = '[green]✓[/]' if present else '[red]✗[/]'
        table.add_row(name, marker)
    console.print(table)


# ---------------------------------------------------------------------------
# `ng group seed-access-status` — beanie-side bootstrap
# ---------------------------------------------------------------------------
# Lives here rather than in cheeto/cmds/ng/group.py so the LDAP-related
# bootstrap commands cluster together. Uses the SeedAccessStatusGroups
# operation from cheeto.operations.group.


@commands.register('ng', 'group', 'seed-access-status',
                   help='Seed standard AccessGroup/StatusGroup records in beanie')
async def group_seed_access_status_cmd(args: Namespace):
    from ...operations import SeedAccessStatusGroups
    console = Console()
    result = await SeedAccessStatusGroups.run(
        args.db, args.author,
        gid_start=args.gid_start,
    )
    table = Table(title='AccessGroup / StatusGroup seed result')
    table.add_column('record name', style='green')
    table.add_column('status')
    for name, status in result.items():
        style = 'yellow' if status == 'created' else 'dim'
        table.add_row(name, f'[{style}]{status}[/]')
    console.print(table)


@group_seed_access_status_cmd.args()
def _(parser: ArgParser):
    parser.add_argument('--gid-start', type=int, default=6000,
                        help='Starting GID for newly-created records '
                             '(default 6000)')
