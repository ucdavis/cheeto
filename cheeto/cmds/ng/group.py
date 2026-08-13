from argparse import Namespace

from ponderosa import ArgParser
from rich.columns import Columns
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from .. import commands
from beanie.operators import In

from ...constants import GROUP_TYPES
from ...log import Console
from ...models.base import link_target_id
from ...models.group import Group
from ...models.group_membership import GroupMembership
from ...models.group_site_info import GroupSiteInfo
from ...models.site import Site
from ...operations import (
    AddGroupMember,
    AddGroupSlurmer,
    AddGroupSponsor,
    AddGroupSudoer,
    CreateClassGroup,
    CreateGroup,
    CreateGroupFromSponsor,
    CreateLabGroup,
    CreateSystemGroup,
    DeleteGroup,
    RemoveGroupMember,
    RemoveGroupSlurmer,
    RemoveGroupSponsor,
    RemoveGroupSudoer,
)
from ...queries import (
    find_groups,
    gather_group_references,
    group_members_at_site,
)
from ...yaml import print_yaml
from . import parent
from ._args import (
    confirm_typed,
    group_args,
    run_per_target,
    site_args,
    user_args,
    yaml_args,
)
from ._slurm_show import group_slurm_at_site


def _group_to_dict(group: Group, roster: dict[str, list[str]] | None = None) -> dict:
    data: dict = {
        'name': group.name,
        'gid': group.gid,
        'type': group.type,
        'created_at': group.created_at,
        'updated_at': group.updated_at,
    }
    if roster is not None:
        data['members'] = roster['members']
        data['sponsors'] = roster['sponsors']
        data['sudoers'] = roster['sudoers']
        data['slurmers'] = roster['slurmers']
    return data


def _render_group_slurm(site_name: str, slurm: dict | None) -> Table:
    table = Table(show_header=False, box=None, pad_edge=False, padding=(0, 1))
    table.add_column(style='bold magenta', no_wrap=True)
    table.add_column()

    if slurm is None:
        table.add_row('status', f'[dim](no slurm data for site {site_name})[/]')
        return table

    account = slurm.get('account')
    if account is not None:
        limits = account['limits']
        limits_str = ', '.join(f'{k}={v}' for k, v in limits.items())
        table.add_row('account limits', limits_str)
        coordinators = account['coordinators']
        table.add_row(
            'coordinators',
            ', '.join(coordinators) if coordinators else '[dim](none)[/]',
        )
    else:
        table.add_row('account', '[dim](none)[/]')

    assocs = slurm.get('associations') or []
    if not assocs:
        table.add_row('associations', '[dim](none)[/]')
    else:
        assoc_table = Table(box=None, pad_edge=False, padding=(0, 1))
        assoc_table.add_column('partition', style='green')
        assoc_table.add_column('qos', style='cyan')
        assoc_table.add_column('priority', style='yellow')
        assoc_table.add_column('flags', style='dim')
        assoc_table.add_column('total tres', style='bold')
        for a in assocs:
            assoc_table.add_row(
                a['partition'],
                a['qos'],
                str(a['qos_priority']),
                ', '.join(a['qos_flags']),
                a['qos_total_tres'],
            )
        table.add_row('associations', assoc_table)

    return table


def _render_group_panel(data: dict) -> Panel:
    table = Table(show_header=False, box=None, pad_edge=False, padding=(0, 1))
    table.add_column(style='bold cyan', no_wrap=True)
    table.add_column()

    for key in ('name', 'gid', 'type', 'created_at', 'updated_at'):
        if key in data and data[key] is not None:
            table.add_row(key, str(data[key]))

    if 'sites' in data:
        names = data['sites']
        table.add_row('sites', ', '.join(names) if names else '[dim](none)[/]')
    if data.get('site') and data.get('on_site') is False:
        table.add_row(
            'presence',
            f"[yellow]not attached to site {data['site']} "
            f"(`group add site`)[/]",
        )

    # Membership is per-site; the roster keys are only present when the
    # command was given a --site.
    if 'members' not in data:
        table.add_row(
            'membership',
            '[dim](pass --site to list members/sponsors/sudoers/slurmers)[/]',
        )
    else:
        for key in ('members', 'sponsors', 'sudoers', 'slurmers'):
            names = data.get(key) or []
            if not names:
                table.add_row(key, '[dim](none)[/]')
            elif key in ('members', 'slurmers'):
                table.add_row(key, Columns(
                    [Text(n, style='green') for n in names],
                    expand=True, equal=True,
                ))
            else:
                table.add_row(key, '\n'.join(names))

    if 'site' in data:
        table.add_row('site', data['site'])
        table.add_row(
            'slurm',
            _render_group_slurm(data['site'], data.get('slurm_at_site')),
        )

    return Panel(table, title=f'[bold]Group:[/] [green]{data["name"]}[/]',
                 border_style='green', expand=False)


parent('ng', 'group', help='Group operations')
parent('ng', 'group', 'new', help='Create new groups')
parent('ng', 'group', 'add', help='Add users to group roles at a site')
parent('ng', 'group', 'remove', help='Remove users from group roles at a site')


@yaml_args.apply()
@commands.register('ng', 'group', 'list',
                   help='List groups matching one or more filters '
                        '(combined by --operator)')
async def group_list(args: Namespace):
    console = Console()
    operator = (args.operator or 'AND').upper()
    if operator not in ('AND', 'OR'):
        console.print(
            f'[red]--operator must be AND or OR (got {args.operator!r})[/]'
        )
        return 1

    groups = await find_groups(
        type=args.type,
        site=args.site,
        user=args.user,
        operator=operator,
        include_hidden=args.all,
    )
    if args.limit is not None and args.limit > 0:
        groups = groups[:args.limit]

    member_counts: dict = {}
    if args.long and groups:
        query = [In(GroupMembership.group.id, [g.id for g in groups])]
        if args.site:
            site = await Site.find_one(Site.name == args.site)
            if site is not None:
                query.append(GroupMembership.site.id == site.id)
        for edge in await GroupMembership.find(*query).to_list():
            gid = link_target_id(edge.group)
            member_counts[gid] = member_counts.get(gid, 0) + 1

    if args.yaml:
        rows = [{
            'name': g.name,
            'gid': g.gid,
            'type': g.type,
            **({'members': member_counts.get(g.id, 0)} if args.long else {}),
        } for g in groups]
        print_yaml(rows)
        return

    title = f'Groups (count={len(groups)}, operator={operator})'
    table = Table(title=title)
    table.add_column('name', style='green', no_wrap=True)
    table.add_column('gid', justify='right')
    table.add_column('type', style='cyan')
    if args.long:
        table.add_column('members', justify='right')
    for g in groups:
        row = [g.name, str(g.gid), g.type]
        if args.long:
            row.append(str(member_counts.get(g.id, 0)))
        table.add_row(*row)
    console.print(table)


@group_list.args()
def _(parser: ArgParser):
    parser.add_argument('--type', default=None, choices=list(GROUP_TYPES),
                        help='Filter by group type')
    parser.add_argument('--site', '-s', default=None,
                        help='Filter to groups attached to a site '
                             '(GroupSiteInfo presence records)')
    parser.add_argument('--user', '-u', default=None,
                        help='Filter to groups a user is a member of')
    parser.add_argument('--operator', default='AND',
                        help='Combine filters with AND (default) or OR')
    parser.add_argument('--limit', '-n', type=int, default=None,
                        help='Maximum number of rows')
    parser.add_argument('--long', action='store_true', default=False,
                        help='Include a member-count column')
    parser.add_argument('--all', action='store_true', default=False,
                        help='Include personal (user-type) and access/'
                             'status infrastructure groups')


@group_args.apply(required=True)
@commands.register('ng', 'group', 'delete',
                   help='Delete a group and all of its references (cascade)')
async def group_delete(args: Namespace):
    from ...queries import find_group_by_name
    console = Console()
    group = await find_group_by_name(args.group)
    if group is None:
        console.print(f'[red]Group {args.group} not found[/]')
        return 1

    refs = await gather_group_references(group)
    table = Table(
        title=f'Records linked to [bold]{args.group}[/] (will be deleted)',
        show_header=False, box=None, pad_edge=False, padding=(0, 1),
    )
    table.add_column(style='cyan', no_wrap=True)
    table.add_column(justify='right')
    for label, n in refs.counts().items():
        table.add_row(label, str(n))
    console.print(table)

    if refs.storages and not args.force:
        names = ', '.join(sorted(st.name for st in refs.storages))
        console.print(
            f'[yellow]This group has {len(refs.storages)} storage '
            f'record(s) ({names}) — deleting them removes the exported '
            f'mount/quota configuration.[/]'
        )
        try:
            answer = input(
                'Also delete these storage records? [y/N]: '
            ).strip().lower()
        except (EOFError, KeyboardInterrupt):
            console.print('\n[yellow]Aborted[/]')
            return 1
        if answer != 'y':
            console.print(
                '[yellow]Aborted — storage records cannot be left behind '
                '(their group link would dangle)[/]'
            )
            return 1

    if not confirm_typed(console, 'group', args.group, force=args.force):
        return 1

    try:
        await DeleteGroup.run(
            args.db, args.author,
            name=args.group, reason=args.reason, cascade_storage=True,
        )
    except ValueError as e:
        console.print(f'[red]{e}[/]')
        return 1
    console.print(f'Deleted group [green]{args.group}[/]')
    console.print(
        '[dim]LDAP entries persist until the next `ng ldap prune`/site '
        'sync; Slurm state until the next `ng slurm sync`.[/]'
    )


@group_delete.args()
def _(parser: ArgParser):
    parser.add_argument('--reason', required=True,
                        help='Why the group is being deleted (recorded in '
                             'History)')
    parser.add_argument('--force', '-f', action='store_true', default=False,
                        help='Skip all confirmation prompts (cascades '
                             'storage records)')


async def _run_membership(args: Namespace, op, verbed: str) -> int:
    """Shared driver for the eight membership-role commands: loop the
    per-(user, group, site) operation over every target user."""
    console = Console()

    async def _one(name: str) -> None:
        await op.run(
            args.db, args.author,
            group_name=args.group, user_name=name, site_name=args.site,
        )

    return await run_per_target(
        console, args.user, _one,
        ok=f'{verbed} {args.group} at {args.site}',
    )


@group_args.apply(required=True)
@commands.register('ng', 'group', 'new', 'generic',
                   help='Create a new group with an explicit GID and type')
async def group_new_generic(args: Namespace):
    console = Console()
    group = await CreateGroup.run(
        args.db, args.author,
        name=args.group, gid=args.gid, type=args.type,
    )
    console.print(f'Created group [green]{group.name}[/] (gid={group.gid})')


@group_new_generic.args()
def _(parser: ArgParser):
    parser.add_argument('--gid', type=int, required=True)
    parser.add_argument('--type', default='group', choices=list(GROUP_TYPES))


@group_args.apply(required=True)
@commands.register('ng', 'group', 'new', 'system',
                   help='Create a new system group')
async def group_new_system(args: Namespace):
    console = Console()
    group = await CreateSystemGroup.run(
        args.db, args.author,
        name=args.group,
    )
    console.print(f'Created system group [green]{group.name}[/] (gid={group.gid})')


@group_args.apply(required=True)
@commands.register('ng', 'group', 'new', 'class',
                   help='Create a new class group')
async def group_new_class(args: Namespace):
    console = Console()
    group = await CreateClassGroup.run(
        args.db, args.author,
        name=args.group,
    )
    console.print(f'Created class group [green]{group.name}[/] (gid={group.gid})')


@group_args.apply(required=True)
@commands.register('ng', 'group', 'new', 'lab',
                   help='Create a new lab group')
async def group_new_lab(args: Namespace):
    console = Console()
    group = await CreateLabGroup.run(
        args.db, args.author,
        name=args.group,
    )
    console.print(f'Created lab group [green]{group.name}[/] (gid={group.gid})')


@site_args.apply(required=True)
@user_args.apply(required=True)
@commands.register('ng', 'group', 'new', 'from-sponsor',
                   help='Create a sponsor group from a user at a site')
async def group_from_sponsor(args: Namespace):
    console = Console()
    group = await CreateGroupFromSponsor.run(
        args.db, args.author,
        sponsor_name=args.user, site_name=args.site,
    )
    console.print(f'Created sponsor group [green]{group.name}[/] (gid={group.gid})')


@site_args.apply(required=True)
@user_args.apply(required=True, multiple=True)
@group_args.apply(required=True)
@commands.register('ng', 'group', 'add', 'member',
                   help='Add users to a group as members at a site')
async def group_add_member(args: Namespace):
    return await _run_membership(args, AddGroupMember, 'added to')


@site_args.apply(required=True)
@user_args.apply(required=True, multiple=True)
@group_args.apply(required=True)
@commands.register('ng', 'group', 'remove', 'member',
                   help='Remove member users from a group at a site')
async def group_remove_member(args: Namespace):
    return await _run_membership(args, RemoveGroupMember, 'removed from')


@site_args.apply(required=True)
@user_args.apply(required=True, multiple=True)
@group_args.apply(required=True)
@commands.register('ng', 'group', 'add', 'sponsor',
                   help='Add users as group sponsors at a site')
async def group_add_sponsor(args: Namespace):
    return await _run_membership(args, AddGroupSponsor, 'added as sponsor of')


@site_args.apply(required=True)
@user_args.apply(required=True, multiple=True)
@group_args.apply(required=True)
@commands.register('ng', 'group', 'remove', 'sponsor',
                   help='Remove sponsor role from users at a site')
async def group_remove_sponsor(args: Namespace):
    return await _run_membership(args, RemoveGroupSponsor, 'removed as sponsor of')


@site_args.apply(required=True)
@user_args.apply(required=True, multiple=True)
@group_args.apply(required=True)
@commands.register('ng', 'group', 'add', 'sudoer',
                   help='Add users as group sudoers at a site')
async def group_add_sudoer(args: Namespace):
    return await _run_membership(args, AddGroupSudoer, 'added as sudoer of')


@site_args.apply(required=True)
@user_args.apply(required=True, multiple=True)
@group_args.apply(required=True)
@commands.register('ng', 'group', 'remove', 'sudoer',
                   help='Remove sudoer role from users at a site')
async def group_remove_sudoer(args: Namespace):
    return await _run_membership(args, RemoveGroupSudoer, 'removed as sudoer of')


@site_args.apply(required=True)
@user_args.apply(required=True, multiple=True)
@group_args.apply(required=True)
@commands.register('ng', 'group', 'add', 'slurmer',
                   help='Add users as group slurmers at a site')
async def group_add_slurmer(args: Namespace):
    return await _run_membership(args, AddGroupSlurmer, 'added as slurmer of')


@site_args.apply(required=True)
@user_args.apply(required=True, multiple=True)
@group_args.apply(required=True)
@commands.register('ng', 'group', 'remove', 'slurmer',
                   help='Remove slurmer role from users at a site')
async def group_remove_slurmer(args: Namespace):
    return await _run_membership(args, RemoveGroupSlurmer, 'removed as slurmer of')


@site_args.apply()
@group_args.apply(required=True)
@commands.register('ng', 'group', 'show',
                   help='Show group information')
async def group_show(args: Namespace):
    console = Console()
    group = await Group.find_one(Group.name == args.group,
                                 fetch_links=True,
                                 with_children=True,
                                 nesting_depth=1)
    if group is None:
        console.print(f'[red]Group {args.group} not found[/]')
        return 1

    gsis = await GroupSiteInfo.find(
        GroupSiteInfo.group.id == group.id,
    ).to_list()
    gsi_site_ids = {
        sid for g in gsis
        if (sid := link_target_id(g.site)) is not None
    }
    gsi_sites = (
        await Site.find(In(Site.id, list(gsi_site_ids))).to_list()
        if gsi_site_ids else []
    )

    if args.site:
        site = await Site.find_one(Site.name == args.site)
        if site is None:
            console.print(f'[red]Site {args.site} not found[/]')
            return 1
        roster = await group_members_at_site(group, site)
        data = _group_to_dict(group, roster=roster)
        data['site'] = args.site
        data['on_site'] = site.id in gsi_site_ids
        data['slurm_at_site'] = await group_slurm_at_site(group, site)
    else:
        data = _group_to_dict(group)
    data['sites'] = sorted(s.name for s in gsi_sites)

    if args.yaml:
        print_yaml(data)
    else:
        console.print(_render_group_panel(data))


@group_show.args()
def _(parser: ArgParser):
    parser.add_argument('--yaml', action='store_true', default=False,
                        help='Output as YAML')
