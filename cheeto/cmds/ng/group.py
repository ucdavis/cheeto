from argparse import Namespace

from ponderosa import ArgParser
from rich.panel import Panel
from rich.table import Table

from .. import commands
from ...constants import GROUP_TYPES
from ...log import Console
from ...models.group import Group
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
    RemoveGroupMember,
    RemoveGroupSlurmer,
    RemoveGroupSponsor,
    RemoveGroupSudoer,
)
from ...yaml import dumps as dumps_yaml, highlight_yaml
from ._args import group_args, site_args, user_args
from ._slurm_show import group_slurm_at_site


def _group_to_dict(group: Group) -> dict:
    return {
        'name': group.name,
        'gid': group.gid,
        'type': group.type,
        'members': sorted(m.name for m in group.members),
        'sponsors': sorted(s.name for s in group.sponsors),
        'sudoers': sorted(s.name for s in group.sudoers),
        'slurmers': sorted(s.name for s in group.slurmers),
        'created_at': group.created_at,
        'updated_at': group.updated_at,
    }


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

    for key in ('members', 'sponsors', 'sudoers', 'slurmers'):
        names = data.get(key) or []
        if names:
            table.add_row(key, '\n'.join(names))
        else:
            table.add_row(key, '[dim](none)[/]')

    if 'site' in data:
        table.add_row('site', data['site'])
        table.add_row(
            'slurm',
            _render_group_slurm(data['site'], data.get('slurm_at_site')),
        )

    return Panel(table, title=f'[bold]Group:[/] [green]{data["name"]}[/]',
                 border_style='green', expand=False)


@commands.register('ng', 'group',
                   help='Group operations')
def group_cmd(args: Namespace):
    pass


@commands.register('ng', 'group', 'new',
                   help='Create a new group')
def group_new_cmd(args: Namespace):
    pass


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


@user_args.apply(required=True)
@commands.register('ng', 'group', 'from-sponsor',
                   help='Create a sponsor group from a user')
async def group_from_sponsor(args: Namespace):
    console = Console()
    group = await CreateGroupFromSponsor.run(
        args.db, args.author,
        sponsor_name=args.user,
    )
    console.print(f'Created sponsor group [green]{group.name}[/] (gid={group.gid})')


@user_args.apply(required=True)
@group_args.apply(required=True)
@commands.register('ng', 'group', 'add', 'member',
                   help='Add a user to a group as a member')
async def group_add_member(args: Namespace):
    console = Console()
    await AddGroupMember.run(
        args.db, args.author,
        group_name=args.group, user_name=args.user,
    )
    console.print(f'Added [green]{args.user}[/] to group [green]{args.group}[/]')


@user_args.apply(required=True)
@group_args.apply(required=True)
@commands.register('ng', 'group', 'remove', 'member',
                   help='Remove a user from a group')
async def group_remove_member(args: Namespace):
    console = Console()
    await RemoveGroupMember.run(
        args.db, args.author,
        group_name=args.group, user_name=args.user,
    )
    console.print(f'Removed [green]{args.user}[/] from group [green]{args.group}[/]')


@user_args.apply(required=True)
@group_args.apply(required=True)
@commands.register('ng', 'group', 'add', 'sponsor',
                   help='Add a user as a group sponsor')
async def group_add_sponsor(args: Namespace):
    console = Console()
    await AddGroupSponsor.run(
        args.db, args.author,
        group_name=args.group, user_name=args.user,
    )
    console.print(f'Added [green]{args.user}[/] as sponsor of [green]{args.group}[/]')


@user_args.apply(required=True)
@group_args.apply(required=True)
@commands.register('ng', 'group', 'remove', 'sponsor',
                   help='Remove a user as a group sponsor')
async def group_remove_sponsor(args: Namespace):
    console = Console()
    await RemoveGroupSponsor.run(
        args.db, args.author,
        group_name=args.group, user_name=args.user,
    )
    console.print(f'Removed [green]{args.user}[/] as sponsor of [green]{args.group}[/]')


@user_args.apply(required=True)
@group_args.apply(required=True)
@commands.register('ng', 'group', 'add', 'sudoer',
                   help='Add a user as a group sudoer')
async def group_add_sudoer(args: Namespace):
    console = Console()
    await AddGroupSudoer.run(
        args.db, args.author,
        group_name=args.group, user_name=args.user,
    )
    console.print(f'Added [green]{args.user}[/] as sudoer of [green]{args.group}[/]')


@user_args.apply(required=True)
@group_args.apply(required=True)
@commands.register('ng', 'group', 'remove', 'sudoer',
                   help='Remove a user as a group sudoer')
async def group_remove_sudoer(args: Namespace):
    console = Console()
    await RemoveGroupSudoer.run(
        args.db, args.author,
        group_name=args.group, user_name=args.user,
    )
    console.print(f'Removed [green]{args.user}[/] as sudoer of [green]{args.group}[/]')


@user_args.apply(required=True)
@group_args.apply(required=True)
@commands.register('ng', 'group', 'add', 'slurmer',
                   help='Add a user as a group slurmer')
async def group_add_slurmer(args: Namespace):
    console = Console()
    await AddGroupSlurmer.run(
        args.db, args.author,
        group_name=args.group, user_name=args.user,
    )
    console.print(f'Added [green]{args.user}[/] as slurmer of [green]{args.group}[/]')


@user_args.apply(required=True)
@group_args.apply(required=True)
@commands.register('ng', 'group', 'remove', 'slurmer',
                   help='Remove a user as a group slurmer')
async def group_remove_slurmer(args: Namespace):
    console = Console()
    await RemoveGroupSlurmer.run(
        args.db, args.author,
        group_name=args.group, user_name=args.user,
    )
    console.print(f'Removed [green]{args.user}[/] as slurmer of [green]{args.group}[/]')


@site_args.apply()
@group_args.apply(required=True)
@commands.register('ng', 'group', 'show',
                   help='Show group information')
async def group_show(args: Namespace):
    console = Console()
    group = await Group.find_one(Group.name == args.group, fetch_links=True, with_children=True)
    if group is None:
        console.print(f'[red]Group {args.group} not found[/]')
        return 1

    data = _group_to_dict(group)
    if args.site:
        site = await Site.find_one(Site.name == args.site)
        if site is None:
            console.print(f'[red]Site {args.site} not found[/]')
            return 1
        data['site'] = args.site
        data['slurm_at_site'] = await group_slurm_at_site(group, site)

    if args.yaml:
        console.print(highlight_yaml(dumps_yaml(data)))
    else:
        console.print(_render_group_panel(data))


@group_show.args()
def _(parser: ArgParser):
    parser.add_argument('--yaml', action='store_true', default=False,
                        help='Output as YAML')
