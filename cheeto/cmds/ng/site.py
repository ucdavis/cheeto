import asyncio
from argparse import Namespace
from pathlib import Path

from ponderosa import ArgParser
from rich.panel import Panel
from rich.table import Table

from .. import commands
from ...log import Console
from ...models.site import Site
from ...operations import (
    AddStickyGroup,
    AddStickySlurmAccount,
    CreateSite,
    RemoveStickyGroup,
    RemoveStickySlurmAccount,
)
from ...puppet import PuppetAccountMap
from ...queries import (
    find_site_by_name,
    resolve_group_names,
    resolve_slurm_account_label,
    resolve_slurm_account_labels,
    site_to_puppet_legacy,
)
from ...yaml import dumps as dumps_yaml, highlight_yaml
from ._args import group_args, site_args


@commands.register('ng', 'site',
                   help='Site operations')
def site_cmd(args: Namespace):
    pass


@site_args.apply(required=True)
@commands.register('ng', 'site', 'new',
                   help='Create a new site')
async def site_new(args: Namespace):
    console = Console()
    site = await CreateSite.run(
        args.db, args.author,
        name=args.site, fqdn=args.fqdn,
    )
    console.print(f'Created site [green]{site.name}[/] ({site.fqdn})')


@site_new.args()
def _(parser: ArgParser):
    parser.add_argument('--fqdn', required=True)


# ---------------------------------------------------------------------------
# `ng site show`
# ---------------------------------------------------------------------------


async def _site_to_dict(site: Site) -> dict:
    sticky_groups, sticky_accounts, default_account = await asyncio.gather(
        resolve_group_names(site.group.sticky),
        resolve_slurm_account_labels(site.slurm.sticky),
        resolve_slurm_account_label(site.slurm.default_account),
    )
    return {
        'name': site.name,
        'fqdn': site.fqdn,
        'sticky_groups': sticky_groups,
        'sticky_slurm_accounts': sticky_accounts,
        'default_slurm_account': default_account,
        'created_at': site.created_at,
        'updated_at': site.updated_at,
    }


def _render_site_panel(data: dict) -> Panel:
    table = Table(show_header=False, box=None, pad_edge=False, padding=(0, 1))
    table.add_column(style='bold cyan', no_wrap=True)
    table.add_column()

    for key in ('name', 'fqdn', 'created_at', 'updated_at'):
        if data.get(key) is not None:
            table.add_row(key, str(data[key]))

    groups = data['sticky_groups']
    table.add_row(
        'sticky groups',
        '\n'.join(groups) if groups else '[dim](none)[/]',
    )
    accounts = data['sticky_slurm_accounts']
    table.add_row(
        'sticky slurm',
        '\n'.join(accounts) if accounts else '[dim](none)[/]',
    )
    default = data['default_slurm_account']
    table.add_row(
        'default slurm',
        default if default else '[dim](none)[/]',
    )

    return Panel(
        table, title=f'[bold]Site:[/] [green]{data["name"]}[/]',
        border_style='green', expand=False,
    )


@site_args.apply(required=True)
@commands.register('ng', 'site', 'show',
                   help='Show site details, including sticky groups + '
                        'slurm accounts and the default slurm account')
async def site_show(args: Namespace):
    console = Console()
    site = await find_site_by_name(args.site)
    if site is None:
        console.print(f'[red]Site {args.site} not found[/]')
        return 1
    data = await _site_to_dict(site)
    if args.yaml:
        console.print(highlight_yaml(dumps_yaml(data)))
    else:
        console.print(_render_site_panel(data))


@site_show.args()
def _(parser: ArgParser):
    parser.add_argument('--yaml', action='store_true', default=False,
                        help='Output as YAML')


# ---------------------------------------------------------------------------
# `ng site sticky` — manage Site.group.sticky and Site.slurm.sticky
# ---------------------------------------------------------------------------


@commands.register('ng', 'site', 'sticky',
                   help='Manage per-site sticky groups and slurm accounts')
def site_sticky_cmd(args: Namespace):
    pass


@commands.register('ng', 'site', 'sticky', 'add',
                   help='Add a sticky group or slurm account to a site')
def site_sticky_add_cmd(args: Namespace):
    pass


@commands.register('ng', 'site', 'sticky', 'remove',
                   help='Remove a sticky group or slurm account from a site')
def site_sticky_remove_cmd(args: Namespace):
    pass


@site_args.apply(required=True)
@group_args.apply(required=True)
@commands.register('ng', 'site', 'sticky', 'add', 'group',
                   help="Add a Group to site.group.sticky; every user at "
                        "the site is then implicitly a member")
async def site_sticky_add_group(args: Namespace):
    console = Console()
    await AddStickyGroup.run(
        args.db, args.author,
        sitename=args.site, groupname=args.group,
    )
    console.print(
        f'Added [green]{args.group}[/] to [bold]{args.site}[/].group.sticky'
    )


@site_args.apply(required=True)
@group_args.apply(required=True)
@commands.register('ng', 'site', 'sticky', 'remove', 'group',
                   help='Remove a Group from site.group.sticky')
async def site_sticky_remove_group(args: Namespace):
    console = Console()
    await RemoveStickyGroup.run(
        args.db, args.author,
        sitename=args.site, groupname=args.group,
    )
    console.print(
        f'Removed [yellow]{args.group}[/] from [bold]{args.site}[/].group.sticky'
    )


@site_args.apply(required=True)
@group_args.apply(required=True)
@commands.register('ng', 'site', 'sticky', 'add', 'slurm',
                   help="Add a group's SlurmAccount to site.slurm.sticky; "
                        "every user at the site is then implicitly a slurmer")
async def site_sticky_add_slurm(args: Namespace):
    console = Console()
    await AddStickySlurmAccount.run(
        args.db, args.author,
        sitename=args.site, groupname=args.group, default=args.default,
    )
    suffix = ' (and set as default_account)' if args.default else ''
    console.print(
        f'Added [green]{args.group}[/] to [bold]{args.site}[/].slurm.sticky{suffix}'
    )


@site_sticky_add_slurm.args()
def _(parser: ArgParser):
    parser.add_argument(
        '--default', action='store_true', default=False,
        help='Also set this account as site.slurm.default_account',
    )


@site_args.apply(required=True)
@group_args.apply(required=True)
@commands.register('ng', 'site', 'sticky', 'remove', 'slurm',
                   help="Remove a group's SlurmAccount from site.slurm.sticky")
async def site_sticky_remove_slurm(args: Namespace):
    console = Console()
    try:
        await RemoveStickySlurmAccount.run(
            args.db, args.author,
            sitename=args.site, groupname=args.group,
            clear_default=args.clear_default,
        )
    except ValueError as e:
        console.print(f'[red]{e}[/]')
        return 1
    suffix = ' (and cleared default_account)' if args.clear_default else ''
    console.print(
        f'Removed [yellow]{args.group}[/] from '
        f'[bold]{args.site}[/].slurm.sticky{suffix}'
    )


@site_sticky_remove_slurm.args()
def _(parser: ArgParser):
    parser.add_argument(
        '--clear-default', action='store_true', default=False,
        help='Clear site.slurm.default_account if it points at this account',
    )


# ---------------------------------------------------------------------------
# `ng site export` — read-only exports of site data
# ---------------------------------------------------------------------------


@commands.register('ng', 'site', 'export',
                   help='Export site data in various formats')
def site_export_cmd(args: Namespace):
    pass


@site_args.apply(required=True)
@commands.register('ng', 'site', 'export', 'puppet-legacy',
                   help="Export site users/groups as v1-compatible "
                        "puppet.hpc YAML (omits storage/share blocks)")
async def site_export_puppet_legacy(args: Namespace):
    console = Console()
    site = await find_site_by_name(args.site)
    if site is None:
        console.print(f'[red]Site {args.site} not found[/]')
        return 1
    puppet_map = await site_to_puppet_legacy(site)
    yaml_text = PuppetAccountMap.Schema().dumps(puppet_map)
    if args.output:
        Path(args.output).write_text(yaml_text)
        console.print(f'Wrote puppet YAML to [green]{args.output}[/]')
    else:
        console.print(highlight_yaml(yaml_text))


@site_export_puppet_legacy.args()
def _(parser: ArgParser):
    parser.add_argument('--output', '-o', default=None,
                        help='Write YAML to this path (default: stdout)')
