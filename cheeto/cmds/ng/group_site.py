from argparse import Namespace

from ponderosa import ArgParser
from rich.table import Table

from .. import commands
from ...log import Console
from ...operations import AddSiteGroup, BackfillGroupSiteInfo, RemoveSiteGroup
from ...yaml import print_yaml
from ._args import group_args, run_per_target, site_args, yaml_args


@site_args.apply(required=True)
@group_args.apply(required=True, multiple=True)
@commands.register('ng', 'group', 'add', 'site',
                   help='Add one or more groups to a site')
async def group_add_site(args: Namespace):
    console = Console()

    async def _one(name: str) -> None:
        await AddSiteGroup.run(
            args.db, args.author,
            group_name=name, site_name=args.site,
        )

    return await run_per_target(
        console, args.group, _one, ok=f'added to {args.site}',
    )


@site_args.apply(required=True)
@group_args.apply(required=True, multiple=True)
@commands.register('ng', 'group', 'remove', 'site',
                   help='Remove one or more groups from a site')
async def group_remove_site(args: Namespace):
    console = Console()

    async def _one(name: str) -> None:
        await RemoveSiteGroup.run(
            args.db, args.author,
            group_name=name, site_name=args.site, force=args.force,
        )

    code = await run_per_target(
        console, args.group, _one, ok=f'removed from {args.site}',
    )
    if code == 0:
        console.print(
            '[dim]LDAP entries are removed by the next prune '
            '(`ng ldap prune`).[/]'
        )
    return code


@group_remove_site.args()
def _(parser: ArgParser):
    parser.add_argument('--force', '-f', action='store_true', default=False,
                        help='Also delete membership edges at the site '
                             '(slurm/storage/sticky references still block)')


@yaml_args.apply()
@site_args.apply()
@commands.register('ng', 'group', 'backfill-sites',
                   help='Create explicit group-site presence records from '
                        'imputed presence (membership edges, sticky groups, '
                        'slurm accounts, storage, personal groups)')
async def group_backfill_sites(args: Namespace):
    console = Console()
    # A dry run writes nothing — skip the History row too.
    report = await BackfillGroupSiteInfo.run(
        args.db, args.author,
        site_name=args.site, dry_run=args.dry_run,
        skip_history=args.dry_run,
    )

    if args.yaml:
        print_yaml(report)
        return

    created_col = 'would create' if args.dry_run else 'created'
    table = Table(
        title='GroupSiteInfo backfill'
              + (' (dry run)' if args.dry_run else ''),
    )
    table.add_column('site', style='green', no_wrap=True)
    table.add_column(created_col, justify='right')
    table.add_column('existing', justify='right')
    table.add_column('extras', justify='right')
    for site_name, row in sorted(report.items()):
        table.add_row(
            site_name,
            str(len(row['created'])),
            str(row['existing']),
            str(len(row['extras'])),
        )
    console.print(table)

    def _names(names: list[str]) -> str:
        # Real sites run to thousands; keep the terminal readable and
        # leave the full list to --yaml.
        if len(names) > 25:
            shown = ', '.join(names[:25])
            return f'{shown}, … and {len(names) - 25} more (--yaml for all)'
        return ', '.join(names)

    for site_name, row in sorted(report.items()):
        if row['created']:
            console.print(
                f'[bold]{site_name}[/] {created_col}: '
                + _names(row['created'])
            )
        if row['extras']:
            console.print(
                f'[bold]{site_name}[/] [yellow]explicit-only (no imputed '
                f'source)[/]: ' + _names(row['extras'])
            )


@group_backfill_sites.args()
def _(parser: ArgParser):
    parser.add_argument('--dry-run', action='store_true', default=False,
                        help='Report what would be created without writing')
