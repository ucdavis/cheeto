from argparse import Namespace

from ponderosa import ArgParser
from rich.table import Table

from .. import commands
from ...log import Console
from ...operations import AddSiteUser, BackfillUserSiteInfo, RemoveSiteUser
from ...yaml import print_yaml
from ._args import run_per_target, site_args, user_args, yaml_args


@site_args.apply(required=True)
@user_args.apply(required=True, multiple=True)
@commands.register('ng', 'user', 'add', 'site',
                   help='Add one or more users to a site')
async def user_add_site(args: Namespace):
    console = Console()

    async def _one(name: str) -> None:
        await AddSiteUser.run(
            args.db, args.author,
            user_name=name, site_name=args.site,
        )

    return await run_per_target(
        console, args.user, _one, ok=f'added to {args.site}',
    )


@site_args.apply(required=True)
@user_args.apply(required=True, multiple=True)
@commands.register('ng', 'user', 'remove', 'site',
                   help='Remove one or more users from a site, with their '
                        'group memberships and Slurm coordinator seats there')
async def user_remove_site(args: Namespace):
    console = Console()

    async def _one(name: str) -> None:
        result = await RemoveSiteUser.run(
            args.db, args.author,
            user_name=name, site_name=args.site,
        )
        if result['memberships_deleted']:
            console.print(
                f'  [dim]{name}: removed {result["memberships_deleted"]} '
                f'group membership(s) at {args.site}[/]'
            )
        if result['coordinator_accounts']:
            console.print(
                f'  [dim]{name}: removed as Slurm coordinator of '
                f'{", ".join(result["coordinator_accounts"])}[/]'
            )

    return await run_per_target(
        console, args.user, _one, ok=f'removed from {args.site}',
    )


@yaml_args.apply()
@site_args.apply()
@commands.register('ng', 'user', 'backfill-sites',
                   help='Create user-site presence records for users holding '
                        'a member/sudoer/slurmer role at a site without one')
async def user_backfill_sites(args: Namespace):
    console = Console()
    # A dry run writes nothing — skip the History row too.
    report = await BackfillUserSiteInfo.run(
        args.db, args.author,
        site_name=args.site, dry_run=args.dry_run,
        skip_history=args.dry_run,
    )

    if args.yaml:
        print_yaml(report)
        return

    created_col = 'would create' if args.dry_run else 'created'
    table = Table(
        title='UserSiteInfo backfill'
              + (' (dry run)' if args.dry_run else ''),
    )
    table.add_column('site', style='green', no_wrap=True)
    table.add_column(created_col, justify='right')
    table.add_column('existing', justify='right')
    for site_name, row in sorted(report.items()):
        table.add_row(
            site_name,
            str(len(row['created'])),
            str(row['existing']),
        )
    console.print(table)

    for site_name, row in sorted(report.items()):
        names = row['created']
        if not names:
            continue
        # Keep the terminal readable; leave the full list to --yaml.
        shown = ', '.join(names[:25])
        if len(names) > 25:
            shown += f', … and {len(names) - 25} more (--yaml for all)'
        console.print(f'[bold]{site_name}[/] {created_col}: {shown}')


@user_backfill_sites.args()
def _(parser: ArgParser):
    parser.add_argument('--dry-run', action='store_true', default=False,
                        help='Report what would be created without writing')
