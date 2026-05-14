from argparse import Namespace

from .. import commands
from ...log import Console
from ...operations import AddSiteUser, RemoveSiteUser
from ._args import site_args, user_args


@site_args.apply(required=True)
@user_args.apply(required=True)
@commands.register('ng', 'user', 'add', 'site',
                   help='Add a user to a site')
async def user_add_site(args: Namespace):
    console = Console()
    await AddSiteUser.run(
        args.db, args.author,
        user_name=args.user, site_name=args.site,
    )
    console.print(f'Added [green]{args.user}[/] to site [green]{args.site}[/]')


@site_args.apply(required=True)
@user_args.apply(required=True)
@commands.register('ng', 'user', 'remove', 'site',
                   help='Remove a user from a site')
async def user_remove_site(args: Namespace):
    console = Console()
    await RemoveSiteUser.run(
        args.db, args.author,
        user_name=args.user, site_name=args.site,
    )
    console.print(f'Removed [green]{args.user}[/] from site [green]{args.site}[/]')
