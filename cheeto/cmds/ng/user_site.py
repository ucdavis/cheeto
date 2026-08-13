from argparse import Namespace

from .. import commands
from ...log import Console
from ...operations import AddSiteUser, RemoveSiteUser
from ._args import run_per_target, site_args, user_args


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
                   help='Remove one or more users from a site')
async def user_remove_site(args: Namespace):
    console = Console()

    async def _one(name: str) -> None:
        await RemoveSiteUser.run(
            args.db, args.author,
            user_name=name, site_name=args.site,
        )

    return await run_per_target(
        console, args.user, _one, ok=f'removed from {args.site}',
    )
