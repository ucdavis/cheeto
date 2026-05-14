from argparse import Namespace

from ponderosa import ArgParser

from .. import commands
from ...log import Console
from ...operations import CreateHomeStorage
from ._args import site_args, user_args


@commands.register('ng', 'storage',
                   help='Storage operations')
def storage_cmd(args: Namespace):
    pass


@site_args.apply(required=True)
@user_args.apply(required=True)
@commands.register('ng', 'storage', 'new', 'home',
                   help='Create home storage for a user')
async def storage_new_home(args: Namespace):
    console = Console()
    storage = await CreateHomeStorage.run(
        args.db, args.author,
        user_name=args.user, site_name=args.site,
        host=args.host, host_path=args.path,
        quota=args.quota,
    )
    console.print(
        f'Created home storage for [green]{args.user}[/] '
        f'on site {args.site} (host={args.host})'
    )


@storage_new_home.args()
def _(parser: ArgParser):
    parser.add_argument('--host', required=True)
    parser.add_argument('--path', default=None,
                        help='Host path (default: /home/<username>)')
    parser.add_argument('--quota', default=None,
                        help='Initial storage quota (e.g., 100G)')
