from argparse import Namespace

from ponderosa import ArgParser

from .. import commands
from ...log import Console
from ...operations import CreateSite
from ._args import site_args


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
