from argparse import Namespace, RawDescriptionHelpFormatter
import sys

from ponderosa import CmdTree

from .. import __version__
from ..args import banner
from ..log import Console


commands = CmdTree(description=banner,
                   formatter_class=RawDescriptionHelpFormatter)
commands._root.add_argument('--version', action='version', version=f'cheeto {__version__}')


@commands.register('help',
                   help='Show the full subcomamnd tree')
def help(args: Namespace):
    console = Console()
    print(banner, file=sys.stderr)
    console.print(commands.format_help())
    return 0