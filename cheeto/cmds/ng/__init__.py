import os
from argparse import Namespace

from ponderosa import ArgParser

from .. import commands
from ...db import connect_beanie
from ...models import User


def parent(*path: str, help: str, aliases: list[str] | None = None):
    """Register a namespace node that prints its own help when invoked
    bare. Explicit `pass` bodies silently no-op (ponderosa only installs
    the print-help default for auto-created nodes), so every grammar
    parent goes through this instead. Must be called before any leaf
    under it is registered."""
    def _handler(args: Namespace):
        # By call time the module-level name is the SubCmd ponderosa
        # returned from register(), which carries the parser.
        node.parser.print_help()

    node = commands.register(*path, help=help, aliases=aliases)(_handler)
    return node


@commands.register('ng',
                   help='Next-generation database operations (beanie/async)')
def ng_cmd(args: Namespace):
    pass


@ng_cmd.args(common=True)
def ng_args(parser: ArgParser):
    parser.add_argument('--author', default=None,
                        help='Username of the author performing the operation '
                             '(default: $USER)')
    parser.add_argument('--no-resolve-author', action='store_true', default=False,
                        help='Do not resolve the author username to a User document. DEBUG ONLY.')


@ng_args.postprocessor(priority=50)
async def connect_db(args: Namespace):
    args.db = await connect_beanie(args.config.mongo, quiet=args.quiet)


@ng_args.postprocessor()
async def resolve_author(args: Namespace):
    if args.no_resolve_author:
        return
    username = args.author or os.environ.get('USER')
    args.author = await User.find_one(User.name == username)


# Import submodules after registration to avoid circular re-registration
from . import site, user, user_site, group, slurm, storage, history, migrate, hippo, iam, ldap  # noqa: E402, F401
