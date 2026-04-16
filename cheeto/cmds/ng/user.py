from argparse import Namespace

from ponderosa import ArgParser

from .. import commands
from ...constants import ACCESS_TYPES, USER_STATUSES, USER_TYPES
from ...encrypt import generate_password
from ...log import Console
from ...operations import (
    AddUserAccess,
    AddUserComment,
    CreateClassUser,
    CreateSharedUser,
    CreateSystemUser,
    CreateUser,
    RemoveUserAccess,
    SetUserPassword,
    SetUserShell,
    SetUserStatus,
    SetUserType,
)
from ._args import (
    email_args,
    fullname_args,
    password_args,
    site_args,
    user_args,
)


def _announce_password(console: Console, password: str) -> None:
    console.print(f'Password: [yellow]{password}[/]')
    console.print('[red]Save this password now \u2014 it will not be displayed again.[/]')


@commands.register('ng', 'user',
                   help='User operations')
def user_cmd(args: Namespace):
    pass


@commands.register('ng', 'user', 'new',
                   help='Create a new user')
def user_new_cmd(args: Namespace):
    pass


@user_args.apply(required=True)
@email_args.apply(required=True)
@fullname_args.apply(required=True)
@password_args.apply()
@commands.register('ng', 'user', 'new', 'generic',
                   help='Create a new user with an explicit UID and type')
async def user_new_generic(args: Namespace):
    console = Console()
    password = generate_password() if args.password else None
    user, group = await CreateUser.run(
        args.db, args.author,
        name=args.user, email=args.email, uid=args.uid,
        fullname=args.fullname, type=args.type,
        gid=args.gid, access=args.access,
        password=password,
    )
    console.print(f'Created user [green]{user.name}[/] (uid={user.uid})')
    if password is not None:
        _announce_password(console, password)


@user_new_generic.args()
def _(parser: ArgParser):
    parser.add_argument('--uid', type=int, required=True)
    parser.add_argument('--gid', type=int, default=None)
    parser.add_argument('--type', default='user', choices=list(USER_TYPES))
    parser.add_argument('--access', nargs='+', default=None,
                        choices=list(ACCESS_TYPES))


@user_args.apply(required=True)
@email_args.apply(default='hpc-help@ucdavis.edu')
@fullname_args.apply(required=False)
@password_args.apply()
@commands.register('ng', 'user', 'new', 'system',
                   help='Create a new system user')
async def user_new_system(args: Namespace):
    console = Console()
    fullname = args.fullname or f'HPCCF {args.user}'
    password = generate_password() if args.password else None
    user, group = await CreateSystemUser.run(
        args.db, args.author,
        name=args.user, email=args.email, fullname=fullname,
        password=password,
    )
    console.print(f'Created system user [green]{user.name}[/] (uid={user.uid})')
    if password is not None:
        _announce_password(console, password)


@user_args.apply(required=True)
@email_args.apply(required=True)
@fullname_args.apply(required=True)
@password_args.apply()
@commands.register('ng', 'user', 'new', 'class',
                   help='Create a new class user')
async def user_new_class(args: Namespace):
    console = Console()
    password = generate_password() if args.password else None
    user, group = await CreateClassUser.run(
        args.db, args.author,
        name=args.user, email=args.email, fullname=args.fullname,
        password=password,
    )
    console.print(f'Created class user [green]{user.name}[/] (uid={user.uid})')
    if password is not None:
        _announce_password(console, password)


@user_args.apply(required=True)
@email_args.apply(required=True)
@fullname_args.apply(required=True)
@password_args.apply()
@commands.register('ng', 'user', 'new', 'shared',
                   help='Create a new shared user')
async def user_new_shared(args: Namespace):
    console = Console()
    password = generate_password() if args.password else None
    user, group = await CreateSharedUser.run(
        args.db, args.author,
        name=args.user, email=args.email, fullname=args.fullname,
        password=password,
    )
    console.print(f'Created shared user [green]{user.name}[/] (uid={user.uid})')
    if password is not None:
        _announce_password(console, password)


@site_args.apply()
@user_args.apply(required=True)
@commands.register('ng', 'user', 'status',
                   help='Set user status')
async def user_status(args: Namespace):
    console = Console()
    await SetUserStatus.run(
        args.db, args.author,
        name=args.user, status=args.status,
        reason=args.reason, site=args.site,
    )
    scope = args.site or 'global'
    console.print(f'Set [green]{args.user}[/] status to [yellow]{args.status}[/] ({scope})')


@user_status.args()
def _(parser: ArgParser):
    parser.add_argument('--status', required=True, choices=list(USER_STATUSES))
    parser.add_argument('--reason', required=True)


@user_args.apply(required=True)
@commands.register('ng', 'user', 'type',
                   help='Set user type')
async def user_type(args: Namespace):
    console = Console()
    await SetUserType.run(
        args.db, args.author,
        name=args.user, type=args.type,
    )
    console.print(f'Set [green]{args.user}[/] type to [yellow]{args.type}[/]')


@user_type.args()
def _(parser: ArgParser):
    parser.add_argument('--type', required=True, choices=list(USER_TYPES))


@user_args.apply(required=True)
@commands.register('ng', 'user', 'shell',
                   help='Set user shell')
async def user_shell(args: Namespace):
    console = Console()
    await SetUserShell.run(
        args.db, args.author,
        name=args.user, shell=args.shell,
    )
    console.print(f'Set [green]{args.user}[/] shell to [yellow]{args.shell}[/]')


@user_shell.args()
def _(parser: ArgParser):
    parser.add_argument('--shell', required=True)


@user_args.apply(required=True)
@commands.register('ng', 'user', 'password',
                   help='Generate and set a new password for a user')
async def user_password(args: Namespace):
    console = Console()
    password = generate_password()
    await SetUserPassword.run(
        args.db, args.author,
        name=args.user, password=password,
    )
    console.print(f'Set password for [green]{args.user}[/]')
    _announce_password(console, password)


@site_args.apply()
@user_args.apply(required=True)
@commands.register('ng', 'user', 'add', 'access',
                   help='Add access type(s) to a user')
async def user_add_access(args: Namespace):
    console = Console()
    await AddUserAccess.run(
        args.db, args.author,
        name=args.user, access=args.access, site=args.site,
    )
    console.print(f'Added access {args.access} to [green]{args.user}[/]')


@user_add_access.args()
def _(parser: ArgParser):
    parser.add_argument('--access', '-a', nargs='+', required=True,
                        choices=list(ACCESS_TYPES))


@site_args.apply()
@user_args.apply(required=True)
@commands.register('ng', 'user', 'remove', 'access',
                   help='Remove access type(s) from a user')
async def user_remove_access(args: Namespace):
    console = Console()
    await RemoveUserAccess.run(
        args.db, args.author,
        name=args.user, access=args.access, site=args.site,
    )
    console.print(f'Removed access {args.access} from [green]{args.user}[/]')


@user_remove_access.args()
def _(parser: ArgParser):
    parser.add_argument('--access', '-a', nargs='+', required=True,
                        choices=list(ACCESS_TYPES))


@user_args.apply(required=True)
@commands.register('ng', 'user', 'comment',
                   help='Add a comment to a user')
async def user_comment(args: Namespace):
    console = Console()
    await AddUserComment.run(
        args.db, args.author,
        name=args.user, comment=args.comment,
    )
    console.print(f'Added comment to [green]{args.user}[/]')


@user_comment.args()
def _(parser: ArgParser):
    parser.add_argument('--comment', required=True)
