from argparse import Namespace

from ponderosa import ArgParser
from rich.panel import Panel
from rich.table import Table

from .. import commands
from ...constants import ACCESS_TYPES, USER_STATUSES, USER_TYPES
from ...encrypt import generate_password
from ...log import Console
from ...models.site import Site
from ...models.user import SshKey, User
from ...models.user_site_info import UserSiteInfo
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
from ...yaml import dumps as dumps_yaml, highlight_yaml
from ._args import (
    email_args,
    fullname_args,
    password_args,
    site_args,
    user_args,
)
from ._slurm_show import user_slurm_at_site


def _announce_password(console: Console, password: str) -> None:
    console.print(f'Password: [yellow]{password}[/]')
    console.print('[red]Save this password now \u2014 it will not be displayed again.[/]')


def _user_to_dict(user: User,
                  ssh_keys: list[SshKey] | None = None,
                  site_info: dict | None = None,
                  slurm_info: list[dict] | None = None) -> dict:
    data = {
        'name': user.name,
        'email': user.email,
        'uid': user.uid,
        'gid': user.gid,
        'fullname': user.fullname,
        'shell': user.shell,
        'type': user.type,
        'status': user.status,
        'home_directory': user.home_directory,
        'access': list(user.access),
        'created_at': user.created_at,
        'updated_at': user.updated_at,
    }
    if user.expires_at is not None:
        data['expires_at'] = user.expires_at
    if user.provisioned_at is not None:
        data['provisioned_at'] = user.provisioned_at
    if ssh_keys:
        data['ssh_keys'] = [k.key for k in ssh_keys]
    if user.comments:
        data['comments'] = list(user.comments)
    if user.iam is not None:
        data['iam'] = {
            'iam_id': user.iam.iam_id,
            'mothra_id': user.iam.mothra_id,
            'colleges': list(user.iam.colleges),
        }
    if site_info is not None:
        data['site_info'] = site_info
    if slurm_info is not None:
        data['slurm_info'] = slurm_info
    return data


def _render_user_slurm(slurm_info: list[dict]) -> Table:
    table = Table(show_header=True, box=None, pad_edge=False, padding=(0, 1))
    table.add_column('group', style='green')
    table.add_column('role', style='yellow')
    table.add_column('partition', style='cyan')
    table.add_column('qos', style='magenta')
    table.add_column('priority', style='dim')
    table.add_column('flags', style='dim')
    table.add_column('total tres', style='bold')
    for entry in slurm_info:
        assocs = entry['slurm'].get('associations') or []
        if not assocs:
            table.add_row(entry['group'], entry['role'],
                          '[dim](none)[/]', '', '', '', '')
            continue
        for a in assocs:
            table.add_row(
                entry['group'], entry['role'],
                a['partition'], a['qos'],
                str(a['qos_priority']),
                ', '.join(a['qos_flags']),
                a['qos_total_tres'],
            )
    return table


def _render_user_panel(data: dict) -> Panel:
    table = Table(show_header=False, box=None, pad_edge=False, padding=(0, 1))
    table.add_column(style='bold cyan', no_wrap=True)
    table.add_column()

    scalar_keys = ('name', 'email', 'uid', 'gid', 'fullname', 'shell',
                   'type', 'status', 'home_directory',
                   'created_at', 'updated_at',
                   'provisioned_at', 'expires_at')
    for key in scalar_keys:
        if key in data and data[key] is not None:
            table.add_row(key, str(data[key]))

    if data.get('access'):
        table.add_row('access', ', '.join(data['access']))

    if data.get('ssh_keys'):
        table.add_row('ssh_keys', '\n'.join(data['ssh_keys']))

    if data.get('comments'):
        table.add_row('comments', '\n'.join(data['comments']))

    if data.get('iam'):
        iam = data['iam']
        iam_str = (f'iam_id={iam["iam_id"]}, '
                   f'mothra_id={iam["mothra_id"]}, '
                   f'colleges={iam["colleges"]}')
        table.add_row('iam', iam_str)

    if 'site_info' in data:
        si = data['site_info']
        si_str = (
            f'site={si["site"]}, status={si["status"]}, '
            f'access={", ".join(si["access"])}'
            + (f', expires_at={si["expires_at"]}' if si.get('expires_at') else '')
            + (f', provisioned_at={si["provisioned_at"]}' if si.get('provisioned_at') else '')
        )
        table.add_row('at site', si_str)

    if 'slurm_info' in data:
        slurm = data['slurm_info']
        if not slurm:
            table.add_row('slurm', '[dim](no slurm access via any group)[/]')
        else:
            table.add_row('slurm', _render_user_slurm(slurm))

    return Panel(table, title=f'[bold]User:[/] [green]{data["name"]}[/]',
                 border_style='green', expand=False)


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


@site_args.apply()
@user_args.apply(required=True)
@commands.register('ng', 'user', 'show',
                   help='Show user information')
async def user_show(args: Namespace):
    console = Console()
    user = await User.find_one(User.name == args.user)
    if user is None:
        console.print(f'[red]User {args.user} not found[/]')
        return 1

    ssh_keys = await SshKey.find(SshKey.user.id == user.id).to_list()

    site_info = None
    slurm_info = None
    if args.site:
        site = await Site.find_one(Site.name == args.site)
        if site is None:
            console.print(f'[red]Site {args.site} not found[/]')
            return 1
        usi = await UserSiteInfo.find_one(
            UserSiteInfo.user.id == user.id,
            UserSiteInfo.site.id == site.id,
        )
        if usi is not None:
            site_info = {
                'site': args.site,
                'status': usi.status,
                'access': list(usi.access),
                'expires_at': usi.expires_at,
                'provisioned_at': usi.provisioned_at,
            }
        slurm_info = await user_slurm_at_site(user, site)

    data = _user_to_dict(
        user, ssh_keys=ssh_keys, site_info=site_info, slurm_info=slurm_info,
    )
    if args.yaml:
        console.print(highlight_yaml(dumps_yaml(data)))
    else:
        console.print(_render_user_panel(data))


@user_show.args()
def _(parser: ArgParser):
    parser.add_argument('--yaml', action='store_true', default=False,
                        help='Output as YAML')
