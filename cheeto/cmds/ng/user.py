from argparse import Namespace
from datetime import datetime, timedelta, timezone

from ponderosa import ArgParser
from rich.panel import Panel
from rich.table import Table

from .. import commands
from ...constants import ACCESS_TYPES, USER_STATUSES, USER_TYPES
from ...encrypt import generate_password
from ...log import Console
from ...models.group import AccessGroup, StatusGroup
from ...models.site import Site
from ...models.user import SshKey, User
from ...models.user_site_info import UserSiteInfo
from ...operations import (
    AddUserAccess,
    AddUserComment,
    AddUserSshKey,
    CreateClassUser,
    CreateSharedUser,
    CreateSystemUser,
    CreateUser,
    RemoveUserAccess,
    RemoveUserSshKey,
    SetUserPassword,
    SetUserShell,
    SetUserStatus,
    SetUserType,
)
from ...queries import (
    effective_access_links,
    find_site_by_name,
    find_user,
    find_users,
    list_user_ssh_keys,
    resolve_access_names,
    resolve_status_name,
    user_groups_at_site,
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


def _project_iam_dates(
    first_missing_at: datetime,
    grace_days: int,
    expiry_offset_days: int,
) -> dict:
    """Compute the fixed grace-end and projected-expiry dates from
    first_missing_at + the configured offsets, plus a 'grace_remaining'
    timedelta relative to now (None if grace has already passed)."""
    fma = first_missing_at
    if fma.tzinfo is None:
        fma = fma.replace(tzinfo=timezone.utc)
    grace_ends = fma + timedelta(days=grace_days)
    projected_expiry = grace_ends + timedelta(days=expiry_offset_days)
    now = datetime.now(timezone.utc)
    remaining = grace_ends - now
    return {
        'grace_ends_on': grace_ends,
        'projected_expires_at': projected_expiry,
        'grace_remaining': remaining if remaining.total_seconds() > 0 else None,
    }


async def _user_to_dict(user: User,
                        ssh_keys: list[SshKey] | None = None,
                        site_info: dict | None = None,
                        group_memberships: list[dict] | None = None,
                        slurm_info: list[dict] | None = None,
                        iam_config=None) -> dict:
    data = {
        'name': user.name,
        'email': user.email,
        'uid': user.uid,
        'gid': user.gid,
        'fullname': user.fullname,
        'shell': user.shell,
        'type': user.type,
        'status': await resolve_status_name(user.status),
        'home_directory': user.home_directory,
        'access': await resolve_access_names(user.access),
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
        iam = user.iam
        iam_data: dict = {
            'iam_status': iam.iam_status,
            'iam_synced_at': iam.iam_synced_at,
            'last_seen_at': iam.last_seen_at,
            'first_missing_at': iam.first_missing_at,
        }
        if iam.person is not None:
            iam_data['iam_id'] = iam.person.iam_id
            iam_data['mothra_id'] = iam.person.mothra_id
            iam_data['user_types'] = list(iam.person.user_types)
            iam_data['associations'] = [
                {
                    'org_name': a.org_name,
                    'dept_name': a.dept_name,
                    'title': a.title,
                    'title_type': a.title_type,
                }
                for a in iam.person.associations
            ]
        if iam.first_missing_at is not None and iam_config is not None:
            iam_data.update(_project_iam_dates(
                iam.first_missing_at,
                iam_config.grace_days,
                iam_config.expiry_offset_days,
            ))
        data['iam'] = iam_data
    if site_info is not None:
        data['site_info'] = site_info
    if group_memberships is not None:
        data['group_memberships'] = group_memberships
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


def _render_user_groups(memberships: list[dict]) -> Table:
    table = Table(show_header=True, box=None, pad_edge=False, padding=(0, 1))
    table.add_column('group', style='green')
    table.add_column('roles', style='yellow')
    for entry in memberships:
        table.add_row(entry['group'], ', '.join(entry['roles']))
    return table


def _render_user_iam(iam: dict) -> Table:
    """Render the iam sub-block: status + sync state + person snapshot."""
    table = Table(show_header=False, box=None, pad_edge=False, padding=(0, 1))
    table.add_column(style='dim', no_wrap=True)
    table.add_column()

    status = iam.get('iam_status', 'present')
    status_style = 'red' if status == 'missing' else 'green'
    table.add_row('iam_status', f'[{status_style}]{status}[/]')

    if 'iam_id' in iam:
        table.add_row('iam_id', str(iam['iam_id']))
    if 'mothra_id' in iam:
        table.add_row('mothra_id', str(iam['mothra_id']))
    if iam.get('user_types'):
        table.add_row('user_types', ', '.join(iam['user_types']))

    for key in ('iam_synced_at', 'last_seen_at'):
        if iam.get(key):
            table.add_row(key, str(iam[key]))
    # first_missing_at is the actionable signal; surface it in red.
    if iam.get('first_missing_at'):
        table.add_row(
            'first_missing_at',
            f'[red]{iam["first_missing_at"]}[/]',
        )

    # Grace/projection: only set when iam_config was passed AND the user
    # is currently in a missing streak.
    if iam.get('grace_ends_on'):
        remaining = iam.get('grace_remaining')
        if remaining is not None:
            days = remaining.days
            hours = remaining.seconds // 3600
            table.add_row(
                'grace remaining', f'[yellow]{days}d {hours}h[/]',
            )
        else:
            table.add_row(
                'grace remaining',
                '[red]past — next sync will set expires_at[/]',
            )
        table.add_row('grace ends on', str(iam['grace_ends_on']))
        table.add_row(
            'projected expires_at',
            f'[red]{iam["projected_expires_at"]}[/]',
        )

    associations = iam.get('associations') or []
    if associations:
        sub = Table(show_header=True, box=None, pad_edge=False, padding=(0, 1))
        sub.add_column('org', style='green')
        sub.add_column('dept', style='cyan')
        sub.add_column('title', style='magenta')
        sub.add_column('class', style='dim')
        for a in associations:
            sub.add_row(
                a.get('org_name', ''), a.get('dept_name', ''),
                a.get('title', ''), a.get('title_type', ''),
            )
        table.add_row('associations', sub)

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
        table.add_row('iam', _render_user_iam(data['iam']))

    if 'site_info' in data:
        si = data['site_info']
        override = si['access']
        effective = si.get('effective_access', override)
        if override:
            access_str = (
                f'access={", ".join(override)} '
                f'(override; effective={", ".join(effective) or "—"})'
            )
        else:
            access_str = (
                f'access=[] (no override; effective from global: '
                f'{", ".join(effective) or "—"})'
            )
        si_str = (
            f'site={si["site"]}, status={si["status"]}, {access_str}'
            + (f', expires_at={si["expires_at"]}' if si.get('expires_at') else '')
            + (f', provisioned_at={si["provisioned_at"]}' if si.get('provisioned_at') else '')
        )
        table.add_row('at site', si_str)

    if 'group_memberships' in data:
        memberships = data['group_memberships']
        if not memberships:
            table.add_row('groups', '[dim](no group memberships at site)[/]')
        else:
            table.add_row('groups', _render_user_groups(memberships))

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


def _read_ssh_key(args: Namespace) -> str | None:
    """Resolve the key text from --key or --key-file. Returns None and
    prints the error if neither/both are given or the file is unreadable."""
    console = Console()
    if (args.key is None) == (args.key_file is None):
        console.print('[red]Specify exactly one of --key or --key-file[/]')
        return None
    if args.key is not None:
        return args.key.strip()
    try:
        return open(args.key_file).read().strip()
    except OSError as e:
        console.print(f'[red]Could not read {args.key_file}: {e}[/]')
        return None


@user_args.apply(required=True)
@commands.register('ng', 'user', 'add', 'ssh-key',
                   help='Add an SSH public key for a user')
async def user_add_ssh_key(args: Namespace):
    console = Console()
    key = _read_ssh_key(args)
    if key is None:
        return 1
    await AddUserSshKey.run(
        args.db, args.author,
        name=args.user, key=key, replace=args.replace,
    )
    if args.replace:
        console.print(
            f'Replaced ssh keys for [green]{args.user}[/] with the new key'
        )
    else:
        console.print(f'Added ssh key for [green]{args.user}[/]')


@user_add_ssh_key.args()
def _(parser: ArgParser):
    parser.add_argument('--key', '-k', default=None,
                        help='SSH public key text (quote it)')
    parser.add_argument('--key-file', '-f', default=None,
                        help='Path to a file containing the SSH public key')
    parser.add_argument('--replace', action='store_true', default=False,
                        help='Delete all existing SSH keys for this user '
                             'and replace with the new one')


@user_args.apply(required=True)
@commands.register('ng', 'user', 'remove', 'ssh-key',
                   help='Remove an SSH public key from a user; prompts if '
                        'more than one is configured')
async def user_remove_ssh_key(args: Namespace):
    console = Console()
    user = await find_user(name=args.user)
    if user is None:
        console.print(f'[red]No user found: {args.user}[/]')
        return 1
    keys = await list_user_ssh_keys(user)
    if not keys:
        console.print(f'[yellow]{args.user} has no ssh keys[/]')
        return

    if len(keys) == 1:
        target = keys[0]
        console.print(f'Removing the only ssh key for [green]{args.user}[/]:')
        console.print(f'  [dim]{target.key}[/]')
    else:
        console.print(
            f'[bold]{args.user}[/] has {len(keys)} ssh keys; pick one to '
            f'remove:'
        )
        for i, k in enumerate(keys, start=1):
            console.print(f'  [cyan]{i}[/]) {k.key}')
        try:
            choice = input(f'Number to remove [1-{len(keys)}] (or blank to abort): ').strip()
        except (EOFError, KeyboardInterrupt):
            console.print('\n[red]Aborted.[/]')
            return 1
        if not choice:
            console.print('[red]Aborted.[/]')
            return 1
        try:
            idx = int(choice) - 1
            target = keys[idx]
            assert 0 <= idx < len(keys)
        except (ValueError, AssertionError, IndexError):
            console.print(f'[red]Invalid selection: {choice!r}[/]')
            return 1

    await RemoveUserSshKey.run(
        args.db, args.author,
        name=args.user, key_id=target.id,
    )
    console.print(f'Removed ssh key from [green]{args.user}[/]')


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
@commands.register('ng', 'user', 'show',
                   help='Show one user, identified by name, uid, or email')
async def user_show(args: Namespace):
    console = Console()
    identifiers = {
        k: v for k, v in (
            ('name', args.user), ('uid', args.uid), ('email', args.email),
        ) if v is not None
    }
    try:
        user = await find_user(**identifiers)
    except ValueError as e:
        console.print(f'[red]{e}[/]')
        return 1
    if user is None:
        ident_str = ', '.join(f'{k}={v!r}' for k, v in identifiers.items())
        console.print(f'[red]No user found for {ident_str}[/]')
        return 1

    ssh_keys = await list_user_ssh_keys(user)

    site_info = None
    slurm_info = None
    group_memberships = None
    if args.site:
        site = await find_site_by_name(args.site)
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
                'status': await resolve_status_name(usi.status),
                'access': await resolve_access_names(usi.access),
                'effective_access': await resolve_access_names(
                    effective_access_links(user, usi),
                ),
                'expires_at': usi.expires_at,
                'provisioned_at': usi.provisioned_at,
            }
        group_rows = await user_groups_at_site(user, site)
        group_memberships = sorted(
            ({'group': r.group.name, 'roles': list(r.roles)}
             for r in group_rows),
            key=lambda d: d['group'],
        )
        slurm_info = await user_slurm_at_site(user, site)

    data = await _user_to_dict(
        user, ssh_keys=ssh_keys, site_info=site_info,
        group_memberships=group_memberships, slurm_info=slurm_info,
        iam_config=args.config.ucdiam,
    )
    if args.yaml:
        console.print(highlight_yaml(dumps_yaml(data)))
    else:
        console.print(_render_user_panel(data))


@user_show.args()
def _(parser: ArgParser):
    parser.add_argument('--user', '-u', default=None, help='Username')
    parser.add_argument('--uid', type=int, default=None, help='Numeric UID')
    parser.add_argument('--email', default=None, help='Email address')
    parser.add_argument('--yaml', action='store_true', default=False,
                        help='Output as YAML')


@commands.register('ng', 'user', 'list',
                   help='List users matching one or more filters '
                        '(combined by --operator)')
async def user_list(args: Namespace):
    console = Console()
    operator = (args.operator or 'AND').upper()
    if operator not in ('AND', 'OR'):
        console.print(
            f'[red]--operator must be AND or OR (got {args.operator!r})[/]'
        )
        return 1

    users = await find_users(
        status=args.status,
        access=args.access,
        type=args.type,
        site=args.site,
        group=args.group,
        operator=operator,
    )

    if args.limit is not None and args.limit > 0:
        users = users[:args.limit]

    status_names = {
        sg.id: sg.status_name
        for sg in await StatusGroup.find_all().to_list()
    }
    access_names = {
        ag.id: ag.access_name
        for ag in await AccessGroup.find_all().to_list()
    }

    def _status_of(u: User) -> str | None:
        if u.status is None:
            return None
        if isinstance(u.status, StatusGroup):
            return u.status.status_name
        return status_names.get(u.status.ref.id)

    def _access_of(u: User) -> list[str]:
        out: list[str] = []
        for link in u.access:
            if isinstance(link, AccessGroup):
                out.append(link.access_name)
                continue
            name = access_names.get(link.ref.id)
            if name is not None:
                out.append(name)
        return out

    if args.yaml:
        rows = [{
            'name': u.name,
            'uid': u.uid,
            'email': u.email,
            'fullname': u.fullname,
            'type': u.type,
            'status': _status_of(u),
            'access': _access_of(u),
        } for u in users]
        console.print(highlight_yaml(dumps_yaml({
            'count': len(rows),
            'users': rows,
        })))
        return

    title = f'Users (count={len(users)}, operator={operator})'
    table = Table(title=title)
    table.add_column('name', style='green', no_wrap=True)
    table.add_column('uid', justify='right')
    table.add_column('type', style='cyan')
    table.add_column('status')
    table.add_column('access')
    if args.long:
        table.add_column('fullname')
        table.add_column('email')
    for u in users:
        status = _status_of(u) or '—'
        access = ', '.join(_access_of(u)) or '—'
        row = [u.name, str(u.uid), u.type, status, access]
        if args.long:
            row += [u.fullname, u.email]
        table.add_row(*row)
    console.print(table)


@user_list.args()
def _(parser: ArgParser):
    parser.add_argument('--status', default=None,
                        help="Filter by user status shorthand "
                             "(e.g. 'active', 'inactive')")
    parser.add_argument('--access', default=None,
                        help="Filter by access shorthand "
                             "(e.g. 'login-ssh', 'sudo')")
    parser.add_argument('--type', default=None, choices=list(USER_TYPES),
                        help='Filter by user type')
    parser.add_argument('--site', '-s', default=None,
                        help='Filter by site membership')
    parser.add_argument('--group', '-g', default=None,
                        help='Filter by group membership')
    parser.add_argument('--operator', default='AND',
                        help='Combine filters with AND (default) or OR. '
                             'Case-insensitive.')
    parser.add_argument('--limit', type=int, default=None,
                        help='Cap the number of users shown')
    parser.add_argument('--long', '-l', action='store_true', default=False,
                        help='Show extra columns (fullname, email)')
    parser.add_argument('--yaml', action='store_true', default=False,
                        help='Output as YAML')
