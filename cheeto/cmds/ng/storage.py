from argparse import Namespace

from ponderosa import ArgParser
from rich.panel import Panel
from rich.table import Table

from .. import commands
from ...constants import MOUNT_FSTYPES, STORAGE_BACKENDS, STORAGE_CATEGORIES
from ...log import Console
from ...models.storage import StorageVolume
from ...operations import (
    CreateHomeStorage,
    CreateStaticMount,
    CreateStorageVolume,
)
from ...queries import find_site_by_name
from ...queries.storage import (
    find_volume,
    list_site_static_mounts,
    list_site_storages,
    list_site_volumes,
)
from ._args import site_args, user_args


@commands.register('ng', 'storage',
                   help='Storage operations')
def storage_cmd(args: Namespace):
    pass


# ---------------------------------------------------------------------------
# `ng storage new home`
# ---------------------------------------------------------------------------


@site_args.apply(required=True)
@user_args.apply(required=True)
@commands.register('ng', 'storage', 'new', 'home',
                   help="Provision a user's home storage (volume + record) "
                        "from the site defaults or explicit args")
async def storage_new_home(args: Namespace):
    console = Console()
    try:
        storage = await CreateHomeStorage.run(
            args.db, args.author,
            user_name=args.user, site_name=args.site,
            quota=args.quota,
            parent_volume=args.parent_volume,
            automount_map=args.automount_map,
            static_mount=args.static_mount,
            no_mount=args.no_mount,
            host=args.host, host_path=args.path,
        )
    except ValueError as e:
        console.print(f'[red]{e}[/]')
        return 1
    console.print(
        f'Created home storage for [green]{args.user}[/] on site {args.site}'
    )


@storage_new_home.args()
def _(parser: ArgParser):
    parser.add_argument('--quota', default=None,
                        help='Quota (default: site default home quota)')
    parser.add_argument('--parent-volume', default=None,
                        help='Parent volume to provision under '
                             '(default: site default home volume)')
    parser.add_argument('--automount-map', default=None,
                        help='Mount via this automount map (default: site '
                             'home mount settings)')
    parser.add_argument('--static-mount', default=None,
                        help='Mount via this static mount (default: site '
                             'home mount settings)')
    parser.add_argument('--no-mount', action='store_true', default=False,
                        help='Create the storage without a mount mechanism')
    parser.add_argument('--host', default=None,
                        help='Escape hatch: create a standalone volume on '
                             'this host instead of under a parent volume')
    parser.add_argument('--path', default=None,
                        help='Host path for --host (default: /home/<user>)')


# ---------------------------------------------------------------------------
# `ng storage list`
# ---------------------------------------------------------------------------


@site_args.apply(required=True)
@commands.register('ng', 'storage', 'list',
                   help='List storage records at a site')
async def storage_list(args: Namespace):
    console = Console()
    site = await find_site_by_name(args.site)
    if site is None:
        console.print(f'[red]Site {args.site} not found[/]')
        return 1
    storages = await list_site_storages(site, category=args.category)
    table = Table(title=f'Storages on {args.site} (count={len(storages)})')
    table.add_column('name', style='green', no_wrap=True)
    table.add_column('category', style='cyan')
    table.add_column('owner')
    table.add_column('volume', style='magenta')
    table.add_column('subpath', style='dim')
    table.add_column('mount', style='yellow')
    table.add_column('quota', justify='right')
    for s in storages:
        try:
            mount = s.mount_path or '—'
        except ValueError as e:
            mount = f'[red]{e}[/]'
        table.add_row(
            s.name, s.category, s.owner.name, s.volume.name,
            s.subpath or '—', mount, s.quota or '—',
        )
    console.print(table)


@storage_list.args()
def _(parser: ArgParser):
    parser.add_argument('--category', default=None,
                        choices=list(STORAGE_CATEGORIES))


# ---------------------------------------------------------------------------
# `ng storage volume ...`
# ---------------------------------------------------------------------------


@commands.register('ng', 'storage', 'volume',
                   help='Storage volume (backing entity) operations')
def storage_volume_cmd(args: Namespace):
    pass


@site_args.apply(required=True)
@commands.register('ng', 'storage', 'volume', 'new',
                   help='Create a storage volume (ZFS dataset / QuoByte '
                        'volume record)')
async def storage_volume_new(args: Namespace):
    console = Console()
    try:
        volume = await CreateStorageVolume.run(
            args.db, args.author,
            site_name=args.site, name=args.name,
            backend=args.backend, host=args.host,
            host_path=args.host_path,
            parent_name=args.parent, quota=args.quota,
            export_options=args.export_options,
            export_ranges=args.export_ranges,
        )
    except ValueError as e:
        console.print(f'[red]{e}[/]')
        return 1
    console.print(
        f'Created volume [green]{volume.name}[/] on {args.site} '
        f'({volume.backend}, {volume.host}:{volume.host_path})'
    )


@storage_volume_new.args()
def _(parser: ArgParser):
    parser.add_argument('name')
    parser.add_argument('--backend', required=True,
                        choices=list(STORAGE_BACKENDS))
    parser.add_argument('--host', required=True)
    parser.add_argument('--host-path', required=True)
    parser.add_argument('--parent', default=None,
                        help='Parent volume name (nested datasets)')
    parser.add_argument('--quota', default=None)
    parser.add_argument('--export-options', default=None)
    parser.add_argument('--export-ranges', nargs='+', default=None)


@site_args.apply(required=True)
@commands.register('ng', 'storage', 'volume', 'list',
                   help='List storage volumes at a site')
async def storage_volume_list(args: Namespace):
    console = Console()
    site = await find_site_by_name(args.site)
    if site is None:
        console.print(f'[red]Site {args.site} not found[/]')
        return 1
    volumes = await list_site_volumes(site)
    table = Table(title=f'Storage volumes on {args.site} (count={len(volumes)})')
    table.add_column('name', style='green', no_wrap=True)
    table.add_column('backend', style='cyan')
    table.add_column('host')
    table.add_column('host_path', style='dim')
    table.add_column('quota', justify='right')
    table.add_column('parent', style='magenta')
    for v in volumes:
        parent = v.parent.name if v.parent is not None else '—'
        table.add_row(
            v.name, v.backend, v.host, v.host_path, v.quota or '—', parent,
        )
    console.print(table)


@site_args.apply(required=True)
@commands.register('ng', 'storage', 'volume', 'show',
                   help='Show one storage volume')
async def storage_volume_show(args: Namespace):
    console = Console()
    site = await find_site_by_name(args.site)
    if site is None:
        console.print(f'[red]Site {args.site} not found[/]')
        return 1
    volume = await find_volume(site, args.name)
    if volume is None:
        console.print(f'[red]Volume {args.name} not found on {args.site}[/]')
        return 1
    n_children = await StorageVolume.find(
        StorageVolume.parent.id == volume.id,
    ).count()

    table = Table(show_header=False, box=None, pad_edge=False, padding=(0, 1))
    table.add_column(style='bold cyan', no_wrap=True)
    table.add_column()
    table.add_row('name', volume.name)
    table.add_row('backend', volume.backend)
    table.add_row('host', volume.host)
    table.add_row('host_path', volume.host_path)
    table.add_row('quota', volume.quota or '[dim](none)[/]')
    if volume.allocations:
        allocs = '\n'.join(
            f'{a.quota}  [dim]{a.comment}[/]' for a in volume.allocations
        )
        table.add_row('allocations', allocs)
    if volume.nfs_export is not None:
        table.add_row('export_options',
                      volume.nfs_export.export_options or '[dim](none)[/]')
        table.add_row('export_ranges',
                      ', '.join(volume.nfs_export.export_ranges) or '[dim](none)[/]')
    table.add_row('children', str(n_children))
    table.add_row('provisioned_at',
                  str(volume.provisioned_at) if volume.provisioned_at
                  else '[dim](unset)[/]')
    console.print(Panel(
        table, title=f'[bold]Volume:[/] [green]{volume.name}[/]',
        border_style='cyan', expand=False,
    ))


@storage_volume_list.args()
def _(parser: ArgParser):
    pass


@storage_volume_show.args()
def _(parser: ArgParser):
    parser.add_argument('name')


# ---------------------------------------------------------------------------
# `ng storage static-mount ...`
# ---------------------------------------------------------------------------


@commands.register('ng', 'storage', 'static-mount',
                   help='Static (fstab-style) mount operations')
def storage_static_mount_cmd(args: Namespace):
    pass


@site_args.apply(required=True)
@commands.register('ng', 'storage', 'static-mount', 'new',
                   help='Create a static mount record')
async def static_mount_new(args: Namespace):
    console = Console()
    try:
        mount = await CreateStaticMount.run(
            args.db, args.author,
            site_name=args.site, name=args.name,
            fstype=args.fstype, mount_path=args.mount_path,
            volume_name=args.volume, subpath=args.subpath,
            spec=args.spec or '', options=args.options,
        )
    except ValueError as e:
        console.print(f'[red]{e}[/]')
        return 1
    console.print(
        f'Created static mount [green]{mount.name}[/] on {args.site} '
        f'at {mount.mount_path}'
    )


@static_mount_new.args()
def _(parser: ArgParser):
    parser.add_argument('name')
    parser.add_argument('--fstype', required=True,
                        choices=list(MOUNT_FSTYPES))
    parser.add_argument('--mount-path', required=True)
    parser.add_argument('--volume', default=None,
                        help='Backing volume name (exclusive with --spec)')
    parser.add_argument('--subpath', default='',
                        help='Subpath under the volume root')
    parser.add_argument('--spec', default=None,
                        help='Raw fstab device spec (e.g. a cvmfs repo)')
    parser.add_argument('--options', nargs='+', default=None,
                        help='Mount options')


@site_args.apply(required=True)
@commands.register('ng', 'storage', 'static-mount', 'list',
                   help='List static mounts at a site')
async def static_mount_list(args: Namespace):
    console = Console()
    site = await find_site_by_name(args.site)
    if site is None:
        console.print(f'[red]Site {args.site} not found[/]')
        return 1
    mounts = await list_site_static_mounts(site)
    table = Table(title=f'Static mounts on {args.site} (count={len(mounts)})')
    table.add_column('name', style='green', no_wrap=True)
    table.add_column('fstype', style='cyan')
    table.add_column('device', style='dim')
    table.add_column('mount_path', style='yellow')
    table.add_column('options')
    for m in mounts:
        table.add_row(
            m.name, m.fstype, m.device_spec, m.mount_path,
            ','.join(m.options),
        )
    console.print(table)
