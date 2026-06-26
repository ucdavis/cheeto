from argparse import Namespace

from ponderosa import ArgParser
from rich.panel import Panel
from rich.table import Table

from .. import commands
from ...constants import MOUNT_FSTYPES, STORAGE_BACKENDS, STORAGE_CATEGORIES
from ...log import Console
from ...models.base import link_target_id
from ...models.storage import StaticMount, Storage, StorageVolume
from ...operations import (
    AddVolumeAllocation,
    CreateAutomountMap,
    CreateHomeStorage,
    CreateStaticMount,
    CreateStorageVolume,
    EditVolumeAllocation,
    RehomeUser,
    RemoveVolumeAllocation,
    SetStorageMount,
    SetVolumeStorageMounts,
)
from ...queries import find_group_by_name, find_site_by_name, find_user_by_name
from ...yaml import print_yaml
from ...queries.storage import (
    find_automount_map,
    find_volume,
    get_storage,
    list_map_storages,
    list_site_automount_maps,
    list_site_static_mounts,
    list_site_storages,
    list_site_volumes,
    mount_mechanism_label,
)
from ._args import group_args, site_args, user_args, yaml_args


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


def _storage_to_dict(s: Storage) -> dict:
    """Plain dict of a Storage's displayed fields for --yaml output."""
    try:
        mount_path = s.mount_path or None
    except ValueError as e:
        mount_path = f'ERROR: {e}'
    return {
        'name': s.name,
        'category': s.category,
        'owner': s.owner.name,
        'group': s.group.name,
        'volume': s.volume.name,
        'host': s.host,
        'host_path': s.host_path,
        'subpath': s.subpath or None,
        'mount_type': mount_mechanism_label(s),
        'mount_path': mount_path,
        'mount_options': s.mount_options,
        'quota': s.quota,
    }


@site_args.apply(required=True)
@user_args.apply()
@group_args.apply()
@yaml_args.apply()
@commands.register('ng', 'storage', 'list',
                   help='List storage records at a site, optionally filtered '
                        'by owner (--user), group, host, and/or category')
async def storage_list(args: Namespace):
    console = Console()
    site = await find_site_by_name(args.site)
    if site is None:
        console.print(f'[red]Site {args.site} not found[/]')
        return 1

    owner_id = group_id = None
    if args.user:
        owner = await find_user_by_name(args.user)
        if owner is None:
            console.print(f'[red]User {args.user} not found[/]')
            return 1
        owner_id = owner.id
    if args.group:
        group = await find_group_by_name(args.group)
        if group is None:
            console.print(f'[red]Group {args.group} not found[/]')
            return 1
        group_id = group.id

    storages = await list_site_storages(
        site, category=args.category, owner_id=owner_id, group_id=group_id,
        host=args.host,
    )

    if args.yaml:
        print_yaml([_storage_to_dict(s) for s in storages])
        return 0

    desc = []
    if args.user:
        desc.append(f'owner={args.user}')
    if args.group:
        desc.append(f'group={args.group}')
    if args.host:
        desc.append(f'host={args.host}')
    if args.category:
        desc.append(f'category={args.category}')
    desc.append(f'count={len(storages)}')
    table = Table(title=f'Storages on {args.site} ({", ".join(desc)})')
    table.add_column('name', style='green', no_wrap=True)
    table.add_column('category', style='cyan')
    table.add_column('owner')
    table.add_column('group')
    table.add_column('volume', style='magenta')
    table.add_column('host')
    table.add_column('subpath', style='dim')
    table.add_column('mount', style='yellow')
    table.add_column('type', style='blue')
    table.add_column('quota', justify='right')
    for s in storages:
        try:
            mount = s.mount_path or '—'
        except ValueError as e:
            mount = f'[red]{e}[/]'
        table.add_row(
            s.name, s.category, s.owner.name, s.group.name, s.volume.name,
            s.host, s.subpath or '—', mount, mount_mechanism_label(s),
            s.quota or '—',
        )
    console.print(table)


@storage_list.args()
def _(parser: ArgParser):
    parser.add_argument('--category', default=None,
                        choices=list(STORAGE_CATEGORIES))
    parser.add_argument('--host', default=None,
                        help='Filter by backing volume host')


# ---------------------------------------------------------------------------
# `ng storage show`
# ---------------------------------------------------------------------------


def _kv_table() -> Table:
    table = Table(show_header=False, box=None, pad_edge=False, padding=(0, 1))
    table.add_column(style='bold cyan', no_wrap=True)
    table.add_column()
    return table


def _prop(fn):
    """Evaluate a derived property, returning an error marker instead of
    raising — mount_path/host_path/device_spec raise when a static mount
    can't cover the storage or a link is unfetched."""
    try:
        return fn()
    except ValueError as e:
        return f'[red]{e}[/]'


def _alloc_lines(volume: StorageVolume) -> str:
    """One line per allocation, prefixed with its 0-based index — the key
    used by `storage volume alloc remove/edit`."""
    return '\n'.join(
        f'[cyan]\\[{i}][/] {a.quota}  [dim]{a.comment}[/]'
        for i, a in enumerate(volume.allocations)
    )


def _render_volume_subtable(volume: StorageVolume) -> Table:
    vt = _kv_table()
    vt.add_row('name', volume.name)
    vt.add_row('backend', volume.backend)
    vt.add_row('host', volume.host)
    vt.add_row('host_path', volume.host_path)
    vt.add_row('quota', volume.quota or '[dim](none)[/]')
    if volume.parent is not None:
        vt.add_row('parent', getattr(volume.parent, 'name', '[dim](unfetched)[/]'))
    if volume.allocations:
        vt.add_row('allocations', _alloc_lines(volume))
    if volume.zfs is not None:
        vt.add_row('zfs dataset', volume.zfs.dataset_name or '[dim](unset)[/]')
    if volume.quobyte is not None:
        vt.add_row('quobyte', f'volume_id={volume.quobyte.volume_id or "—"} '
                              f'tenant={volume.quobyte.tenant or "—"}')
    if volume.nfs_export is not None:
        vt.add_row('export_options',
                   volume.nfs_export.export_options or '[dim](none)[/]')
        vt.add_row('export_ranges',
                   ', '.join(volume.nfs_export.export_ranges) or '[dim](none)[/]')
    if volume.provisioned_at:
        vt.add_row('provisioned_at', str(volume.provisioned_at))
    return vt


def _render_automount_subtable(storage: Storage) -> Table:
    amap = storage.automount_map
    at = _kv_table()
    at.add_row('name', getattr(amap, 'name', '[dim](unfetched)[/]'))
    prefix = getattr(amap, 'prefix', None)
    if prefix:
        at.add_row('prefix', prefix)
    base_opts = getattr(amap, 'options', None)
    if base_opts:
        at.add_row('map options', ','.join(base_opts))
    if storage.mount_name:
        at.add_row('mount_name', storage.mount_name)
    mo = storage.mount_overrides
    if mo.options:
        at.add_row('override options', ','.join(mo.options))
    if mo.add_options:
        at.add_row('add options', ','.join(mo.add_options))
    if mo.remove_options:
        at.add_row('remove options', ','.join(mo.remove_options))
    return at


def _render_static_subtable(sm: StaticMount) -> Table:
    st = _kv_table()
    st.add_row('name', sm.name)
    st.add_row('fstype', sm.fstype)
    st.add_row('mount_path', sm.mount_path)
    st.add_row('device', _prop(lambda: sm.device_spec) or '[dim](none)[/]')
    if sm.subpath:
        st.add_row('subpath', sm.subpath)
    if sm.spec:
        st.add_row('spec', sm.spec)
    st.add_row('options', ','.join(sm.options) or '[dim](none)[/]')
    return st


def _volume_to_dict(volume: StorageVolume, *, n_children: int | None = None) -> dict:
    """Plain dict of a StorageVolume for --yaml output (mirrors the show panel).
    `parent` resolves to a name only when the link was fetched (volume list);
    `volume show` reports the child count via `n_children`."""
    d = {
        'name': volume.name,
        'backend': volume.backend,
        'host': volume.host,
        'host_path': volume.host_path,
        'quota': volume.quota,
        'parent': getattr(volume.parent, 'name', None)
        if volume.parent is not None else None,
        'allocations': [
            {'quota': a.quota, 'comment': a.comment} for a in volume.allocations
        ],
        'zfs_dataset': volume.zfs.dataset_name if volume.zfs is not None else None,
        'quobyte': (
            {'volume_id': volume.quobyte.volume_id,
             'tenant': volume.quobyte.tenant}
            if volume.quobyte is not None else None
        ),
        'nfs_export': (
            {'export_options': volume.nfs_export.export_options,
             'export_ranges': list(volume.nfs_export.export_ranges)}
            if volume.nfs_export is not None else None
        ),
        'provisioned_at': volume.provisioned_at,
    }
    if n_children is not None:
        d['children'] = n_children
    return d


def _static_mount_to_dict(sm: StaticMount) -> dict:
    """Plain dict of a StaticMount for --yaml output."""
    try:
        device = sm.device_spec
    except ValueError as e:
        device = f'ERROR: {e}'
    return {
        'name': sm.name,
        'fstype': sm.fstype,
        'mount_path': sm.mount_path,
        'device': device,
        'subpath': sm.subpath or None,
        'spec': sm.spec or None,
        'options': list(sm.options),
    }


def _automount_map_to_dict(amap, *, entries: list | None = None) -> dict:
    """Plain dict of an AutomountMap for --yaml output; `entries` (for show)
    is the list of storages mounted under it."""
    d = {
        'name': amap.name,
        'prefix': amap.prefix,
        'options': list(amap.options),
    }
    if entries is not None:
        d['entries'] = entries
    return d


def _render_storage_panel(storage: Storage) -> Panel:
    t = _kv_table()
    t.add_row('name', storage.name)
    t.add_row('category', storage.category)
    t.add_row('owner', storage.owner.name)
    t.add_row('group', storage.group.name)
    t.add_row('mount type', mount_mechanism_label(storage))
    t.add_row('mount path', _prop(lambda: storage.mount_path) or '[dim](none)[/]')

    opts = _prop(lambda: storage.mount_options)
    if isinstance(opts, list):
        opts = ','.join(opts) if opts else '[dim](none)[/]'
    t.add_row('mount options', opts)

    if storage.subpath:
        t.add_row('subpath', storage.subpath)
    t.add_row('host_path', _prop(lambda: storage.host_path) or '[dim](none)[/]')
    t.add_row('quota', _prop(lambda: storage.quota) or '[dim](none)[/]')
    if storage.globus:
        t.add_row('globus', 'yes')
    if storage.expires_at:
        t.add_row('expires_at', str(storage.expires_at))
    if storage.provisioned_at:
        t.add_row('provisioned_at', str(storage.provisioned_at))
    if storage.nfs_export is not None:
        t.add_row('nfs_export (storage override)',
                  f'{storage.nfs_export.export_options or "[dim](no opts)[/]"} '
                  f'ranges={", ".join(storage.nfs_export.export_ranges) or "—"}')

    t.add_row('volume', _render_volume_subtable(storage.volume))
    if storage.automount_map is not None:
        t.add_row('automount map', _render_automount_subtable(storage))
    elif storage.static_mount is not None:
        t.add_row('static mount', _render_static_subtable(storage.static_mount))

    return Panel(
        t,
        title=f'[bold]Storage:[/] [green]{storage.name}[/] '
              f'[dim]({storage.category})[/]',
        border_style='green', expand=False,
    )


@site_args.apply(required=True)
@yaml_args.apply()
@commands.register('ng', 'storage', 'show',
                   help='Show a storage record in full detail, including its '
                        'backing volume and mount mechanism')
async def storage_show(args: Namespace):
    console = Console()
    site = await find_site_by_name(args.site)
    if site is None:
        console.print(f'[red]Site {args.site} not found[/]')
        return 1
    storage = await get_storage(site, args.name, args.category)
    if storage is None:
        suffix = f' (category={args.category})' if args.category else ''
        console.print(
            f'[red]Storage {args.name} not found on {args.site}{suffix}[/]'
        )
        return 1
    if args.yaml:
        print_yaml(_storage_to_dict(storage))
        return 0
    console.print(_render_storage_panel(storage))


@storage_show.args()
def _(parser: ArgParser):
    parser.add_argument('name')
    parser.add_argument('--category', default=None,
                        choices=list(STORAGE_CATEGORIES),
                        help='Disambiguate when a name exists in multiple '
                             'categories')


# ---------------------------------------------------------------------------
# `ng storage set-mount`
# ---------------------------------------------------------------------------


@site_args.apply(required=True)
@commands.register('ng', 'storage', 'set-mount',
                   help="Set, change, or clear a storage's mount mechanism")
async def storage_set_mount(args: Namespace):
    console = Console()
    try:
        storage = await SetStorageMount.run(
            args.db, args.author,
            site_name=args.site, name=args.name, category=args.category,
            automount_map=args.automount_map, mount_name=args.mount_name or '',
            static_mount=args.static_mount, no_mount=args.no_mount,
        )
    except ValueError as e:
        console.print(f'[red]{e}[/]')
        return 1
    console.print(
        f'Set mount for [green]{args.name}[/] on {args.site} -> '
        f'{mount_mechanism_label(storage)}'
    )


@storage_set_mount.args()
def _(parser: ArgParser):
    parser.add_argument('--name', required=True, help='Storage name')
    parser.add_argument('--category', default=None,
                        choices=list(STORAGE_CATEGORIES),
                        help='Disambiguate when a name exists in multiple '
                             'categories')
    parser.add_argument('--mount-name', default=None,
                        help='Automount entry name (default: storage name)')
    mech = parser.add_mutually_exclusive_group(required=True)
    mech.add_argument('--automount-map', default=None,
                      help='Attach via this automount map')
    mech.add_argument('--static-mount', default=None,
                      help='Attach via this static mount')
    mech.add_argument('--no-mount', action='store_true', default=False,
                      help='Clear the mount mechanism')


# ---------------------------------------------------------------------------
# `ng storage rehome`
# ---------------------------------------------------------------------------


@site_args.apply(required=True)
@user_args.apply(required=True)
@commands.register('ng', 'storage', 'rehome',
                   help="Move a user's home storage onto the site default "
                        'home volume (removes the current home record and '
                        'recreates it from the site defaults)')
async def storage_rehome(args: Namespace):
    console = Console()
    site = await find_site_by_name(args.site)
    if site is None:
        console.print(f'[red]Site {args.site} not found[/]')
        return 1

    storage = await get_storage(site, args.user, 'home')
    if storage is None:
        console.print(f'[red]No home storage for {args.user} on {args.site}[/]')
        return 1

    default_id = link_target_id(site.storage.default_home_volume)
    if default_id is None:
        console.print(
            f'[red]Site {args.site} has no default home volume; set one with '
            f'`ng site storage set-defaults`[/]'
        )
        return 1

    if link_target_id(storage.volume.parent) == default_id:
        console.print(
            f"[green]{args.user}[/]'s home on {args.site} is already on the "
            f'site default home volume; nothing to do'
        )
        return 0

    default_volume = await StorageVolume.get(default_id)
    console.print(_render_storage_panel(storage))
    new_name = (f'{default_volume.name}/{args.user}'
                if default_volume is not None else '[red](default dangling)[/]')
    console.print(
        f'[bold]Rehome plan:[/] volume [magenta]{storage.volume.name}[/] '
        f'-> [magenta]{new_name}[/]; quota -> '
        f'{site.storage.default_home_quota or "[dim](none)[/]"} (site default)'
    )
    console.print(
        '[yellow]The old volume record is left in place and files on disk are '
        'NOT moved; this only changes where the home is provisioned.[/]'
    )

    if not args.force:
        try:
            answer = input(
                f'Rehome {args.user} on {args.site}? [y/N]: '
            ).strip().lower()
        except (EOFError, KeyboardInterrupt):
            console.print('\n[red]Aborted.[/]')
            return 1
        if answer != 'y':
            console.print('[red]Aborted.[/]')
            return 1

    try:
        new_storage = await RehomeUser.run(
            args.db, args.author,
            user_name=args.user, site_name=args.site,
        )
    except ValueError as e:
        console.print(f'[red]{e}[/]')
        return 1
    console.print(
        f'Rehomed [green]{args.user}[/] on {args.site}: '
        f'[magenta]{storage.volume.name}[/] -> '
        f'[magenta]{new_storage.volume.name}[/] (old volume left in place)'
    )


@storage_rehome.args()
def _(parser: ArgParser):
    parser.add_argument('--force', '-f', action='store_true', default=False,
                        help='Skip the confirmation prompt')


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
@yaml_args.apply()
@commands.register('ng', 'storage', 'volume', 'list',
                   help='List storage volumes at a site')
async def storage_volume_list(args: Namespace):
    console = Console()
    site = await find_site_by_name(args.site)
    if site is None:
        console.print(f'[red]Site {args.site} not found[/]')
        return 1
    volumes = await list_site_volumes(site)
    if args.yaml:
        print_yaml([_volume_to_dict(v) for v in volumes])
        return 0
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
@yaml_args.apply()
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
    if args.yaml:
        print_yaml(_volume_to_dict(volume, n_children=n_children))
        return 0

    table = Table(show_header=False, box=None, pad_edge=False, padding=(0, 1))
    table.add_column(style='bold cyan', no_wrap=True)
    table.add_column()
    table.add_row('name', volume.name)
    table.add_row('backend', volume.backend)
    table.add_row('host', volume.host)
    table.add_row('host_path', volume.host_path)
    table.add_row('quota', volume.quota or '[dim](none)[/]')
    if volume.allocations:
        table.add_row('allocations', _alloc_lines(volume))
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


@site_args.apply(required=True)
@commands.register('ng', 'storage', 'volume', 'set-mounts',
                   help='Set the mount mechanism on every storage backed by a '
                        "volume's full descendant subtree")
async def storage_volume_set_mounts(args: Namespace):
    console = Console()
    try:
        result = await SetVolumeStorageMounts.run(
            args.db, args.author,
            site_name=args.site, volume_name=args.name,
            automount_map=args.automount_map,
            static_mount=args.static_mount, no_mount=args.no_mount,
        )
    except ValueError as e:
        console.print(f'[red]{e}[/]')
        return 1
    console.print(
        f'Updated [green]{result["updated"]}[/] storage(s) under '
        f'[magenta]{args.name}[/] -> {result["mechanism"] or "(none matched)"}'
    )
    for warning in result['warnings']:
        console.print(f'[yellow]warning:[/] {warning}')


@storage_volume_set_mounts.args()
def _(parser: ArgParser):
    parser.add_argument('name', help='Parent volume name (subtree root)')
    mech = parser.add_mutually_exclusive_group(required=True)
    mech.add_argument('--automount-map', default=None,
                      help='Attach all to this automount map')
    mech.add_argument('--static-mount', default=None,
                      help='Attach all to this static mount')
    mech.add_argument('--no-mount', action='store_true', default=False,
                      help='Clear the mount mechanism on all')


# ---------------------------------------------------------------------------
# `ng storage volume alloc ...`
# ---------------------------------------------------------------------------


@commands.register('ng', 'storage', 'volume', 'alloc',
                   help="Manage a volume's quota allocations")
def storage_volume_alloc_cmd(args: Namespace):
    pass


def _print_allocs(console: Console, volume: StorageVolume) -> None:
    if volume.allocations:
        console.print(_alloc_lines(volume))
        console.print(
            f'volume [magenta]{volume.name}[/] quota: [bold]{volume.quota}[/] '
            f'across {len(volume.allocations)} allocation(s)'
        )
    else:
        console.print(
            f'volume [magenta]{volume.name}[/] now has '
            f'[dim]no allocations (no quota)[/]'
        )


@site_args.apply(required=True)
@commands.register('ng', 'storage', 'volume', 'alloc', 'add',
                   help='Add a quota allocation to a volume')
async def storage_volume_alloc_add(args: Namespace):
    console = Console()
    try:
        volume = await AddVolumeAllocation.run(
            args.db, args.author,
            site_name=args.site, volume_name=args.name,
            quota=args.quota, comment=args.comment,
        )
    except ValueError as e:
        console.print(f'[red]{e}[/]')
        return 1
    console.print(
        f'Added allocation [green]{args.quota}[/] to [magenta]{args.name}[/] '
        f'on {args.site}'
    )
    _print_allocs(console, volume)


@storage_volume_alloc_add.args()
def _(parser: ArgParser):
    parser.add_argument('name', help='Volume name')
    parser.add_argument('--quota', required=True,
                        help='Allocation quota (e.g. 1T)')
    parser.add_argument('--comment', default='',
                        help='Allocation comment / label')


@site_args.apply(required=True)
@commands.register('ng', 'storage', 'volume', 'alloc', 'remove',
                   help='Remove a quota allocation from a volume by index')
async def storage_volume_alloc_remove(args: Namespace):
    console = Console()
    try:
        volume = await RemoveVolumeAllocation.run(
            args.db, args.author,
            site_name=args.site, volume_name=args.name, index=args.index,
        )
    except ValueError as e:
        console.print(f'[red]{e}[/]')
        return 1
    console.print(
        f'Removed allocation [yellow]\\[{args.index}][/] from '
        f'[magenta]{args.name}[/] on {args.site}'
    )
    _print_allocs(console, volume)


@storage_volume_alloc_remove.args()
def _(parser: ArgParser):
    parser.add_argument('name', help='Volume name')
    parser.add_argument('--index', type=int, required=True,
                        help='0-based allocation index (see `volume show`)')


@site_args.apply(required=True)
@commands.register('ng', 'storage', 'volume', 'alloc', 'edit',
                   help="Edit a quota allocation's quota (and optionally its "
                        'comment) by index')
async def storage_volume_alloc_edit(args: Namespace):
    console = Console()
    try:
        volume = await EditVolumeAllocation.run(
            args.db, args.author,
            site_name=args.site, volume_name=args.name, index=args.index,
            quota=args.quota, comment=args.comment,
        )
    except ValueError as e:
        console.print(f'[red]{e}[/]')
        return 1
    console.print(
        f'Edited allocation [green]\\[{args.index}][/] on '
        f'[magenta]{args.name}[/] on {args.site}'
    )
    _print_allocs(console, volume)


@storage_volume_alloc_edit.args()
def _(parser: ArgParser):
    parser.add_argument('name', help='Volume name')
    parser.add_argument('--index', type=int, required=True,
                        help='0-based allocation index (see `volume show`)')
    parser.add_argument('--quota', required=True, help='New quota (e.g. 2T)')
    parser.add_argument('--comment', default=None,
                        help='Optionally relabel the allocation')


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
@yaml_args.apply()
@commands.register('ng', 'storage', 'static-mount', 'list',
                   help='List static mounts at a site')
async def static_mount_list(args: Namespace):
    console = Console()
    site = await find_site_by_name(args.site)
    if site is None:
        console.print(f'[red]Site {args.site} not found[/]')
        return 1
    mounts = await list_site_static_mounts(site)
    if args.yaml:
        print_yaml([_static_mount_to_dict(m) for m in mounts])
        return 0
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


# ---------------------------------------------------------------------------
# `ng storage automount-map ...`
# ---------------------------------------------------------------------------


@commands.register('ng', 'storage', 'automount-map',
                   help='Automount table (autofs map) operations')
def storage_automount_map_cmd(args: Namespace):
    pass


@site_args.apply(required=True)
@commands.register('ng', 'storage', 'automount-map', 'new',
                   help='Create an automount map (autofs table)')
async def automount_map_new(args: Namespace):
    console = Console()
    try:
        amap = await CreateAutomountMap.run(
            args.db, args.author,
            site_name=args.site, name=args.name,
            prefix=args.prefix, options=args.options,
        )
    except ValueError as e:
        console.print(f'[red]{e}[/]')
        return 1
    console.print(
        f'Created automount map [green]{amap.name}[/] on {args.site} '
        f'(prefix={amap.prefix})'
    )


@automount_map_new.args()
def _(parser: ArgParser):
    parser.add_argument('name')
    parser.add_argument('--prefix', required=True,
                        help='Autofs mount prefix (e.g. /home)')
    parser.add_argument('--options', nargs='+', default=None,
                        help='Base mount options for entries in this map')


@site_args.apply(required=True)
@yaml_args.apply()
@commands.register('ng', 'storage', 'automount-map', 'list',
                   help='List automount maps at a site')
async def automount_map_list(args: Namespace):
    console = Console()
    site = await find_site_by_name(args.site)
    if site is None:
        console.print(f'[red]Site {args.site} not found[/]')
        return 1
    maps = await list_site_automount_maps(site)
    if args.yaml:
        rows = []
        for m in maps:
            n = await Storage.find(Storage.automount_map.id == m.id).count()
            rows.append({**_automount_map_to_dict(m), 'storages': n})
        print_yaml(rows)
        return 0
    table = Table(title=f'Automount maps on {args.site} (count={len(maps)})')
    table.add_column('name', style='green', no_wrap=True)
    table.add_column('prefix', style='cyan')
    table.add_column('options')
    table.add_column('storages', justify='right')
    for m in maps:
        n = await Storage.find(Storage.automount_map.id == m.id).count()
        table.add_row(m.name, m.prefix, ','.join(m.options) or '—', str(n))
    console.print(table)


@site_args.apply(required=True)
@yaml_args.apply()
@commands.register('ng', 'storage', 'automount-map', 'show',
                   help='Show an automount map and its entries (the storages '
                        'mounted under it)')
async def automount_map_show(args: Namespace):
    console = Console()
    site = await find_site_by_name(args.site)
    if site is None:
        console.print(f'[red]Site {args.site} not found[/]')
        return 1
    amap = await find_automount_map(site, args.name)
    if amap is None:
        console.print(f'[red]Automount map {args.name} not found on {args.site}[/]')
        return 1
    storages = await list_map_storages(amap)
    if args.yaml:
        entries = []
        for s in storages:
            try:
                device = f'{s.host}:{s.host_path}'
            except ValueError as e:
                device = f'ERROR: {e}'
            try:
                options = list(s.mount_options)
            except ValueError as e:
                options = f'ERROR: {e}'
            entries.append({'entry': s.mount_name or s.name,
                            'device': device, 'options': options})
        print_yaml(_automount_map_to_dict(amap, entries=entries))
        return 0
    table = Table(
        title=f'Automount map {amap.name} '
              f'(prefix={amap.prefix}, {len(storages)} entries)',
    )
    table.add_column('entry', style='green', no_wrap=True)
    table.add_column('device', style='dim')
    table.add_column('options')
    for s in storages:
        try:
            device = f'{s.host}:{s.host_path}'
        except ValueError as e:
            device = f'[red]{e}[/]'
        try:
            options = ','.join(s.mount_options) or '—'
        except ValueError as e:
            options = f'[red]{e}[/]'
        table.add_row(s.mount_name or s.name, device, options)
    console.print(table)


@automount_map_show.args()
def _(parser: ArgParser):
    parser.add_argument('name')
