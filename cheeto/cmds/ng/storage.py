from argparse import Namespace

from ponderosa import ArgParser
from rich.panel import Panel
from rich.table import Table

from .. import commands
from ...constants import MOUNT_FSTYPES, STORAGE_BACKENDS, STORAGE_CATEGORIES
from ...log import Console
from ...models.base import link_target_id
from ...models.host import StorageHost
from ...models.site import Site
from ...models.storage import StaticMount, Storage, StorageVolume
from ...operations import (
    AddVolumeAllocation,
    BackfillStorageHosts,
    CreateAutomountMap,
    CreateGroupStorage,
    CreateHomeStorage,
    CreateStaticMount,
    CreateStorageHost,
    CreateStorageVolume,
    DeleteStorageHost,
    EditStorageHost,
    EditStorageVolume,
    EditVolumeAllocation,
    RehomeUser,
    RemoveVolumeAllocation,
    SetStorageMount,
    SetVolumeStorageMounts,
)
from ...operations.base import UNSET
from ...queries import find_group_by_name, find_site_by_name, find_user_by_name
from ...yaml import print_yaml
from ...queries.storage import (
    effective_nfs_export,
    find_automount_map,
    find_storage_host,
    find_volume,
    get_storage,
    list_map_storages,
    list_site_automount_maps,
    list_site_static_mounts,
    list_site_storage_hosts,
    list_site_storages,
    list_site_volumes,
    mount_mechanism_label,
    volume_counts_by_host,
)
from . import parent
from ._args import (
    group_args,
    has_storage_defaults_args,
    site_args,
    storage_defaults_args,
    storage_defaults_kwargs,
    user_args,
    yaml_args,
)


parent('ng', 'storage', help='Storage operations')
parent('ng', 'storage', 'new',
       help='Create storage records, volumes, hosts, static mounts, and '
            'automount maps')
parent('ng', 'storage', 'list',
       help='List storage-related records at a site')
parent('ng', 'storage', 'show',
       help='Show a storage-related record in detail')
parent('ng', 'storage', 'set',
       help='Set mount mechanisms on storages')
parent('ng', 'storage', 'add',
       help='Add sub-records (volume quota allocations)')
parent('ng', 'storage', 'remove',
       help='Remove sub-records (volume quota allocations)')
parent('ng', 'storage', 'edit',
       help='Edit volumes, storage hosts, and volume quota allocations')
parent('ng', 'storage', 'delete',
       help='Delete storage hosts')


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
# `ng storage new group`
# ---------------------------------------------------------------------------


@site_args.apply(required=True)
@group_args.apply(required=True)
@commands.register('ng', 'storage', 'new', 'group',
                   help="Provision a group's shared storage (volume + record) "
                        'under a parent volume or host')
async def storage_new_group(args: Namespace):
    console = Console()
    try:
        await CreateGroupStorage.run(
            args.db, args.author,
            group_name=args.group, site_name=args.site,
            owner_name=args.owner,
            quota=args.quota, comment=args.comment,
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
        f'Created group storage for [green]{args.group}[/] on site {args.site}'
    )


@storage_new_group.args()
def _(parser: ArgParser):
    parser.add_argument('--owner', default=None,
                        help='Owning user (default: the group sponsor at the '
                             'site; required if there is no single sponsor)')
    parser.add_argument('--quota', required=True,
                        help='Quota for the group volume (e.g. 10T)')
    parser.add_argument('--comment', default=None,
                        help='Label for the initial quota allocation, e.g. a '
                             "ticket or purchase reference (default: "
                             "'initial group allocation')")
    parser.add_argument('--parent-volume', default=None,
                        help='Parent volume to provision under (required '
                             'unless --host is given)')
    parser.add_argument('--automount-map', default=None,
                        help="Mount via this automount map (default: a map "
                             "named 'group' at the site)")
    parser.add_argument('--static-mount', default=None,
                        help='Mount via this static mount')
    parser.add_argument('--no-mount', action='store_true', default=False,
                        help='Create the storage without a mount mechanism')
    parser.add_argument('--host', default=None,
                        help='Escape hatch: create a standalone volume on '
                             'this host instead of under a parent volume')
    parser.add_argument('--path', default=None,
                        help='Host path for --host (default: /group/<group>)')


# ---------------------------------------------------------------------------
# `ng storage list storages`
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
@commands.register('ng', 'storage', 'list', 'storages',
                   help='List storage records at a site, optionally filtered '
                        'by owner (--user), group, host, and/or category')
async def storage_list_storages(args: Namespace):
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


@storage_list_storages.args()
def _(parser: ArgParser):
    parser.add_argument('--category', default=None,
                        choices=list(STORAGE_CATEGORIES))
    parser.add_argument('--host', default=None,
                        help='Filter by backing volume host')


# ---------------------------------------------------------------------------
# `ng storage show storage`
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
    used by `storage remove/edit allocation`."""
    return '\n'.join(
        f'[cyan]\\[{i}][/] {a.quota}  [dim]{a.comment}[/]'
        for i, a in enumerate(volume.allocations)
    )


def _host_link_label(volume: StorageVolume) -> str:
    """The StorageHost a volume links to: hostname when fetched, 'linked'
    when only the Link is known, or a hint when the backfill has not run."""
    sh = volume.storage_host
    if sh is None:
        return '[dim]— (unlinked; run `storage backfill-hosts`)[/]'
    return getattr(sh, 'hostname', None) or 'linked'


def _effective_export_lines(effective) -> str:
    cfg, levels = effective
    if cfg is None:
        return '[dim](none at any tier)[/]'
    opts = cfg.export_options or '[dim](none)[/]'
    ranges = ', '.join(cfg.export_ranges) or '[dim](none)[/]'
    return (
        f'{opts}  [dim](options: {levels["export_options"]})[/]\n'
        f'{ranges}  [dim](ranges: {levels["export_ranges"]})[/]'
    )


def _effective_to_dict(effective) -> dict | None:
    cfg, levels = effective
    if cfg is None:
        return None
    return {
        'export_options': cfg.export_options,
        'export_ranges': list(cfg.export_ranges),
        'source': dict(levels),
    }


async def _volume_tiers(volume: StorageVolume):
    """(site, storage_host) default tiers for a volume, using the fetched
    links when present and falling back to lookups (the host by name, so
    unlinked pre-backfill volumes still resolve)."""
    site = volume.site
    if not isinstance(site, Site):
        site_id = link_target_id(site)
        site = await Site.get(site_id) if site_id is not None else None
    host = volume.storage_host
    if not isinstance(host, StorageHost):
        host = (
            await find_storage_host(site, volume.host)
            if site is not None else None
        )
    return site, host


async def _effective_export(volume: StorageVolume, storage: Storage | None = None):
    site, host = await _volume_tiers(volume)
    return effective_nfs_export(
        volume=volume, host=host, site=site, storage=storage,
    )


def _render_volume_subtable(volume: StorageVolume, *, effective=None) -> Table:
    vt = _kv_table()
    vt.add_row('name', volume.name)
    vt.add_row('backend', volume.backend)
    vt.add_row('host', volume.host)
    vt.add_row('storage_host', _host_link_label(volume))
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
    if effective is not None:
        vt.add_row('effective export', _effective_export_lines(effective))
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


def _volume_to_dict(
    volume: StorageVolume, *, n_children: int | None = None, effective=None,
) -> dict:
    """Plain dict of a StorageVolume for --yaml output (mirrors the show panel).
    `parent` resolves to a name only when the link was fetched (list volumes);
    `show volume` reports the child count via `n_children` and the resolved
    export config via `effective`."""
    d = {
        'name': volume.name,
        'backend': volume.backend,
        'host': volume.host,
        'storage_host_linked': link_target_id(volume.storage_host) is not None,
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
    if effective is not None:
        d['effective_nfs_export'] = _effective_to_dict(effective)
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


def _render_storage_panel(storage: Storage, *, effective=None) -> Panel:
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

    t.add_row('volume', _render_volume_subtable(storage.volume, effective=effective))
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
@commands.register('ng', 'storage', 'show', 'storage',
                   help='Show a storage record in full detail, including its '
                        'backing volume and mount mechanism')
async def storage_show_storage(args: Namespace):
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
    effective = await _effective_export(storage.volume, storage)
    if args.yaml:
        data = _storage_to_dict(storage)
        data['effective_nfs_export'] = _effective_to_dict(effective)
        print_yaml(data)
        return 0
    console.print(_render_storage_panel(storage, effective=effective))


@storage_show_storage.args()
def _(parser: ArgParser):
    parser.add_argument('name')
    parser.add_argument('--category', default=None,
                        choices=list(STORAGE_CATEGORIES),
                        help='Disambiguate when a name exists in multiple '
                             'categories')


# ---------------------------------------------------------------------------
# `ng storage set mount`
# ---------------------------------------------------------------------------


@site_args.apply(required=True)
@commands.register('ng', 'storage', 'set', 'mount',
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
    parser.add_argument('name', help='Storage name')
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
            f'`ng site set storage-defaults`[/]'
        )
        return 1

    if link_target_id(storage.volume.parent) == default_id:
        console.print(
            f"[green]{args.user}[/]'s home on {args.site} is already on the "
            f'site default home volume; nothing to do'
        )
        return 0

    default_volume = await StorageVolume.get(default_id)
    console.print(_render_storage_panel(
        storage, effective=await _effective_export(storage.volume, storage),
    ))
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
# `ng storage new volume`
# ---------------------------------------------------------------------------


@site_args.apply(required=True)
@commands.register('ng', 'storage', 'new', 'volume',
                   help='Create a storage volume (ZFS dataset / QuoByte '
                        'volume record) on an existing storage host')
async def storage_new_volume(args: Namespace):
    console = Console()
    try:
        volume = await CreateStorageVolume.run(
            args.db, args.author,
            site_name=args.site, name=args.name,
            backend=args.backend, host=args.host,
            host_path=args.host_path, template=args.template,
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


@storage_new_volume.args()
def _(parser: ArgParser):
    parser.add_argument('name')
    parser.add_argument('--backend', required=True,
                        choices=list(STORAGE_BACKENDS))
    parser.add_argument('--host', required=True,
                        help='Storage host (must exist: `storage new host`)')
    parser.add_argument('--host-path', default=None,
                        help='Path on the host; omit to derive it from the '
                             "host/site ZFS path template selected by "
                             '--template')
    parser.add_argument('--template', default=None,
                        choices=list(STORAGE_CATEGORIES),
                        help='Storage category whose ZFS path template '
                             'derives --host-path ({name} = last component '
                             'of the volume name)')
    parser.add_argument('--parent', default=None,
                        help='Parent volume name (nested datasets)')
    parser.add_argument('--quota', default=None)
    parser.add_argument('--export-options', default=None,
                        help='Volume-level export options (omit to inherit '
                             'the host/site default)')
    parser.add_argument('--export-ranges', nargs='+', default=None,
                        help='Volume-level export ranges (omit to inherit '
                             'the host/site default)')


# ---------------------------------------------------------------------------
# `ng storage list volumes` / `ng storage show volume`
# ---------------------------------------------------------------------------


@site_args.apply(required=True)
@yaml_args.apply()
@commands.register('ng', 'storage', 'list', 'volumes',
                   help='List storage volumes at a site')
async def storage_list_volumes(args: Namespace):
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
    table.add_column('host rec', style='dim')
    table.add_column('host_path', style='dim')
    table.add_column('quota', justify='right')
    table.add_column('parent', style='magenta')
    for v in volumes:
        parent = getattr(v.parent, 'name', '—')
        linked = 'linked' if link_target_id(v.storage_host) is not None else '—'
        table.add_row(
            v.name, v.backend, v.host, linked, v.host_path, v.quota or '—',
            parent,
        )
    console.print(table)


@site_args.apply(required=True)
@yaml_args.apply()
@commands.register('ng', 'storage', 'show', 'volume',
                   help='Show one storage volume')
async def storage_show_volume(args: Namespace):
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
    effective = await _effective_export(volume)
    if args.yaml:
        print_yaml(_volume_to_dict(
            volume, n_children=n_children, effective=effective,
        ))
        return 0

    table = _render_volume_subtable(volume, effective=effective)
    table.add_row('children', str(n_children))
    if not volume.provisioned_at:
        table.add_row('provisioned_at', '[dim](unset)[/]')
    console.print(Panel(
        table, title=f'[bold]Volume:[/] [green]{volume.name}[/]',
        border_style='cyan', expand=False,
    ))


@storage_show_volume.args()
def _(parser: ArgParser):
    parser.add_argument('name')


# ---------------------------------------------------------------------------
# `ng storage set volume-mounts`
# ---------------------------------------------------------------------------


@site_args.apply(required=True)
@commands.register('ng', 'storage', 'set', 'volume-mounts',
                   help='Set the mount mechanism on every storage backed by a '
                        "volume's full descendant subtree")
async def storage_set_volume_mounts(args: Namespace):
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


@storage_set_volume_mounts.args()
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
# `ng storage add/remove/edit allocation`
# ---------------------------------------------------------------------------


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
@commands.register('ng', 'storage', 'add', 'allocation',
                   help='Add a quota allocation to a volume')
async def storage_add_allocation(args: Namespace):
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


@storage_add_allocation.args()
def _(parser: ArgParser):
    parser.add_argument('name', help='Volume name')
    parser.add_argument('--quota', required=True,
                        help='Allocation quota (e.g. 1T)')
    parser.add_argument('--comment', default='',
                        help='Allocation comment / label')


@site_args.apply(required=True)
@commands.register('ng', 'storage', 'remove', 'allocation',
                   help='Remove a quota allocation from a volume by index')
async def storage_remove_allocation(args: Namespace):
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


@storage_remove_allocation.args()
def _(parser: ArgParser):
    parser.add_argument('name', help='Volume name')
    parser.add_argument('--index', type=int, required=True,
                        help='0-based allocation index (see `storage show '
                             'volume`)')


@site_args.apply(required=True)
@commands.register('ng', 'storage', 'edit', 'allocation',
                   help="Edit a quota allocation's quota (and optionally its "
                        'comment) by index')
async def storage_edit_allocation(args: Namespace):
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


@storage_edit_allocation.args()
def _(parser: ArgParser):
    parser.add_argument('name', help='Volume name')
    parser.add_argument('--index', type=int, required=True,
                        help='0-based allocation index (see `storage show '
                             'volume`)')
    parser.add_argument('--quota', required=True, help='New quota (e.g. 2T)')
    parser.add_argument('--comment', default=None,
                        help='Optionally relabel the allocation')


# ---------------------------------------------------------------------------
# `ng storage new static-mount` / `ng storage list static-mounts`
# ---------------------------------------------------------------------------


@site_args.apply(required=True)
@commands.register('ng', 'storage', 'new', 'static-mount',
                   help='Create a static mount record')
async def storage_new_static_mount(args: Namespace):
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


@storage_new_static_mount.args()
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
@commands.register('ng', 'storage', 'list', 'static-mounts',
                   help='List static mounts at a site')
async def storage_list_static_mounts(args: Namespace):
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
# `ng storage new automount-map` / `list automount-maps` / `show automount-map`
# ---------------------------------------------------------------------------


@site_args.apply(required=True)
@commands.register('ng', 'storage', 'new', 'automount-map',
                   help='Create an automount map (autofs table)')
async def storage_new_automount_map(args: Namespace):
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


@storage_new_automount_map.args()
def _(parser: ArgParser):
    parser.add_argument('name')
    parser.add_argument('--prefix', required=True,
                        help='Autofs mount prefix (e.g. /home)')
    parser.add_argument('--options', nargs='+', default=None,
                        help='Base mount options for entries in this map')


@site_args.apply(required=True)
@yaml_args.apply()
@commands.register('ng', 'storage', 'list', 'automount-maps',
                   help='List automount maps at a site')
async def storage_list_automount_maps(args: Namespace):
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
@commands.register('ng', 'storage', 'show', 'automount-map',
                   help='Show an automount map and its entries (the storages '
                        'mounted under it)')
async def storage_show_automount_map(args: Namespace):
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


@storage_show_automount_map.args()
def _(parser: ArgParser):
    parser.add_argument('name')


# ---------------------------------------------------------------------------
# `ng storage edit volume`
# ---------------------------------------------------------------------------


def _unset_if_none(value):
    return UNSET if value is None else value


@site_args.apply(required=True)
@commands.register('ng', 'storage', 'edit', 'volume',
                   help='Edit a storage volume: rename, move host/path, or '
                        'change its own export config')
async def storage_edit_volume(args: Namespace):
    console = Console()
    try:
        volume = await EditStorageVolume.run(
            args.db, args.author,
            site_name=args.site, volume_name=args.name,
            new_name=_unset_if_none(args.rename),
            host=_unset_if_none(args.host),
            host_path=_unset_if_none(args.host_path),
            export_options=_unset_if_none(args.export_options),
            export_ranges=_unset_if_none(args.export_ranges),
            clear_nfs_export=args.clear_export,
            zfs_dataset_name=_unset_if_none(args.zfs_dataset),
        )
    except ValueError as e:
        console.print(f'[red]{e}[/]')
        return 1
    console.print(
        f'Edited volume [green]{volume.name}[/] on {args.site} '
        f'({volume.host}:{volume.host_path})'
    )
    effective = await _effective_export(volume)
    console.print(_effective_export_lines(effective))


@storage_edit_volume.args()
def _(parser: ArgParser):
    parser.add_argument('name', help='Volume name')
    parser.add_argument('--rename', default=None, metavar='NEW_NAME',
                        help='New volume name (refused while the volume has '
                             'children)')
    parser.add_argument('--host', default=None,
                        help='Move to this storage host (must exist; refused '
                             'while the volume has children)')
    parser.add_argument('--host-path', default=None,
                        help='New path on the host (refused while the volume '
                             'has children)')
    parser.add_argument('--export-options', default=None,
                        help="Set the volume's own export options")
    parser.add_argument('--export-ranges', nargs='+', default=None,
                        metavar='RANGE',
                        help="Replace the volume's own export ranges")
    parser.add_argument('--clear-export', action='store_true', default=False,
                        help="Remove the volume's own export config so it "
                             'inherits the host/site default')
    parser.add_argument('--zfs-dataset', default=None,
                        help='ZFS dataset name (managed zfs volumes only)')


# ---------------------------------------------------------------------------
# `ng storage new/list/show/edit/delete host`
# ---------------------------------------------------------------------------


def _templates_lines(templates: dict[str, str]) -> str:
    return '\n'.join(f'{k}: {v}' for k, v in sorted(templates.items()))


def _host_to_dict(host: StorageHost, *, n_volumes: int | None = None,
                  effective=None) -> dict:
    d = {
        'hostname': host.hostname,
        'nfs_export': (
            host.nfs_export.model_dump() if host.nfs_export is not None
            else None
        ),
        'zfs_path_templates': dict(host.zfs_path_templates),
        'created_at': host.created_at,
        'updated_at': host.updated_at,
    }
    if n_volumes is not None:
        d['volumes'] = n_volumes
    if effective is not None:
        d['effective_nfs_export'] = _effective_to_dict(effective)
    return d


@site_args.apply(required=True)
@storage_defaults_args.apply(scope='host', clearable=False)
@commands.register('ng', 'storage', 'new', 'host',
                   help='Create a storage host record (a NAS / file server '
                        'at the site) with optional host-level defaults')
async def storage_new_host(args: Namespace):
    console = Console()
    try:
        host = await CreateStorageHost.run(
            args.db, args.author,
            site_name=args.site, hostname=args.hostname,
            export_options=args.export_options,
            export_ranges=args.export_ranges,
            zfs_path_templates=dict(args.zfs_path_template or []) or None,
        )
    except ValueError as e:
        console.print(f'[red]{e}[/]')
        return 1
    console.print(
        f'Created storage host [green]{host.hostname}[/] on {args.site}'
    )


@storage_new_host.args()
def _(parser: ArgParser):
    parser.add_argument('hostname')


@site_args.apply(required=True)
@yaml_args.apply()
@commands.register('ng', 'storage', 'list', 'hosts',
                   help='List storage hosts at a site with their defaults '
                        'and volume counts')
async def storage_list_hosts(args: Namespace):
    console = Console()
    site = await find_site_by_name(args.site)
    if site is None:
        console.print(f'[red]Site {args.site} not found[/]')
        return 1
    hosts = await list_site_storage_hosts(site)
    counts = await volume_counts_by_host(site)
    if args.yaml:
        print_yaml([
            _host_to_dict(h, n_volumes=counts.get(h.hostname, 0))
            for h in hosts
        ])
        return 0
    unlinked = sorted(set(counts) - {h.hostname for h in hosts})
    table = Table(title=f'Storage hosts on {args.site} (count={len(hosts)})')
    table.add_column('hostname', style='green', no_wrap=True)
    table.add_column('export_options')
    table.add_column('export_ranges', style='dim')
    table.add_column('zfs templates', style='cyan')
    table.add_column('volumes', justify='right')
    for h in hosts:
        export = h.nfs_export
        table.add_row(
            h.hostname,
            (export.export_options if export else None) or '—',
            ', '.join(export.export_ranges) if export and export.export_ranges
            else '—',
            _templates_lines(h.zfs_path_templates) or '—',
            str(counts.get(h.hostname, 0)),
        )
    console.print(table)
    if unlinked:
        console.print(
            f'[yellow]{len(unlinked)} host string(s) on volumes have no host '
            f'record:[/] {", ".join(unlinked)} — run '
            f'`storage backfill-hosts --site {args.site}`'
        )


@site_args.apply(required=True)
@yaml_args.apply()
@commands.register('ng', 'storage', 'show', 'host',
                   help='Show a storage host, its defaults, and the export '
                        'config volumes on it inherit')
async def storage_show_host(args: Namespace):
    console = Console()
    site = await find_site_by_name(args.site)
    if site is None:
        console.print(f'[red]Site {args.site} not found[/]')
        return 1
    host = await find_storage_host(site, args.hostname)
    if host is None:
        console.print(
            f'[red]Storage host {args.hostname} not found on {args.site}[/]'
        )
        return 1
    n_volumes = await StorageVolume.find(
        StorageVolume.site.id == site.id,
        StorageVolume.host == host.hostname,
    ).count()
    # What a volume on this host with no config of its own inherits.
    effective = effective_nfs_export(volume=None, host=host, site=site)
    if args.yaml:
        print_yaml(_host_to_dict(host, n_volumes=n_volumes, effective=effective))
        return 0
    t = _kv_table()
    t.add_row('hostname', host.hostname)
    t.add_row('site', site.name)
    export = host.nfs_export
    t.add_row('export_options',
              (export.export_options if export else None) or '[dim](none)[/]')
    t.add_row('export_ranges',
              ', '.join(export.export_ranges) if export and export.export_ranges
              else '[dim](none)[/]')
    t.add_row('zfs templates',
              _templates_lines(host.zfs_path_templates) or '[dim](none)[/]')
    t.add_row('effective export', _effective_export_lines(effective))
    t.add_row('volumes', str(n_volumes))
    t.add_row('created_at', str(host.created_at))
    t.add_row('updated_at', str(host.updated_at))
    console.print(Panel(
        t, title=f'[bold]Storage host:[/] [green]{host.hostname}[/]',
        border_style='cyan', expand=False,
    ))


@storage_show_host.args()
def _(parser: ArgParser):
    parser.add_argument('hostname')


@site_args.apply(required=True)
@storage_defaults_args.apply(scope='host')
@commands.register('ng', 'storage', 'edit', 'host',
                   help="Edit a storage host's defaults (export config, ZFS "
                        'path templates)')
async def storage_edit_host(args: Namespace):
    console = Console()
    if not has_storage_defaults_args(args):
        console.print('[red]Nothing to edit; pass at least one option[/]')
        return 1
    try:
        host = await EditStorageHost.run(
            args.db, args.author,
            site_name=args.site, hostname=args.hostname,
            **storage_defaults_kwargs(args),
        )
    except ValueError as e:
        console.print(f'[red]{e}[/]')
        return 1
    console.print(
        f'Edited storage host [green]{host.hostname}[/] on {args.site}'
    )


@storage_edit_host.args()
def _(parser: ArgParser):
    parser.add_argument('hostname')


@site_args.apply(required=True)
@commands.register('ng', 'storage', 'delete', 'host',
                   help='Delete a storage host record (refused while volumes '
                        'still reference it)')
async def storage_delete_host(args: Namespace):
    console = Console()
    if not args.force:
        try:
            answer = input(
                f'Delete storage host {args.hostname} on {args.site}? [y/N]: '
            ).strip().lower()
        except (EOFError, KeyboardInterrupt):
            console.print('\n[red]Aborted.[/]')
            return 1
        if answer != 'y':
            console.print('[red]Aborted.[/]')
            return 1
    try:
        await DeleteStorageHost.run(
            args.db, args.author,
            site_name=args.site, hostname=args.hostname,
        )
    except ValueError as e:
        console.print(f'[red]{e}[/]')
        return 1
    console.print(
        f'Deleted storage host [green]{args.hostname}[/] on {args.site}'
    )


@storage_delete_host.args()
def _(parser: ArgParser):
    parser.add_argument('hostname')
    parser.add_argument('--force', '-f', action='store_true', default=False,
                        help='Skip the confirmation prompt')


# ---------------------------------------------------------------------------
# `ng storage backfill-hosts`
# ---------------------------------------------------------------------------


@site_args.apply()
@commands.register('ng', 'storage', 'backfill-hosts',
                   help='One-time backfill: create a storage host per '
                        '(site, host) on existing volumes, link the volumes, '
                        'and optionally seed site export defaults and clear '
                        'redundant per-volume copies')
async def storage_backfill_hosts(args: Namespace):
    console = Console()
    try:
        result = await BackfillStorageHosts.run(
            args.db, args.author,
            skip_history=args.dry_run,
            site_name=args.site,
            seed_site_defaults=args.seed_site_defaults,
            dedupe_exports=args.dedupe_exports,
            dry_run=args.dry_run,
        )
    except ValueError as e:
        console.print(f'[red]{e}[/]')
        return 1
    prefix = '[yellow]DRY RUN[/] ' if args.dry_run else ''
    console.print(
        f'{prefix}Backfilled storage hosts'
        f'{f" on {args.site}" if args.site else ""}: '
        f'sites={result["sites_processed"]} '
        f'hosts_created={result["hosts_created"]} '
        f'volumes_linked={result["volumes_linked"]} '
        f'sites_seeded={result["sites_seeded"]} '
        f'sites_skipped_mixed={result["sites_skipped_mixed"]} '
        f'exports_deduped={result["exports_deduped"]}'
    )


@storage_backfill_hosts.args()
def _(parser: ArgParser):
    parser.add_argument('--seed-site-defaults', action='store_true',
                        default=False,
                        help="Set each site's export default from its "
                             'volumes when they all share one config')
    parser.add_argument('--dedupe-exports', action='store_true', default=False,
                        help='Clear per-volume export configs that equal the '
                             'host/site default (run after seeding)')
    parser.add_argument('--dry-run', action='store_true', default=False,
                        help='Report what would change without writing')
