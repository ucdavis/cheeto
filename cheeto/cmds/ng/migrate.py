import secrets
from argparse import Namespace
from typing import Iterable

from beanie import Document
from ponderosa import ArgParser, arggroup

from .. import commands
from ...database import connect_mongoengine
from ...database.site import Site as OldSite
from ...log import Console
from ...models.group import AccessGroup, Group, StatusGroup
from ...models.site import Site
from ...models.slurm import (
    SlurmAccount,
    SlurmAllocation,
    SlurmAssociation,
    SlurmPartition,
    SlurmQOS,
)
from ...models.site import SiteStorageSettings
from ...models.storage import (
    AutomountMap,
    StaticMount,
    Storage,
    StorageVolume,
)
from ...models.user import SshKey, User
from ...models.user_site_info import UserSiteInfo
from ...operations import (
    MigrateAccessStatusGroups,
    MigrateAutomountMaps,
    MigrateGroups,
    MigrateSiteGlobals,
    MigrateSites,
    MigrateSlurmAccounts,
    MigrateSlurmAssociations,
    MigrateSlurmPartitions,
    MigrateSlurmQOSes,
    MigrateStorageVolumes,
    MigrateStorages,
    MigrateUser,
    MigrateUsers,
)
from ._args import user_args


@commands.register('ng', 'migrate',
                   help='Migrate data from old mongoengine database to new beanie models')
def migrate_cmd(args: Namespace):
    pass


@migrate_cmd.args(common=True)
def migrate_args(parser: ArgParser):
    parser.add_argument(
        '--drop', action='store_true', default=False,
        help='Permanently delete existing data in the target collections '
             'before importing. Requires typed confirmation.',
    )


@migrate_args.postprocessor(priority=70)
def connect_old_db(args: Namespace):
    connect_mongoengine(args.config.mongo, quiet=args.quiet)


@arggroup('sites filter')
def sites_filter_args(parser: ArgParser):
    parser.add_argument(
        '--sites', nargs='+', default=None, metavar='SITE',
        help='Restrict migration to these v1 sites (default: all). Use to '
             'exclude deprecated sites in the old database.',
    )


def _validate_sites_filter(
    console: Console, sitenames: list[str] | None,
) -> bool:
    """Typo guard: every --sites name must exist in the v1 database. Runs
    before any writes so a misspelled site aborts the whole run."""
    if not sitenames:
        return True
    known = {s.sitename for s in OldSite.objects()}
    unknown = sorted(set(sitenames) - known)
    if unknown:
        console.print(
            f'[red]Unknown v1 site(s): {", ".join(unknown)}[/]'
        )
        console.print(f'Known v1 sites: {", ".join(sorted(known))}')
        return False
    return True


def _confirm_drop(console: Console, scope_label: str,
                  collections: Iterable[str]) -> bool:
    """Prompt the user to type a generated 6-digit code before destructive drops.

    Returns True only when the user types the code exactly. False otherwise
    (mismatch, EOF, or KeyboardInterrupt).
    """
    code = f'{secrets.randbelow(1_000_000):06d}'
    console.print()
    console.rule('[bold red]DANGER: --drop will PERMANENTLY DELETE data[/]',
                 style='red')
    console.print(
        f'[bold red]Scope:[/] [yellow]{scope_label}[/]\n'
        f'[bold red]Collections to be wiped:[/]'
    )
    for name in collections:
        console.print(f'  [red]•[/] [yellow]{name}[/]')
    console.print(
        '\n[bold red]This action is IRREVERSIBLE.[/] '
        'All documents in the listed collections will be removed before '
        'the migration runs. There is no undo.\n'
    )
    console.print(
        f'To confirm, re-type this code exactly: [bold yellow]{code}[/]'
    )
    try:
        entered = input('Confirmation code: ').strip()
    except (EOFError, KeyboardInterrupt):
        console.print('\n[red]Aborted: no confirmation received.[/]')
        return False
    if entered != code:
        console.print('[red]Aborted: confirmation code did not match.[/]')
        return False
    console.print('[green]Confirmed. Proceeding with drop.[/]\n')
    return True


def _unique_collection_names(
    models: Iterable[type[Document]],
) -> list[str]:
    # Polymorphic subclasses share a collection, so dedupe by name.
    seen: set[str] = set()
    out: list[str] = []
    for m in models:
        n = m.Settings.name
        if n in seen:
            continue
        seen.add(n)
        out.append(n)
    return out


async def _drop_models(
    args: Namespace,
    console: Console,
    models: Iterable[type[Document]],
) -> None:
    models = list(models)
    name_to_model = {m.Settings.name: m for m in models}
    for name in _unique_collection_names(models):
        await name_to_model[name].get_pymongo_collection().drop()
        console.print(f'  [red]dropped collection[/] {name}')

    # Re-init so beanie recreates the collection-level indexes (unique
    # name/gid, etc.) on the next insert. Without this, the drop leaves
    # a fresh collection with no constraints, and silent dup-key data
    # corruption would be possible.
    from beanie import init_beanie

    from ...models import ALL_MODELS
    await init_beanie(
        database=args.db[args.config.mongo.database],
        document_models=ALL_MODELS,
    )


async def _maybe_drop(args: Namespace, console: Console,
                      scope_label: str,
                      models: list[type[Document]]) -> bool:
    """If --drop is set, confirm + drop the listed models' collections.
    Returns False to abort."""
    if not args.drop:
        return True
    if not _confirm_drop(
        console, scope_label, _unique_collection_names(models),
    ):
        return False
    await _drop_models(args, console, models)
    return True


@sites_filter_args.apply()
@commands.register('ng', 'migrate', 'sites',
                   help='Migrate all sites')
async def migrate_sites(args: Namespace):
    console = Console()
    if not _validate_sites_filter(console, args.sites):
        return 1
    if not await _maybe_drop(args, console, 'sites', [Site]):
        return 1
    sites = await MigrateSites.run(args.db, args.author, sitenames=args.sites)
    console.print(f'Migrated [green]{len(sites)}[/] sites')


@sites_filter_args.apply()
@commands.register('ng', 'migrate', 'site-globals',
                   help='Fold v1 Site.global_groups + global_slurmers into '
                        'v2 site.{group,slurm}.sticky (run AFTER groups + '
                        'slurm accounts have been migrated)')
async def migrate_site_globals(args: Namespace):
    console = Console()
    if not _validate_sites_filter(console, args.sites):
        return 1
    updated = await MigrateSiteGlobals.run(
        args.db, args.author, sitenames=args.sites,
    )
    console.print(f'Updated globals on [green]{updated}[/] sites')


@sites_filter_args.apply()
@user_args.apply(required=True)
@commands.register('ng', 'migrate', 'user',
                   help='Migrate a single user and their site memberships')
async def migrate_user(args: Namespace):
    console = Console()
    if not _validate_sites_filter(console, args.sites):
        return 1
    if args.drop:
        existing = await User.find_one(User.name == args.user)
        scope = f'user "{args.user}"'
        collections = ['users (this user only)',
                       'user_site_info (for this user)',
                       'ssh_keys (for this user)']
        if not _confirm_drop(console, scope, collections):
            return 1
        if existing is None:
            console.print(
                f'[yellow]No existing user named {args.user!r} to drop.[/]'
            )
        else:
            await SshKey.find(SshKey.user.id == existing.id).delete()
            await UserSiteInfo.find(
                UserSiteInfo.user.id == existing.id,
            ).delete()
            await existing.delete()
            console.print(f'  [red]dropped[/] user {args.user!r} '
                          f'(plus their site_info and ssh_keys)')

    user = await MigrateUser.run(
        args.db, args.author,
        username=args.user,
        sitenames=args.sites,
    )
    console.print(f'Migrated user [green]{user.name}[/] (uid={user.uid})')


USER_DROP_MODELS: list[type[Document]] = [User, UserSiteInfo, SshKey]


# AccessGroup and StatusGroup share the `groups` collection with Group
# (polymorphic via is_root=True). --drop on this step drops the entire
# `groups` collection, including any regular Group records. The
# confirmation prompt names `groups` so the operator can see this.
ACCESS_STATUS_DROP_MODELS: list[type[Document]] = [AccessGroup, StatusGroup]


@commands.register('ng', 'migrate', 'access-status-groups',
                   help='Migrate AccessGroup/StatusGroup records from v1 '
                        '(must run before "users")')
async def migrate_access_status_groups(args: Namespace):
    console = Console()
    if not await _maybe_drop(
        args, console, 'AccessGroup/StatusGroup records',
        ACCESS_STATUS_DROP_MODELS,
    ):
        return 1
    result = await MigrateAccessStatusGroups.run(args.db, args.author)
    a_created = sum(1 for v in result['access'].values() if v == 'created')
    s_created = sum(1 for v in result['status'].values() if v == 'created')
    a_skipped = len(result['access']) - a_created
    s_skipped = len(result['status']) - s_created
    console.print(
        f'AccessGroup: [green]{a_created}[/] created, '
        f'[dim]{a_skipped}[/] already existed'
    )
    console.print(
        f'StatusGroup: [green]{s_created}[/] created, '
        f'[dim]{s_skipped}[/] already existed'
    )


@sites_filter_args.apply()
@commands.register('ng', 'migrate', 'users',
                   help='Migrate all users and their site memberships')
async def migrate_users(args: Namespace):
    console = Console()
    if not _validate_sites_filter(console, args.sites):
        return 1
    if not await _maybe_drop(args, console, 'all users', USER_DROP_MODELS):
        return 1
    users = await MigrateUsers.run(args.db, args.author, sitenames=args.sites)
    console.print(f'Migrated [green]{len(users)}[/] users')


@sites_filter_args.apply()
@commands.register('ng', 'migrate', 'groups',
                   help='Migrate all groups with members, sponsors, and sudoers')
async def migrate_groups(args: Namespace):
    console = Console()
    if not _validate_sites_filter(console, args.sites):
        return 1
    if not await _maybe_drop(args, console, 'all groups', [Group]):
        return 1
    groups = await MigrateGroups.run(args.db, args.author, sitenames=args.sites)
    console.print(f'Migrated [green]{len(groups)}[/] groups')


@commands.register('ng', 'migrate', 'slurm',
                   help='Migrate Slurm partitions, QOSes, accounts, and associations')
def migrate_slurm_cmd(args: Namespace):
    pass


@sites_filter_args.apply()
@commands.register('ng', 'migrate', 'slurm', 'partitions',
                   help='Migrate all Slurm partitions')
async def migrate_slurm_partitions(args: Namespace):
    console = Console()
    if not _validate_sites_filter(console, args.sites):
        return 1
    if not await _maybe_drop(args, console, 'Slurm partitions',
                             [SlurmPartition]):
        return 1
    parts = await MigrateSlurmPartitions.run(
        args.db, args.author, sitenames=args.sites,
    )
    console.print(f'Migrated [green]{len(parts)}[/] Slurm partitions')


@sites_filter_args.apply()
@commands.register('ng', 'migrate', 'slurm', 'qoses',
                   help='Migrate all Slurm QOSes (and create allocations)')
async def migrate_slurm_qoses(args: Namespace):
    console = Console()
    if not _validate_sites_filter(console, args.sites):
        return 1
    if not await _maybe_drop(args, console, 'Slurm QOSes (and their allocations)',
                             [SlurmQOS, SlurmAllocation]):
        return 1
    qoses = await MigrateSlurmQOSes.run(
        args.db, args.author, sitenames=args.sites,
    )
    console.print(f'Migrated [green]{len(qoses)}[/] Slurm QOSes')


@sites_filter_args.apply()
@commands.register('ng', 'migrate', 'slurm', 'accounts',
                   help='Migrate Slurm accounts for SiteGroups with Slurm data')
async def migrate_slurm_accounts(args: Namespace):
    console = Console()
    if not _validate_sites_filter(console, args.sites):
        return 1
    if not await _maybe_drop(args, console, 'Slurm accounts', [SlurmAccount]):
        return 1
    accounts = await MigrateSlurmAccounts.run(
        args.db, args.author, sitenames=args.sites,
    )
    console.print(f'Migrated [green]{len(accounts)}[/] Slurm accounts')


@sites_filter_args.apply()
@commands.register('ng', 'migrate', 'slurm', 'associations',
                   help='Migrate all Slurm associations')
async def migrate_slurm_associations(args: Namespace):
    console = Console()
    if not _validate_sites_filter(console, args.sites):
        return 1
    if not await _maybe_drop(args, console, 'Slurm associations',
                             [SlurmAssociation]):
        return 1
    assocs = await MigrateSlurmAssociations.run(
        args.db, args.author, sitenames=args.sites,
    )
    console.print(f'Migrated [green]{len(assocs)}[/] Slurm associations')


SLURM_DROP_MODELS: list[type[Document]] = [
    SlurmAssociation, SlurmAccount, SlurmQOS, SlurmAllocation, SlurmPartition,
]


async def _run_slurm_migrations(args: Namespace, console: Console) -> None:
    console.rule('Migrating Slurm partitions')
    parts = await MigrateSlurmPartitions.run(
        args.db, args.author, sitenames=args.sites,
    )
    console.print(f'  [green]{len(parts)}[/] partitions migrated')

    console.rule('Migrating Slurm QOSes')
    qoses = await MigrateSlurmQOSes.run(
        args.db, args.author, sitenames=args.sites,
    )
    console.print(f'  [green]{len(qoses)}[/] QOSes migrated')

    console.rule('Migrating Slurm accounts')
    accounts = await MigrateSlurmAccounts.run(
        args.db, args.author, sitenames=args.sites,
    )
    console.print(f'  [green]{len(accounts)}[/] accounts migrated')

    console.rule('Migrating Slurm associations')
    assocs = await MigrateSlurmAssociations.run(
        args.db, args.author, sitenames=args.sites,
    )
    console.print(f'  [green]{len(assocs)}[/] associations migrated')


@sites_filter_args.apply()
@commands.register('ng', 'migrate', 'slurm', 'all',
                   help='Migrate all Slurm records (partitions, qoses, accounts, associations)')
async def migrate_slurm_all(args: Namespace):
    console = Console()
    if not _validate_sites_filter(console, args.sites):
        return 1
    if not await _maybe_drop(args, console, 'all Slurm records',
                             SLURM_DROP_MODELS):
        return 1
    await _run_slurm_migrations(args, console)
    console.rule('Slurm migration complete')


STORAGE_DROP_MODELS: list[type[Document]] = [
    Storage, StaticMount, StorageVolume, AutomountMap,
]


async def _reset_site_storage_settings(console: Console) -> None:
    """Null Site.storage settings after dropping the storage collections —
    the embedded default_home_volume / mount links would otherwise dangle."""
    async for site in Site.find_all():
        if (
            site.storage.default_home_volume is not None
            or site.storage.home_automount_map is not None
            or site.storage.home_static_mount is not None
            or site.storage.default_home_quota is not None
        ):
            site.storage = SiteStorageSettings()
            await site.save()
            console.print(
                f'  [yellow]reset storage settings on site[/] {site.name}'
            )


async def _run_storage_migrations(args: Namespace, console: Console) -> None:
    console.rule('Migrating automount maps')
    maps = await MigrateAutomountMaps.run(
        args.db, args.author, sitenames=args.sites,
    )
    console.print(f'  [green]{maps}[/] automount maps migrated')

    console.rule('Migrating storage volumes')
    vols = await MigrateStorageVolumes.run(
        args.db, args.author, sitenames=args.sites,
    )
    console.print(f'  [green]{vols}[/] storage volumes migrated')

    console.rule('Migrating storages')
    storages = await MigrateStorages.run(
        args.db, args.author, sitenames=args.sites,
    )
    console.print(f'  [green]{storages}[/] storages migrated')


@commands.register('ng', 'migrate', 'storage',
                   help='Migrate automount maps, storage volumes, and storages')
def migrate_storage_cmd(args: Namespace):
    pass


@sites_filter_args.apply()
@commands.register('ng', 'migrate', 'storage', 'maps',
                   help='Migrate all automount maps')
async def migrate_storage_maps(args: Namespace):
    console = Console()
    if not _validate_sites_filter(console, args.sites):
        return 1
    if not await _maybe_drop(args, console, 'automount maps', [AutomountMap]):
        return 1
    maps = await MigrateAutomountMaps.run(
        args.db, args.author, sitenames=args.sites,
    )
    console.print(f'Migrated [green]{maps}[/] automount maps')


@sites_filter_args.apply()
@commands.register('ng', 'migrate', 'storage', 'volumes',
                   help='Migrate storage volumes (collection roots, ZFS '
                        'datasets, bare NFS exports)')
async def migrate_storage_volumes(args: Namespace):
    console = Console()
    if not _validate_sites_filter(console, args.sites):
        return 1
    if not await _maybe_drop(args, console, 'storage volumes',
                             [StorageVolume]):
        return 1
    if args.drop:
        await _reset_site_storage_settings(console)
    vols = await MigrateStorageVolumes.run(
        args.db, args.author, sitenames=args.sites,
    )
    console.print(f'Migrated [green]{vols}[/] storage volumes')


@sites_filter_args.apply()
@commands.register('ng', 'migrate', 'storage', 'storages',
                   help='Migrate storage records (run AFTER maps + volumes)')
async def migrate_storage_storages(args: Namespace):
    console = Console()
    if not _validate_sites_filter(console, args.sites):
        return 1
    if not await _maybe_drop(args, console, 'storages', [Storage]):
        return 1
    storages = await MigrateStorages.run(
        args.db, args.author, sitenames=args.sites,
    )
    console.print(f'Migrated [green]{storages}[/] storages')


@sites_filter_args.apply()
@commands.register('ng', 'migrate', 'storage', 'all',
                   help='Migrate all storage records (maps, volumes, storages)')
async def migrate_storage_all(args: Namespace):
    console = Console()
    if not _validate_sites_filter(console, args.sites):
        return 1
    if not await _maybe_drop(args, console, 'all storage records',
                             STORAGE_DROP_MODELS):
        return 1
    if args.drop:
        await _reset_site_storage_settings(console)
    await _run_storage_migrations(args, console)
    console.rule('Storage migration complete')


ALL_DROP_MODELS: list[type[Document]] = [
    SlurmAssociation, SlurmAccount, SlurmQOS, SlurmAllocation, SlurmPartition,
    Storage, StaticMount, StorageVolume, AutomountMap,
    Group, SshKey, UserSiteInfo, User, Site, AccessGroup, StatusGroup,
]


@sites_filter_args.apply()
@commands.register('ng', 'migrate', 'all',
                   help='Migrate sites, access/status groups, users, regular '
                        'groups, Slurm, and storage records in order')
async def migrate_all(args: Namespace):
    console = Console()
    if not _validate_sites_filter(console, args.sites):
        return 1
    if args.sites:
        console.print(
            f'[yellow]Migration restricted to site(s): '
            f'{", ".join(sorted(args.sites))}[/]'
        )
    if not await _maybe_drop(args, console,
                             'EVERYTHING the migration writes',
                             ALL_DROP_MODELS):
        return 1

    console.rule('Migrating sites')
    sites = await MigrateSites.run(args.db, args.author, sitenames=args.sites)
    console.print(f'  [green]{len(sites)}[/] sites migrated')

    # AccessGroup / StatusGroup must exist before MigrateUsers runs — the
    # new User schema has Link[AccessGroup] and Link[StatusGroup] fields
    # that resolve via find_one(access_name=...) at migration time.
    console.rule('Migrating AccessGroup/StatusGroup records')
    seed_result = await MigrateAccessStatusGroups.run(args.db, args.author)
    console.print(
        f'  [green]{sum(1 for v in seed_result["access"].values() if v == "created")}[/] '
        f'AccessGroup records created, '
        f'[green]{sum(1 for v in seed_result["status"].values() if v == "created")}[/] '
        f'StatusGroup records created'
    )

    console.rule('Migrating users')
    users = await MigrateUsers.run(args.db, args.author, sitenames=args.sites)
    console.print(f'  [green]{len(users)}[/] users migrated')

    console.rule('Migrating groups')
    groups = await MigrateGroups.run(args.db, args.author, sitenames=args.sites)
    console.print(f'  [green]{len(groups)}[/] groups migrated')

    await _run_slurm_migrations(args, console)

    # Storage runs after users/groups (Storage.owner/group links) and
    # before the site-globals fold so the two Site-document writers stay
    # adjacent and sequential.
    await _run_storage_migrations(args, console)

    # Site globals (`global_groups` / `global_slurmers`) fold last —
    # they reference v2 Groups + SlurmAccounts, so both must already
    # exist.
    console.rule('Migrating Site globals (sticky groups + slurmers)')
    await MigrateSiteGlobals.run(args.db, args.author, sitenames=args.sites)

    console.rule('Migration complete')
