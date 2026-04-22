from argparse import Namespace

from ponderosa import ArgParser

from .. import commands
from ...database import connect_mongoengine
from ...log import Console
from ...operations import (
    MigrateGroups,
    MigrateSites,
    MigrateSlurmAccounts,
    MigrateSlurmAssociations,
    MigrateSlurmPartitions,
    MigrateSlurmQOSes,
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
    pass


@migrate_args.postprocessor(priority=70)
def connect_old_db(args: Namespace):
    connect_mongoengine(args.config.mongo, quiet=args.quiet)


@commands.register('ng', 'migrate', 'sites',
                   help='Migrate all sites')
async def migrate_sites(args: Namespace):
    console = Console()
    sites = await MigrateSites.run(args.db, args.author)
    console.print(f'Migrated [green]{len(sites)}[/] sites')


@user_args.apply(required=True)
@commands.register('ng', 'migrate', 'user',
                   help='Migrate a single user and their site memberships')
async def migrate_user(args: Namespace):
    console = Console()
    user = await MigrateUser.run(
        args.db, args.author,
        username=args.user,
    )
    console.print(f'Migrated user [green]{user.name}[/] (uid={user.uid})')


@commands.register('ng', 'migrate', 'users',
                   help='Migrate all users and their site memberships')
async def migrate_users(args: Namespace):
    console = Console()
    users = await MigrateUsers.run(args.db, args.author)
    console.print(f'Migrated [green]{len(users)}[/] users')


@commands.register('ng', 'migrate', 'groups',
                   help='Migrate all groups with members, sponsors, and sudoers')
async def migrate_groups(args: Namespace):
    console = Console()
    groups = await MigrateGroups.run(args.db, args.author)
    console.print(f'Migrated [green]{len(groups)}[/] groups')


@commands.register('ng', 'migrate', 'slurm',
                   help='Migrate Slurm partitions, QOSes, accounts, and associations')
def migrate_slurm_cmd(args: Namespace):
    pass


@commands.register('ng', 'migrate', 'slurm', 'partitions',
                   help='Migrate all Slurm partitions')
async def migrate_slurm_partitions(args: Namespace):
    console = Console()
    parts = await MigrateSlurmPartitions.run(args.db, args.author)
    console.print(f'Migrated [green]{len(parts)}[/] Slurm partitions')


@commands.register('ng', 'migrate', 'slurm', 'qoses',
                   help='Migrate all Slurm QOSes (and create allocations)')
async def migrate_slurm_qoses(args: Namespace):
    console = Console()
    qoses = await MigrateSlurmQOSes.run(args.db, args.author)
    console.print(f'Migrated [green]{len(qoses)}[/] Slurm QOSes')


@commands.register('ng', 'migrate', 'slurm', 'accounts',
                   help='Migrate Slurm accounts for SiteGroups with Slurm data')
async def migrate_slurm_accounts(args: Namespace):
    console = Console()
    accounts = await MigrateSlurmAccounts.run(args.db, args.author)
    console.print(f'Migrated [green]{len(accounts)}[/] Slurm accounts')


@commands.register('ng', 'migrate', 'slurm', 'associations',
                   help='Migrate all Slurm associations')
async def migrate_slurm_associations(args: Namespace):
    console = Console()
    assocs = await MigrateSlurmAssociations.run(args.db, args.author)
    console.print(f'Migrated [green]{len(assocs)}[/] Slurm associations')


async def _run_slurm_migrations(args: Namespace, console: Console) -> None:
    console.rule('Migrating Slurm partitions')
    parts = await MigrateSlurmPartitions.run(args.db, args.author)
    console.print(f'  [green]{len(parts)}[/] partitions migrated')

    console.rule('Migrating Slurm QOSes')
    qoses = await MigrateSlurmQOSes.run(args.db, args.author)
    console.print(f'  [green]{len(qoses)}[/] QOSes migrated')

    console.rule('Migrating Slurm accounts')
    accounts = await MigrateSlurmAccounts.run(args.db, args.author)
    console.print(f'  [green]{len(accounts)}[/] accounts migrated')

    console.rule('Migrating Slurm associations')
    assocs = await MigrateSlurmAssociations.run(args.db, args.author)
    console.print(f'  [green]{len(assocs)}[/] associations migrated')


@commands.register('ng', 'migrate', 'slurm', 'all',
                   help='Migrate all Slurm records (partitions, qoses, accounts, associations)')
async def migrate_slurm_all(args: Namespace):
    console = Console()
    await _run_slurm_migrations(args, console)
    console.rule('Slurm migration complete')


@commands.register('ng', 'migrate', 'all',
                   help='Migrate sites, users, groups, and all Slurm records in order')
async def migrate_all(args: Namespace):
    console = Console()

    console.rule('Migrating sites')
    sites = await MigrateSites.run(args.db, args.author)
    console.print(f'  [green]{len(sites)}[/] sites migrated')

    console.rule('Migrating users')
    users = await MigrateUsers.run(args.db, args.author)
    console.print(f'  [green]{len(users)}[/] users migrated')

    console.rule('Migrating groups')
    groups = await MigrateGroups.run(args.db, args.author)
    console.print(f'  [green]{len(groups)}[/] groups migrated')

    await _run_slurm_migrations(args, console)

    console.rule('Migration complete')
