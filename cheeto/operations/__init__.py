from .base import Operation
from .site import (
    AddSiteAlias,
    AddStickyGroup,
    AddStickySlurmAccount,
    ClearSiteDefaultSlurmAccount,
    CreateSite,
    ExportRootSSHKeys,
    ExportSympaEmails,
    RemoveSite,
    RemoveSiteAlias,
    RemoveStickyGroup,
    RemoveStickySlurmAccount,
    SetSiteDefaultSlurmAccount,
    SetSiteStorageDefaults,
)
from .user import (
    AddUserAccess,
    AddUserComment,
    AddUserSshKey,
    ClearOffboardingSiteStatuses,
    ClearRedundantSiteStatuses,
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
    clear_user_site_statuses,
)
from .user_site import AddSiteUser, RemoveSiteUser
from .group import (
    CreateClassGroup,
    CreateGroup,
    CreateGroupFromSponsor,
    CreateLabGroup,
    CreateSystemGroup,
    SeedAccessStatusGroups,
)
from .group_membership import (
    AddGroupMember,
    AddGroupSlurmer,
    AddGroupSponsor,
    AddGroupSudoer,
    RemoveGroupMember,
    RemoveGroupSlurmer,
    RemoveGroupSponsor,
    RemoveGroupSudoer,
)
from .slurm import (
    AddQOSAllocation,
    CreateSlurmAssociation,
    CreateSlurmPartition,
    CreateSlurmQOS,
    EditSlurmAllocation,
    EditSlurmQOS,
    ProvisionSlurmAllocation,
    RemoveSlurmAssociation,
    RemoveSlurmPartition,
    RemoveSlurmQOS,
    SyncSlurm,
)
from .iam import (
    ReapOffboardedUsers,
    SyncAllUsersIAM,
    SyncUserIAM,
    SyncUserIAMResult,
)
from .ldap import (
    BackfillLDAPInfo,
    BootstrapLDAPSite,
    ClearLDAPTree,
    LDAPSyncResult,
    PruneSiteLDAP,
    SyncGroupToLDAP,
    SyncSiteAutomounts,
    SyncSiteLDAP,
    SyncUserToLDAP,
)
from .puppet import SyncOldPuppet
from .storage import (
    CreateAutomountMap,
    CreateHomeStorage,
    CreateStaticMount,
    CreateStorageVolume,
    ExportPuppetStorage,
    SetStorageMount,
    SetVolumeStorageMounts,
)

# The v1 -> v2 migration ops live in cheeto/legacy/migrate.py, behind the
# optional `legacy` extra. Expose them lazily through this package so
# `from cheeto.operations import MigrateX` keeps working, while plain
# `import cheeto.operations` stays mongoengine-free (the import only happens
# on first access to a Migrate* name, where require_legacy() also fires).
_LEGACY_MIGRATE_OPS = frozenset({
    'MigrateAccessStatusGroups', 'MigrateAutomountMaps', 'MigrateGroups',
    'MigrateSiteGlobals', 'MigrateSites', 'MigrateSlurmAccounts',
    'MigrateSlurmAssociations', 'MigrateSlurmPartitions', 'MigrateSlurmQOSes',
    'MigrateStorageVolumes', 'MigrateStorages', 'MigrateUser', 'MigrateUsers',
})


def __getattr__(name):
    if name in _LEGACY_MIGRATE_OPS:
        from ..legacy import migrate
        return getattr(migrate, name)
    raise AttributeError(f'module {__name__!r} has no attribute {name!r}')
