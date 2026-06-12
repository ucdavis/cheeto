from .base import Operation
from .site import (
    AddStickyGroup,
    AddStickySlurmAccount,
    ClearSiteDefaultSlurmAccount,
    CreateSite,
    ExportRootSSHKeys,
    ExportSympaEmails,
    RemoveSite,
    RemoveStickyGroup,
    RemoveStickySlurmAccount,
    SetSiteDefaultSlurmAccount,
    SetSiteStorageDefaults,
)
from .user import (
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
from .storage import (
    CreateHomeStorage,
    CreateStaticMount,
    CreateStorageVolume,
    ExportPuppetStorage,
)
from .migrate import (
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
