from .base import OPERATIONS, Operation, get_operation, operation_names
from .email import SendUserEmail
from .site import (
    AddSiteAlias,
    AddStickyGroup,
    AddStickySlurmAccount,
    ClearSiteDefaultSlurmAccount,
    CreateSite,
    ExportRootSSHKeys,
    ExportSympaEmails,
    DeleteSite,
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
    ClearUserExpiry,
    CreateClassUser,
    CreateClassUsers,
    CreateSharedUser,
    CreateSystemUser,
    CreateUser,
    DeleteUser,
    RemoveUserAccess,
    RemoveUserSshKey,
    SetUserPassword,
    SetUserShell,
    SetUserStatus,
    SetUserType,
    clear_user_site_statuses,
)
from .user_site import (
    AddSiteUser,
    BackfillUserSiteInfo,
    RemoveSiteUser,
    ensure_user_site,
)
from .group import (
    CreateClassGroup,
    CreateGroup,
    CreateGroupFromSponsor,
    CreateLabGroup,
    CreateSystemGroup,
    DeleteGroup,
    SeedAccessStatusGroups,
    SeedTypeGroups,
)
from .group_site import (
    AddSiteGroup,
    BackfillGroupSiteInfo,
    RemoveSiteGroup,
    ensure_group_site,
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
    ensure_group_membership,
)
from .slurm import (
    AddQOSAllocation,
    CreateSlurmAccount,
    CreateSlurmAssociation,
    CreateSlurmPartition,
    CreateSlurmQOS,
    EditSlurmAccount,
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
    create_user_from_iam,
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
    AddVolumeAllocation,
    CreateAutomountMap,
    CreateGroupStorage,
    CreateHomeStorage,
    CreateStaticMount,
    CreateStorageVolume,
    EditStorageVolume,
    EditVolumeAllocation,
    ExportPuppetStorage,
    RehomeUser,
    RemoveVolumeAllocation,
    SetStorageMount,
    SetVolumeStorageMounts,
)
from .storage_host import (
    BackfillStorageHosts,
    CreateStorageHost,
    DeleteStorageHost,
    EditStorageHost,
)
