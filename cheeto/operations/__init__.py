from .base import Operation
from .site import CreateSite
from .user import (
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
from .user_site import AddSiteUser, RemoveSiteUser
from .group import (
    CreateClassGroup,
    CreateGroup,
    CreateGroupFromSponsor,
    CreateLabGroup,
    CreateSystemGroup,
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
)
from .storage import CreateHomeStorage
from .migrate import (
    MigrateGroups,
    MigrateSites,
    MigrateSlurmAccounts,
    MigrateSlurmAssociations,
    MigrateSlurmPartitions,
    MigrateSlurmQOSes,
    MigrateUser,
    MigrateUsers,
)
