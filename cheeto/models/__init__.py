from .base import BaseDocument, Expirable
from .ldap_sync import LDAPInfo, LDAPSyncable
from .site import (
    Site,
    SiteGroupSettings,
    SiteSlurmSettings,
    SiteStorageSettings,
)
from .user import SshKey, UCDIAMInfo, User
from .group import AccessGroup, Group, StatusGroup
from .slurm import (
    SlurmAccount,
    SlurmAccountLimits,
    SlurmAllocation,
    SlurmAssociation,
    SlurmPartition,
    SlurmQOS,
    SlurmTRES,
)
from .storage import (
    AutomountMap,
    MountOverrides,
    NFSExportConfig,
    QuobyteConfig,
    StaticMount,
    Storage,
    StorageAllocation,
    StorageVolume,
    ZFSConfig,
)
from .user_site_info import UserSiteInfo
from .site_association import SiteAssociation
from .group_membership import GroupMembership
from .history import History
from .hippo import HippoEvent

# Rebuild models to resolve forward references (BackLink/Link with string
# refs). Order matters: the storage classes must be rebuilt (and present in
# this frame's globals) before the Site-side rebuilds resolve their string
# refs to them.
User.model_rebuild()
SshKey.model_rebuild()
Group.model_rebuild()
AccessGroup.model_rebuild()
StatusGroup.model_rebuild()
UserSiteInfo.model_rebuild()
GroupMembership.model_rebuild()
SlurmAccount.model_rebuild()
StorageVolume.model_rebuild()   # self-ref Link['StorageVolume']
Storage.model_rebuild()
# NOTE: the Site*Settings embedded models hold DocRef (bare ObjectId)
# references, not Links — links in embedded models silently degrade to
# inline snapshots (see models/base.py::DocRef) — so they need no rebuild.

# NOTE: SiteAssociation is the abstract base for GroupMembership; it has no
# collection of its own and is intentionally NOT in ALL_MODELS.
ALL_MODELS = [
    Site,
    User,
    SshKey,
    Group,
    AccessGroup,
    StatusGroup,
    UserSiteInfo,
    GroupMembership,
    SlurmAccount,
    SlurmAllocation,
    SlurmPartition,
    SlurmQOS,
    SlurmAssociation,
    AutomountMap,
    StorageVolume,
    StaticMount,
    Storage,
    History,
    HippoEvent,
]
