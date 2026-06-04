from .base import BaseDocument, Expirable
from .site import Site, SiteGroupSettings, SiteSlurmSettings
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
    Storage,
    StorageAllocation,
)
from .user_site_info import UserSiteInfo
from .site_association import SiteAssociation
from .group_membership import GroupMembership
from .history import History
from .hippo import HippoEvent

# Rebuild models to resolve forward references (BackLink/Link with string refs)
User.model_rebuild()
SshKey.model_rebuild()
Group.model_rebuild()
AccessGroup.model_rebuild()
StatusGroup.model_rebuild()
UserSiteInfo.model_rebuild()
GroupMembership.model_rebuild()
SlurmAccount.model_rebuild()
Storage.model_rebuild()
SiteSlurmSettings.model_rebuild()
SiteGroupSettings.model_rebuild()
Site.model_rebuild()

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
    Storage,
    History,
    HippoEvent,
]
