from .base import BaseDocument, Expirable
from .site import Site
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
from .history import History
from .hippo import HippoEvent

# Rebuild models to resolve forward references (BackLink/Link with string refs)
User.model_rebuild()
SshKey.model_rebuild()
Group.model_rebuild()
AccessGroup.model_rebuild()
StatusGroup.model_rebuild()
UserSiteInfo.model_rebuild()
SlurmAccount.model_rebuild()
Storage.model_rebuild()

ALL_MODELS = [
    Site,
    User,
    SshKey,
    Group,
    AccessGroup,
    StatusGroup,
    UserSiteInfo,
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
