from .base import BaseDocument
from .site import Site
from .user import SshKey, UCDIAMInfo, User
from .group import Group
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

# Rebuild models to resolve forward references (BackLink/Link with string refs)
User.model_rebuild()
Group.model_rebuild()
SlurmAccount.model_rebuild()
Storage.model_rebuild()

ALL_MODELS = [
    Site,
    User,
    Group,
    UserSiteInfo,
    SlurmAccount,
    SlurmPartition,
    SlurmQOS,
    SlurmAssociation,
    AutomountMap,
    Storage,
    History,
]
