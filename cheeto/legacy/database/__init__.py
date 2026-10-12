#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) The Regents of the University of California, Davis
# License: Modified BSD
#
# v1 (mongoengine) models, retained only for the v1 -> v2 migration. Importing
# this package requires the optional `legacy` extra (mongoengine); the guard
# below turns a missing dependency into an actionable message.

# PEP 810 (Python 3.15+): keep this package's imports eager, submodules
# included. mongoengine resolves string references such as
# ReferenceField('SiteGroup') from its document registry at class creation,
# so site.py's import of SiteGroup must execute, not defer. See cheeto/__init__.py.
__eager_imports__ = True

from .. import require_legacy  # noqa: E402

require_legacy()

from .base import (  # noqa: E402
    connect_mongoengine,
    SyncQuerySet,
    BaseDocument,
    handler,
)
from .site import Site  # noqa: E402
from .hippo import HippoEvent  # noqa: E402
from .user import (  # noqa: E402
    DuplicateUser,
    DuplicateGlobalUser,
    DuplicateSiteUser,
    NonExistentGlobalUser,
    NonExistentSiteUser,
    GlobalUser,
    InvalidUser,
    SiteUser,
    UserSearch,
    User,
    global_user_t,
    site_user_t,
)
from .group import (  # noqa: E402
    GlobalGroup,
    SiteGroup,
    SiteSlurmAccount,
    global_group_t,
    site_group_t,
    DuplicateGroup,
    DuplicateGlobalGroup,
    DuplicateSiteGroup,
    NonExistentGroup,
    NonExistentGlobalGroup,
    NonExistentSiteGroup,
)
from .slurm import (  # noqa: E402
    SiteSlurmAssociation,
    SiteSlurmPartition,
    SiteSlurmQOS,
    SlurmTRES,
)
from .storage import (  # noqa: E402
    Automount,
    AutomountMap,
    Storage,
    StorageMount,
    StorageMountSource,
    NFSMountSource,
    ZFSMountSource,
    NFSSourceCollection,
    ZFSSourceCollection,
    NonExistentStorage,
)
