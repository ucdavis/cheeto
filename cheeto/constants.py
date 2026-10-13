#!/usr/bin/env python
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2026
# (c) The Regents of the University of California, Davis, 2026
# File   : constants.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 12.03.2026

from typing import Final

MIN_PIGROUP_GID = 100_000_000

MIN_SHARED_UID = 2_000_000_000

UINT_MAX = 4_294_967_296

MIN_CLASS_ID = 3_000_000_000

MIN_LABGROUP_ID = 3_900_000_000
MAX_LABGROUP_ID = 3_910_000_000

MIN_SYSTEM_UID  = 4_000_000_000
MIN_SPECIAL_GID = 3_333_333_000

DATA_QUOTA_REGEX = r'[+-]?([0-9]*[.])?[0-9]+[MmGgTtPp]'

DEFAULT_SHELL = '/usr/bin/bash'

ENABLED_SHELLS : Final = (
    "/bin/sh",
    "/bin/bash",
    "/bin/zsh",
    "/usr/bin/sh",
    "/usr/bin/zsh",
    "/usr/bin/bash"
)

DISABLED_SHELLS : Final = (
    "/usr/sbin/nologin-account-disabled",
    "/bin/false",
    "/usr/sbin/nologin"
)

SHELLS : Final = ENABLED_SHELLS + DISABLED_SHELLS

USER_TYPES : Final = (
    'user',
    'admin',
    'system',
    'class',
    'shared'
)

GROUP_TYPES : Final = (
    'user',
    'access',
    'status',
    'system',
    'group',
    'admin',
    'class',
    'usertype'
)

USER_STATUSES : Final = (
    'active',
    'inactive',
    'disabled',
    'offboarding'
)

ACCESS_TYPES : Final = (
    'login-ssh',
    'ondemand',
    'compute-ssh',
    'root-ssh',
    'sudo',
    'slurm'
)


EMAIL_REGEX = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'

STORAGE_BACKENDS : Final = (
    'zfs',
    'quobyte',
)

STORAGE_CATEGORIES : Final = (
    'home',
    'group',
    'share',
)

MOUNT_FSTYPES : Final = (
    'nfs',
    'nfs4',
    'cvmfs',
)

SLURM_QOS_VALID_FLAGS : Final = (
    'DenyOnLimit',
    'EnforceUsageThreshold',
    'NoDecay',
    'NoReserve',
    'OverPartQOS',
    'PartitionMaxNodes',
    'PartitionMinNodes',
    'PartitionTimeLimit',
    'RequiresReservation',
    'UsageFactorSafe',
)

HIPPO_EVENT_ACTIONS : Final = (
    'CreateAccount',
    'AddAccountToGroup',
    'UpdateSshKey',
    'RemoveAccountFromGroup',
    'CreateGroup'
    )


HIPPO_EVENT_STATUSES : Final = (
    'Pending',
    'Complete',
    'Failed',
    'Canceled'
)

IAM_USER_TYPES : Final = (
    'employee', # isEmployee or isHSEmployee
    'faculty',
    'staff',
    'student',
    'external'
)

# User.type values that may have IAM entries. system/class/shared accounts
# are administrative/role users and never resolve to an IAM person.
IAM_SYNCABLE_USER_TYPES : Final = (
    'user',
    'admin',
)

# Authoritative current state of an IAM record on a User. 'present' = IAM
# returned a person record on the last definitive sync. 'missing' = IAM
# returned 200-empty/404 — start the offboarding grace clock.
IAM_STATUSES : Final = (
    'present',
    'missing',
)