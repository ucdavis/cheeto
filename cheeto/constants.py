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

QOS_TRES_REGEX = (
    r'^(?=.*\bcpus=(?P<cpus>-1|inf|\d+)\b)?'
    r'(?=.*\bgpus=(?P<gpus>-1|inf|\d+)\b)?'
    r'(?=.*\bmem=(?P<mem>-1|inf|(?:\d+(?:\.\d+)?|\.\d+)[MmGgTtPp])\b)?'
    r'.*$'
)

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
    'class'
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

STORAGE_TYPES : Final = (
    'zfs',
    'quobyte',
)

STORAGE_CATEGORIES : Final = (
    'home',
    'group',
    'share',
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

MOUNT_OPTS : Final = (
    # General Mount Options
    "async",          # All I/O to the file system should be done asynchronously
    "atime",          # Update inode access times for each access
    "noatime",        # Do not update inode access times
    "auto",           # Can be mounted with the -a option
    "noauto",         # Can only be mounted explicitly
    "defaults",       # Use default options: rw, suid, dev, exec, auto, nouser, and async
    "dev",            # Interpret character or block special devices on the file system
    "nodev",          # Do not interpret character or block special devices
    "diratime",       # Update directory inode access times
    "nodiratime",     # Do not update directory inode access times
    "dirsync",        # All directory updates within the file system should be done synchronously
    "exec",           # Permit execution of binaries
    "noexec",         # Do not permit execution of binaries
    "group",          # Allow an ordinary user to mount the file system if one of their groups matches the file system's group
    "iversion",       # Increment inode version when the inode is modified
    "noiversion",     # Do not increment inode version when the inode is modified
    "mand",           # Allow mandatory locking
    "nomand",         # Do not allow mandatory locking
    "noacl",          # Disable Access Control Lists
    "acl",            # Enable Access Control Lists
    "nouser",         # Only root can mount
    "user",           # Allow an ordinary user to mount the file system
    "owner",          # Allow the file system to be mounted by its owner
    "remount",        # Attempt to remount an already-mounted file system
    "ro",             # Mount the file system read-only
    "rw",             # Mount the file system read-write
    "suid",           # Allow set-user-identifier or set-group-identifier bits to take effect
    "nosuid",         # Ignore set-user-identifier or set-group-identifier bits
    "sync",           # All I/O to the file system should be done synchronously
    "user_xattr",     # Enable user-specified extended attributes
    "nouser_xattr",   # Disable user-specified extended attributes
    "relatime",       # Update inode access times relative to modify/change time
    "norelatime",     # Do not use relative atime
    "strictatime",    # Always update the last access time
    "nostrictatime",  # Update the last access time relative to modify/change time
    "lazytime",       # Lazy inode time updates
    "nolazytime",     # Do not use lazy inode time updates
    "discard",        # Issue discard requests to the device when blocks are freed
    "nodiscard",      # Do not issue discard requests
    "errors", 
    "quota",          # Enable disk quotas
    "noquota",        # Disable disk quotas
    "usrquota",       # Enable user disk quotas
    "grpquota",       # Enable group disk quotas
    "context",        # Set context for entire file system
    "fscontext",      # Set context for entire file system
    "defcontext",     # Set default context for unlabeled files
    "rootcontext",    # Set context for the root inode
    "prjquota",       # Enable project disk quotas
    "xattr",          # Enable extended attributes
    "noxattr",        # Disable extended attributes

    # NFS-specific Mount Options (duplicates removed)
    "bg",             # Background mount if the first attempt fails
    "fg",             # Foreground mount (default)
    "soft",           # Soft mount, retry after an error
    "hard",           # Hard mount, continue retrying indefinitely
    "intr",           # Allow interrupts on hard mounts
    "nointr",         # Do not allow interrupts on hard mounts
    "rsize",          # Read buffer size (in bytes)
    "wsize",          # Write buffer size (in bytes)
    "timeo",          # Set NFS timeout (in tenths of a second)
    "retrans",        # Number of NFS retransmissions
    "sec",            # Security flavor (e.g., sec=sys, sec=krb5)
    "vers",           # NFS protocol version (e.g., vers=3, vers=4)
    "proto",          # Transport protocol (e.g., proto=tcp, proto=udp)
    "port",           # NFS server port
    "mountport",      # NFS mount daemon port
    "mountproto",     # Mount protocol (e.g., tcp, udp)
    "lock",           # Enable file locking
    "nolock",         # Disable file locking
    "lookupcache",    # Control lookup caching (e.g., all, none, pos)
    "nocto",          # No close-to-open consistency checking
    "actimeo",        # Attribute cache timeout
    "retry",          # Number of retries for mount
    "tcp",            # Use TCP protocol
    "udp",            # Use UDP protocol
    "fsc",            # Enable FS-Cache
    "nofsc",          # Disable FS-Cache
    "local_lock",     # Enable local locking
    "noresvport",     # Do not use a reserved port
    "resvport",       # Use a reserved port
    "minorversion",   # NFS minor version
    "namlen",         # Maximum filename length
    "clientaddr",     # Set client address
    "mountaddr",      # Set mount server address
    "nconnect",       # Number of connections to the server
    "maxcache",       # Maximum cache size
    "rdma",           # Use RDMA transport
    "fstype",
    "ac",
    "noac"
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