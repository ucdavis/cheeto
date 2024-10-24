#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023-2024
# (c) The Regents of the University of California, Davis, 2023
# File   : types.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 22.03.2023

from collections import OrderedDict, namedtuple
from collections.abc import Hashable, Iterable, Sequence
import dataclasses
import datetime
import logging
from pathlib import Path
from typing import Annotated, ClassVar, Type, Union, List, Self, Generator, Optional
import sys

from marshmallow import validate as mv
from marshmallow import fields as mf
from marshmallow import post_dump, Schema as _Schema
from marshmallow_dataclass import dataclass
import marshmallow_dataclass
import marshmallow_dataclass.collection_field
from marshmallow_dataclass.union_field import Union as mdUnion

from . import yaml
from .errors import ExitCode
from .yaml import parse_yaml, puppet_merge


UINT_MAX = 4_294_967_296

MIN_CLASS_ID = 3_000_000_000

MIN_LABGROUP_ID = 3_900_000_000
MAX_LABGROUP_ID = 3_910_000_000

DATA_QUOTA_REGEX = r'[+-]?([0-9]*[.])?[0-9]+[MmGgTtPp]'

DEFAULT_SHELL = '/usr/bin/bash'

ENABLED_SHELLS = {
    "/bin/sh",
    "/bin/bash",
    "/bin/zsh",
    "/usr/bin/sh",
    "/usr/bin/zsh",
    "/usr/bin/bash"
}

DISABLED_SHELLS = {
    "/usr/sbin/nologin-account-disabled",
    "/bin/false",
    "/usr/sbin/nologin"
}

USER_TYPES = {
    'user',
    'admin',
    'system',
    'class'
}

GROUP_TYPES = {
    'user',
    'access',
    'system',
    'group',
    'admin',
    'class'
}

USER_STATUSES = {
    'active',
    'inactive',
    'disabled'
}

ACCESS_TYPES = {
    'login-ssh',
    'ondemand',
    'compute-ssh',
    'root-ssh',
    'sudo',
    'slurm'
}


HIPPO_EVENT_ACTIONS = {
    'CreateAccount',
    'AddAccountToGroup',
    'UpdateSshKey'
}


HIPPO_EVENT_STATUSES = {
    'Pending',
    'Complete',
    'Failed',
    'Canceled'
}

MOUNT_OPTS = {
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
}


def is_listlike(obj):
    return isinstance(obj, (Sequence, set)) and not isinstance(obj, (str, bytes, bytearray))


class _BaseModel:

    SKIP_VALUES = [None, {}, []]
    Schema: ClassVar[Type[_Schema]] = _Schema # For the type checker

    class Meta:
        ordered = True
        render_module = yaml

    def items(self):
        return dataclasses.asdict(self).items() #type: ignore

    @post_dump
    def remove_skip_values(self, data, **kwargs):
        return OrderedDict([
            (key, value) for key, value in data.items()
            if value not in BaseModel.SKIP_VALUES
        ])

    @staticmethod
    def _sortable(data):
        if not isinstance(data, Iterable):
            return False
        # Get the type of the first element
        try:
            first = next(iter(data))
        except StopIteration:
            # empty iterable, still sortable
            return True
        keytype = type(first)
        # Check that all elements are of that type
        return isinstance(first, Hashable) and all(map(lambda item: isinstance(item, keytype), data))

    @staticmethod
    def _sort(data):
        if BaseModel._sortable(data):
            if is_listlike(data):
                return sorted(data)
            elif isinstance(data, (dict, OrderedDict)):
                return OrderedDict(sorted(data.items(), key=lambda t: t[0]))
            else:
                return data
        else:
            return data

    @post_dump
    def sort_listlikes(self, data, **kwargs):
        return BaseModel._sort(data)

    @classmethod
    def load(cls, data: dict, **kwargs) -> Self:
        return cls.Schema().load(data, **kwargs)

    def dumps(self):
        return type(self).Schema().dumps(self) #type: ignore

    @classmethod
    def load_yaml(cls, filename: Union[Path, str]):
        return cls.Schema().load(parse_yaml(str(filename))) #type: ignore

    def save_yaml(self, filename: Path):
        with filename.open('w') as fp:
            print(type(self).Schema().dumps(self), file=fp) #type: ignore

    def to_raw_yaml(self):
        return type(self).Schema().dump(self) #type: ignore

    def to_dict(self):
        return self.to_raw_yaml()

    @classmethod
    def from_other(cls, other, **kwargs):
        return cls.Schema().load(puppet_merge(other.to_raw_yaml(), dict(**kwargs))) #type: ignore

    @classmethod
    def field_names(cls):
        return set(cls.Schema().fields.keys())

    @classmethod
    def field_deserializer(cls, field_name: str):
        field = cls.Schema().fields[field_name]
        if not hasattr(field, 'inner'):
            return field.deserialize
        else:
            return field.inner.deserialize


def _describe_schema(fields, level):
    for field in fields:
        indent = ' ' * level * 4
        if isinstance(field, mf.Nested):
            print(f'{indent}- {field.name}: {field.nested.__name__}')
            _describe_schema(field.schema.fields.values(), level + 1)
            continue
        elif isinstance(field, mdUnion):
            types = '|'.join((t[1].__class__.__name__ for t in field.union_fields))
            print(f'{indent}- {field.name}: {types}')
            continue
        elif isinstance(field, tuple):
            field = field[1]
        print(f'{indent}- {field.name}: {field.__class__.__name__}')


def describe_schema(schema):
    _describe_schema(schema.fields.values(), 0)


@dataclass(frozen=True)
class BaseModel(_BaseModel):

    Schema: ClassVar[Type[_Schema]] = _Schema # For the type checker


def validate_yaml_forest(yaml_forest: dict,
                         MapSchema: Type[BaseModel], 
                         strict: Optional[bool] = False,
                         partial: Optional[bool] = False) -> Generator[tuple[str, BaseModel], None, None]: 

    logger = logging.getLogger(__name__)

    for source_root, yaml_obj in yaml_forest.items():

        try:
            puppet_data = MapSchema.load(yaml_obj,
                                         partial=partial)
        except marshmallow.exceptions.ValidationError as e: #type: ignore
            logger.error(f'[red]ValidationError: {source_root}[/]')
            logger.error(e.messages)
            if strict:
                sys.exit(ExitCode.VALIDATION_ERROR)
            continue
        else:
            yield source_root, puppet_data


SlurmAccount = namedtuple('SlurmAccount', ['max_user_jobs',
                                           'max_group_jobs',
                                           'max_submit_jobs',
                                           'max_job_length'])


SlurmAssociation = namedtuple('SlurmAssociation', ['username',
                                                   'account',
                                                   'partition',
                                                   'qos'])


KerberosID = Annotated[str, mf.String(validate=mv.Regexp(r'[a-z_]([a-z0-9_-]{0,31}|[a-z0-9_-]{0,30}\$)'))]


class SimpleDate(mf.Date):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, format='%Y-%m-%d', **kwargs)


Date = Annotated[datetime.date, SimpleDate]

Email = Annotated[str, mf.Email]

IPv4 = Annotated[str, mf.IPv4]

PuppetEnsure = Annotated[str, mf.String(validate=mv.OneOf(("present", "absent")))]

PuppetMembership = Annotated[str, mf.String(validate=mv.OneOf(("inclusive", "minimum")))]

PuppetAbsent = Annotated[str, mf.String(validate=mv.Equal("absent"))]

UInt32 = Annotated[int, mf.Integer(validate=mv.Range(min=0, max=UINT_MAX))]

IAMID = UInt32

MothraID = UInt32

LinuxUID = UInt32

LinuxGID = UInt32

# TODO: Needs to actually be "x" or "min length"
LinuxPassword = Annotated[str, mf.String(validate=mv.Length(min=0))]

Shell = Annotated[str, mf.String(validate=mv.OneOf(ENABLED_SHELLS | DISABLED_SHELLS))]

DataQuota = Annotated[str, mf.String(validate=mv.Regexp(r'[+-]?([0-9]*[.])?[0-9]+[MmGgTtPp]'))]

SlurmQOSValidFlags = ("DenyOnLimit",
                      "EnforceUsageThreshold",
                      "NoDecay",
                      "NoReserve",
                      "OverPartQOS",
                      "PartitionMaxNodes",
                      "PartitionMinNodes",
                      "PartitionTimeLimit",
                      "RequiresReservation",
                      "UsageFactorSafe")

SlurmQOSFlag = Annotated[str, mf.String(validate=mv.OneOf(SlurmQOSValidFlags))]

UserType = Annotated[str, mf.String(validate=mv.OneOf(USER_TYPES))]

UserStatus = Annotated[str, mf.String(validate=mv.OneOf(USER_STATUSES))]

AccessType = Annotated[str, mf.String(validate=mv.OneOf(ACCESS_TYPES))]


def hippo_to_cheeto_access(hippo_access_types: List[str]):
    access = set()
    if 'OpenOnDemand' in hippo_access_types: #type: ignore
        access.add('ondemand')
    if 'SshKey' in hippo_access_types: #type: ignore
        access.add('login-ssh')
    return access


SEQUENCE_FIELDS = {
    marshmallow_dataclass.collection_field.Sequence,
    marshmallow_dataclass.collection_field.Set,
    mf.List,
    mf.Tuple
}
