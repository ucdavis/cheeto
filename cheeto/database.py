#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023-2024
# (c) The Regents of the University of California, Davis, 2023-2024
# File   : database.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 09.08.2024

import argparse
from collections import defaultdict
from collections.abc import Iterable
from enum import member
import logging
from pathlib import Path
from re import A, L
from typing import List, Optional, no_type_check, Self, Union

from bson.json_util import default
from mongoengine import *
from mongoengine import signals
from mongoengine.queryset.visitor import Q as Qv

from cheeto.git import GitRepo

from .args import subcommand
from .config import HippoConfig, LDAPConfig, MongoConfig, Config
from .errors import ExitCode
from .hippoapi.api.event_queue import (event_queue_pending_events,
                                       event_queue_update_status)
from .hippoapi.client import AuthenticatedClient
from .hippoapi.models import (QueuedEventAccountModel,
                              QueuedEventModel,
                              QueuedEventDataModel,
                              QueuedEventUpdateModel)
from .ldap import LDAPCommitFailed, LDAPManager, LDAPUser, LDAPGroup
from .log import Console
from .puppet import (MIN_PIGROUP_GID,
                     MIN_SYSTEM_UID,
                     PuppetAccountMap, 
                     PuppetGroupRecord,
                     PuppetGroupStorage, PuppetShareMap, PuppetShareRecord, PuppetShareStorage,
                     PuppetUserRecord,
                     PuppetUserStorage,
                     SlurmQOS as PuppetSlurmQOS,
                     SlurmQOSTRES as PuppetSlurmQOSTRES,
                     add_repo_args,
                     SiteData)
from .types import (DEFAULT_SHELL, DISABLED_SHELLS, ENABLED_SHELLS, GROUP_TYPES,
                    HIPPO_EVENT_ACTIONS, HIPPO_EVENT_STATUSES, MOUNT_OPTS, SlurmAccount, hippo_to_cheeto_access,  
                    is_listlike, UINT_MAX, USER_TYPES,
                    USER_STATUSES, ACCESS_TYPES, SlurmQOSValidFlags)
from .utils import (TIMESTAMP_NOW,
                    __pkg_dir__,
                    _ctx_name, removed_nones,
                    size_to_megs,
                    removed,
                    remove_nones)
from .yaml import dumps as dumps_yaml
from .yaml import (highlight_yaml,
                   parse_yaml, puppet_merge)


def POSIXNameField(**kwargs):
    return StringField(regex=r'[a-z_]([a-z0-9_-]{0,31}|[a-z0-9_-]{0,30}\$)',
                       min_length=1,
                       max_length=32,
                       **kwargs)

def POSIXIDField(**kwargs):
    return LongField(min_value=0, 
                     max_value=UINT_MAX,
                     **kwargs)

def UInt32Field(**kwargs) -> LongField:
    return LongField(min_value=0,
                     max_value=UINT_MAX,
                     **kwargs)

def UserTypeField(**kwargs): 
    return StringField(choices=USER_TYPES,
                       **kwargs)

def UserStatusField(**kwargs):
    return StringField(choices=USER_STATUSES,
                       **kwargs)

def UserAccessField(**kwargs) -> StringField:
    return StringField(choices=ACCESS_TYPES,
                       **kwargs)

def GroupTypeField(**kwargs) -> StringField:
    return StringField(choices=GROUP_TYPES,
                       **kwargs)

def ShellField(**kwargs) -> StringField:
    return StringField(choices=ENABLED_SHELLS | DISABLED_SHELLS,
                       default=DEFAULT_SHELL,
                       **kwargs)

def DataQuotaField(**kwargs) -> StringField:
    return StringField(regex=r'[+-]?([0-9]*[.])?[0-9]+[MmGgTtPp]',
                       **kwargs)

def SlurmQOSFlagField(**kwargs) -> StringField:
    return StringField(choices=SlurmQOSValidFlags,
                       **kwargs)

class InvalidUser(RuntimeError):
    pass


def handler(event):
    """Signal decorator to allow use of callback functions as class decorators."""

    def decorator(fn):
        def apply(cls):
            event.connect(fn, sender=cls)
            return cls

        fn.apply = apply
        return fn

    return decorator


class BaseDocument(Document):
    meta = {
        'abstract': True
    }

    def to_dict(self, strip_id=True, strip_empty=False, raw=False, rekey=False, **kwargs):
        data = self.to_mongo(use_db_field=False).to_dict()
        if strip_id:
            data.pop('id', None)
            data.pop('_id', None)
        for key in list(data.keys()):
            if strip_empty:
                if not data[key] and data[key] != 0:
                    del data[key]
            if not raw and key in data \
               and isinstance(self._fields[key], (ReferenceField, GenericReferenceField)):
                data[key] = getattr(self, key).to_dict(strip_empty=strip_empty,
                                                       raw=raw,
                                                       strip_id=strip_id,
                                                       rekey=rekey)
            if rekey and key in data and key.startswith('_'):
                data[key.lstrip('_') ] = data[key]
                del data[key]
        return data

    def clean(self):
        if 'sitename' in self._fields: #type: ignore
            query_site_exists(self.sitename) #type: ignore

    def __repr__(self):
        return dumps_yaml(self.to_dict(raw=True, strip_id=False, strip_empty=False))

    def pretty(self) -> str:
        data = self.to_dict(raw=False, strip_id=True, strip_empty=True, rekey=True)
        return dumps_yaml(data)


class HippoEvent(BaseDocument):
    hippo_id = LongField(required=True, unique=True)
    action = StringField(required=True,
                         choices=HIPPO_EVENT_ACTIONS)
    n_tries = IntField(required=True, default=0)
    status = StringField(required=True,
                         default='Pending',
                         choices=HIPPO_EVENT_STATUSES)
    data = DictField()


class GlobalUser(BaseDocument):
    username = POSIXNameField(required=True, primary_key=True)
    email = EmailField(required=True)
    uid = POSIXIDField(required=True)
    gid = POSIXIDField(required=True)
    fullname = StringField(required=True)
    shell = ShellField(required=True)
    home_directory = StringField(required=True)
    type = UserTypeField(required=True)
    status = UserStatusField(required=True, default='active')
    password = StringField()
    ssh_key = ListField(StringField())
    access = ListField(UserAccessField(), default=lambda: ['login-ssh'])
    comments = ListField(StringField())

    ldap_synced = BooleanField(default=False)
    iam_synced = BooleanField(default=False)

    @classmethod
    def from_puppet(cls, username: str, 
                         puppet_record: PuppetUserRecord,
                         ssh_key: Optional[str] = None) -> Self:
        
        usertype = puppet_record.usertype
        shell = puppet_record.shell if puppet_record.shell else DEFAULT_SHELL

        if ssh_key is None and usertype != 'system':
            access = {'ondemand'}
        else:
            access = {'login-ssh'}

        tags = {} if puppet_record.tag is None else puppet_record.tag
        if 'root-ssh-tag' in tags:
            access.add('root-ssh')
        if 'ssh-tag' in tags:
            access.add('compute-ssh')
        if 'sudo-tag' in tags:
            access.add('sudo')

        if usertype == 'admin':
            access.add('root-ssh')
            access.add('compute-ssh')
            access.add('sudo')

        pw = puppet_record.password
        if pw == 'x':
            pw = None

        home_directory = puppet_record.home
        if home_directory is None:
            home_directory = f'/home/{username}'

        return cls(
            username = username,
            email = puppet_record.email,
            uid = puppet_record.uid,
            gid = puppet_record.gid,
            fullname = puppet_record.fullname,
            shell = shell,
            home_directory=home_directory,
            type = usertype,
            status = puppet_record.status,
            ssh_key = [] if ssh_key is None else ssh_key.split('\n'),
            access = access,
            password = pw
        )

    @classmethod
    def from_hippo(cls, hippo_data: QueuedEventAccountModel) -> Self:
        return cls(
            username = hippo_data.kerberos,
            email = hippo_data.email,
            uid = hippo_data.mothra,
            gid = hippo_data.mothra,
            fullname = hippo_data.name,
            shell = DEFAULT_SHELL,
            home_directory = f'/home/{hippo_data.kerberos}',
            type = 'user',
            status = 'active',
            ssh_key = [hippo_data.key],
            access = hippo_to_cheeto_access(hippo_data.access_types) #type: ignore
        )

    #@property
    #def home_directory(self):
    #    return f'/home/{self.username}'


global_user_t = Union[GlobalUser, str]


@handler(signals.post_save)
def user_apply_globals(sender, document, **kwargs):
    logger = logging.getLogger(__name__)
    site = Site.objects().get(sitename=document.sitename)
    if site.global_groups:
        for site_group in site.global_groups:
            logger.info(f'Splat {site.sitename}: Add member {document.username} to group {site_group.groupname}')
            site_group.update(add_to_set___members=document,
                              ldap_synced=False)
    if site.global_slurmers:
        for site_group in site.global_slurmers:
            logger.info(f'Splat {site.sitename}: Add slurmer {document.username} to group {site_group.groupname}')
            site_group.update(add_to_set___slurmers=document,
                              ldap_synced=False)


@user_apply_globals.apply #type: ignore
class SiteUser(BaseDocument):
    username = POSIXNameField(required=True)
    sitename = StringField(required=True, unique_with='username')
    parent = ReferenceField(GlobalUser, required=True, reverse_delete_rule=CASCADE)
    expiry = DateField()
    _status = UserStatusField(default='active')
    #slurm: Optional[SlurmRecord] = None
    _access = ListField(UserAccessField(), default=lambda: ['login-ssh'])

    ldap_synced = BooleanField(default=False)
    iam_synced = BooleanField(default=False)

    meta = {
        'indexes': [
            {
                'fields': ('username', 'sitename'),
                'unique': True
            }
        ]
    }

    @property
    def email(self):
        return self.parent.email

    @property
    def uid(self):
        return self.parent.uid

    @property
    def gid(self):
        return self.parent.gid

    @property
    def fullname(self):
        return self.parent.fullname

    @property
    def shell(self):
        return self.parent.shell

    @property
    def home_directory(self):
        return self.parent.home_directory

    @property
    def password(self):
        return self.parent.password

    @property
    def type(self):
        return self.parent.type

    @property
    def status(self):
        if self.parent.status != 'active':
            return self.parent.status
        else:
            return self._status

    @property
    def ssh_key(self):
        return self.parent.ssh_key

    @property
    def access(self):
        return set(self._access) | set(self.parent.access)

    @property
    def comments(self):
        return self.parent.comments

    @classmethod
    def from_puppet(cls, username: str,
                         sitename: str,
                         parent: GlobalUser,
                         puppet_record: PuppetUserRecord) -> Self:

        puppet_data = puppet_record.to_dict()
        access = set()
        if 'tag' in puppet_data:
            tags = puppet_data['tag']
            if 'root-ssh-tag' in tags:
                access.add('root-ssh')
            if 'ssh-tag' in tags:
                access.add('compute-ssh')
            if 'sudo-tag' in tags:
                access.add('sudo')
        
        return cls(
            username = username,
            sitename = sitename,
            expiry = puppet_data.get('expiry', None),
            _access = access,
            _status = puppet_record.status,
            parent = parent
        )

    def to_dict(self, strip_id=True, strip_empty=False, raw=False, **kwargs):
        data = super().to_dict(strip_id=strip_id, strip_empty=strip_empty, raw=raw, **kwargs)
        if not raw:
            data['parent'] = self.parent.to_dict(strip_id=strip_id, raw=raw, strip_empty=strip_empty, **kwargs) #type: ignore
        return data

site_user_t = Union[SiteUser, str]


class GlobalGroup(BaseDocument):
    groupname = POSIXNameField(required=True, primary_key=True)
    gid = POSIXIDField(required=True)
    type = GroupTypeField(required=True, default='group')

    ldap_synced = BooleanField(default=False)
    iam_synced = BooleanField(default=False)
    
    @classmethod
    def from_puppet(cls, groupname: str, puppet_record: PuppetGroupRecord) -> Self:
        return cls(
            groupname = groupname,
            gid = puppet_record.gid
        )


global_group_t = Union[GlobalGroup, str]


class SiteSlurmPartition(BaseDocument):
    partitionname = POSIXNameField(required=True)
    sitename = StringField(required=True, unique_with='partitionname')


class SlurmTRES(EmbeddedDocument):
    cpus = IntField(default=-1)
    gpus = IntField(default=-1)
    mem = DataQuotaField()

    def to_slurm(self) -> str:
        tokens = [f'cpu={self.cpus}',
                  f'mem={size_to_megs(self.mem) if self.mem is not None else -1}', #type: ignore
                  f'gres/gpu={self.gpus}']
        return ','.join(tokens)

    @staticmethod
    def negate() -> str:
        return 'cpu=-1,mem=-1,gres/gpu=-1'

    def clean(self):
        if self.mem:
            self.mem = f'{size_to_megs(self.mem)}M' #type: ignore

    @classmethod
    def from_puppet(cls, puppet_tres: PuppetSlurmQOSTRES) -> Self:
        return cls(cpus=puppet_tres.cpus,
                   gpus=puppet_tres.gpus,
                   mem=puppet_tres.mem)

    def to_puppet(self) -> PuppetSlurmQOSTRES:
        return PuppetSlurmQOSTRES(cpus=None if self.cpus == -1 else self.cpus,
                                  gpus=None if self.gpus == -1 else self.gpus,
                                  mem=self.mem)

    def to_dict(self):
        data = self.to_mongo(use_db_field=False).to_dict()
        data.pop('id', None)
        data.pop('_id', None)
        return data


class SiteSlurmQOS(BaseDocument):
    sitename = StringField(required=True)
    qosname = StringField(required=True, unique_with='sitename')
    group_limits = EmbeddedDocumentField(SlurmTRES)
    user_limits = EmbeddedDocumentField(SlurmTRES)
    job_limits = EmbeddedDocumentField(SlurmTRES)
    priority = IntField()
    flags = ListField(SlurmQOSFlagField)

    @property
    def group(self):
        return self.group_limits

    @property
    def user(self):
        return self.user_limits

    @property
    def job(self):
        return self.job_limits

    @no_type_check
    def to_slurm(self) -> List[str]:
        tokens = []
        grptres = self.group_limits.to_slurm() \
            if self.group_limits is not None else SlurmTRES.negate() 
        usertres = self.user_limits.to_slurm() \
            if self.user_limits is not None else SlurmTRES.negate()
        jobtres = self.job_limits.to_slurm() \
            if self.job_limits is not None else SlurmTRES.negate()
        flags = ','.join(self.flags) if self.flags else '-1'
        
        tokens.append(f'GrpTres={grptres}')
        tokens.append(f'MaxTRESPerUser={usertres}')
        tokens.append(f'MaxTresPerJob={jobtres}')
        tokens.append(f'Flags={flags}')
        tokens.append(f'Priority={self.priority}')

        return tokens

    @no_type_check
    def to_puppet(self):
        return PuppetSlurmQOS(group=self.group_limits.to_puppet() if self.group_limits else None,
                              user=self.user_limits.to_puppet() if self.user_limits else None,
                              job=self.job_limits.to_puppet() if self.job_limits else None,
                              priority=self.priority if self.priority else 0,
                              flags=self.flags if self.flags else None)
        #return PuppetSlurmQOS.load(dict(group=removed_nones(self.group_limits.to_dict())\
        #                                    if self.group_limits else None,
        #                                user=removed_nones(self.user_limits.to_dict())\
        #                                    if self.user_limits else None,
        #                                job=removed_nones(self.job_limits.to_dict())\
        #                                    if self.job_limits else None,
        #                                priority=self.priority if self.priority else 0,
        #                                flags=self.flags if self.flags else None))

    @classmethod
    def from_puppet(cls, qosname: str, sitename: str, puppet_qos: PuppetSlurmQOS) -> Self:
        return cls(qosname=qosname,
                   sitename=sitename,
                   group_limits=SlurmTRES.from_puppet(puppet_qos.group) \
                       if puppet_qos.group is not None else None,
                   user_limits=SlurmTRES.from_puppet(puppet_qos.user) \
                       if puppet_qos.user is not None else None,
                   job_limits=SlurmTRES.from_puppet(puppet_qos.job) \
                       if puppet_qos.job is not None else None,
                   priority = puppet_qos.priority,
                   flags = list(puppet_qos.flags) \
                       if puppet_qos.flags is not None else None)


class SiteSlurmAccount(EmbeddedDocument):
    max_user_jobs = IntField(default=-1)
    max_group_jobs = IntField(default=-1)
    max_submit_jobs = IntField(default=-1)
    max_job_length = StringField(default='-1')


class SiteGroup(BaseDocument):
    groupname = POSIXNameField(required=True)
    sitename = StringField(required=True, unique_with='groupname')
    parent = ReferenceField(GlobalGroup, required=True, reverse_delete_rule=CASCADE)
    _members = ListField(ReferenceField(SiteUser, reverse_delete_rule=PULL))
    _sponsors = ListField(ReferenceField(SiteUser, reverse_delete_rule=PULL))
    _sudoers = ListField(ReferenceField(SiteUser, reverse_delete_rule=PULL))
    _slurmers = ListField(ReferenceField(SiteUser, reverse_delete_rule=PULL))
    slurm = EmbeddedDocumentField(SiteSlurmAccount, default=SiteSlurmAccount)

    ldap_synced = BooleanField(default=False)
    iam_synced = BooleanField(default=False)

    meta = {
        'indexes': [
            {
                'fields': ('groupname', 'sitename'),
                'unique': True
            },
            {
                'fields': ['_members']
            },
            {
                'fields': ['_slurmers']
            }
        ]
    }

    @property
    def gid(self):
        return self.parent.gid

    @property
    def members(self):
        return {m.username for m in self._members} #type: ignore

    @property
    def sponsors(self):
        return {s.username for s in self._sponsors} #type: ignore

    @property
    def sudoers(self):
        return {s.username for s in self._sudoers} #type: ignore

    @property
    def slurmers(self):
        return {s.username for s in self._slurmers} #type: ignore

    @classmethod
    def from_puppet(cls, groupname: str,
                         sitename: str,
                         parent: GlobalGroup,
                         puppet_record: PuppetGroupRecord) -> Self:
        max_jobs = puppet_record.slurm.max_jobs if puppet_record.slurm else -1
        return cls(
            groupname = groupname,
            sitename = sitename,
            parent = parent,
            slurm = SiteSlurmAccount(max_user_jobs=max_jobs)
        )

    def to_dict(self, strip_id=True, strip_empty=False, raw=False, **kwargs):
        data = super().to_dict(strip_id=strip_id, strip_empty=strip_empty, raw=raw, **kwargs)
        if not raw:
            data['parent'] = self.parent.to_dict() #type: ignore
            data.pop('_members')
            data['members'] = self.members
            data.pop('_sponsors')
            data['sponsors'] = self.sponsors
            data.pop('_sudoers')
            data['sudoers'] = self.sudoers
            data.pop('_slurmers')
            data['slurmers'] = self.slurmers
        return data


site_group_t = Union[SiteGroup, str]


class SiteSlurmAssociation(BaseDocument):
    sitename = StringField(required=True)
    qos = ReferenceField(SiteSlurmQOS, required=True, reverse_delete_rule=CASCADE)
    partition = ReferenceField(SiteSlurmPartition, required=True, reverse_delete_rule=CASCADE)
    group = ReferenceField(SiteGroup, required=True, reverse_delete_rule=CASCADE)

    def to_dict(self, strip_id=True, strip_empty=False, raw=False, **kwargs):
        data = super().to_dict(strip_id=strip_id, strip_empty=strip_empty, raw=raw, **kwargs)
        if not raw:
            data['qos'] = self.qos.to_dict(strip_id=strip_id) #type: ignore
            data['partition'] = self.partition.partitionname #type: ignore
            data['group'] = self.group.groupname #type: ignore
        return data


class NFSSourceCollection(BaseDocument):
    sitename = StringField(required=True)
    name = StringField(required=True)
    _host = StringField()
    prefix = StringField()
    _export_options = StringField()
    _export_ranges = ListField(StringField())

    meta = {
        'allow_inheritance': True,
        'indexes': [
            {
                'fields': ['sitename', 'name', '_cls'],
                'name': 'primary',
                'unique': True
            }
        ]
    }


class ZFSSourceCollection(NFSSourceCollection):
    _quota = DataQuotaField()


class StorageMountSource(BaseDocument):
    name = StringField(required=True)
    sitename = StringField(required=True)
    _host  = StringField()
    owner = ReferenceField(GlobalUser, required=True, reverse_delete_rule=CASCADE)
    group = ReferenceField(GlobalGroup, required=True, reverse_delete_rule=CASCADE)

    meta = {
        'allow_inheritance': True,
        'indexes': [
            {
                'fields': ['sitename', 'name', '_cls'],
                'name': 'primary',
                'unique': True
            }
        ]
    }

    def to_dict(self, strip_id=True, strip_empty=False, raw=False, **kwargs):
        data = super().to_dict(strip_id=strip_id, strip_empty=strip_empty, raw=raw, **kwargs)
        if not raw:
            data['owner'] = data['owner']['username']
            data['group'] = data['group']['groupname']
        return data


class NFSMountSource(StorageMountSource):
    _host_path = StringField()
    _export_options = StringField()
    _export_ranges = ListField(StringField())
    collection = GenericReferenceField(choices=[NFSSourceCollection,
                                                ZFSSourceCollection])

    @property
    def export_options(self) -> str:
        if self._export_options:
            return self._export_options
        if self.collection and self.collection._export_options:
            return self.collection._export_options
        return ''

    @property
    def export_ranges(self):
        if self.collection:
            return sorted(set(self.collection._export_ranges) | set(self._export_ranges))
        else:
            return sorted(set(self._export_ranges))

    @property
    def host(self):
        if self._host:
            return self._host
        elif self.collection._host:
            return self.collection._host
        else:
            raise ValueError(f'MountSource {self.name} has no host specified')

    @property
    def host_path(self):
        if self._host_path:
            return Path(self._host_path)
        elif self.collection.prefix:
            return Path(self.collection.prefix) / self.name
        else:
            raise ValueError(f'MountSource {self.name} has neither host_path nor collection prefix')



class ZFSMountSource(NFSMountSource):
    _quota = DataQuotaField()

    @property
    def quota(self):
        if self._quota:
            return self._quota
        elif self.collection._quota:
            return self.collection._quota
        else:
            return None

def validate_mount_options(option: str):
    tokens = option.split('=')
    if tokens[0] not in MOUNT_OPTS:
        raise ValidationError(f'{option} not a valid mount option')

class MountOptionsMixin(Document):
    _options = ListField(StringField(validation=validate_mount_options))
    _add_options = ListField(StringField(validation=validate_mount_options))
    _remove_options = ListField(StringField(validation=validate_mount_options))

    meta = {
        'abstract': True
    }


class AutomountMap(BaseDocument, MountOptionsMixin):
    sitename = StringField(required=True)
    prefix = StringField(required=True)
    tablename = StringField(required=True, unique_with=['sitename', 'prefix'])


class StorageMount(BaseDocument, MountOptionsMixin):
    sitename = StringField(required=True)

    meta = {
        'allow_inheritance': True
    }


class Automount(StorageMount):
    name = StringField(required=True)
    map = ReferenceField(AutomountMap, required=True, unique_with='name')

    @property
    def mount_options(self):
        if self._options:
            return self._options

        options = set(self.map._options)
        if self._remove_options:
            options = options - self._remove_options
        if self._add_options:
            options = options | self._add_options

        return options

    @property
    def mount_path(self):
        return Path(self.map.prefix) / self.name


class QuobyteMount(StorageMount):
    pass


class BeeGFSMount(StorageMount):
    pass


class Storage(BaseDocument):
    name = StringField(required=True, unique_with='mount')
    source = GenericReferenceField(required=True,
                                   choices=[NFSMountSource,
                                            ZFSMountSource])
    mount = GenericReferenceField(required=True,
                                  choices=[Automount])
    globus = BooleanField()

    @property
    def owner(self):
        return self.source.owner.username

    @property
    def group(self):
        return self.source.group.groupname

    @property
    def host_path(self):
        return self.source.host_path

    @property
    def host(self):
        return self.source.host

    @property
    def mount_path(self):
        return self.mount.mount_path

    @property
    def mount_options(self):
        return self.mount.mount_options

    @property
    def quota(self):
        if not isinstance(self.source, ZFSMountSource):
            return None
        else:
            return self.source.quota


@handler(signals.post_save)
def site_apply_globals(sender, document, **kwargs):
    logger = logging.getLogger(__name__)
    logger.info(f'Site {document.sitename} modified, syncing globals')
    if document.global_groups or document.global_slurmers:
        site_users = SiteUser.objects(sitename=document.sitename)
        logger.info(f'Update globals with {len(site_users)} users')
        for site_group in document.global_groups:
            for site_user in site_users:
                site_group.update(add_to_set___groups=site_user,
                                  ldap_synced=False)
        for site_group in document.global_slurmers:
            for site_user in site_users:
                site_group.update(add_to_set___slurmers=site_user,
                                  ldap_synced=False)


@site_apply_globals.apply #type: ignore
class Site(BaseDocument):
    sitename = StringField(required=True, primary_key=True)
    fqdn = StringField(required=True)
    global_groups = ListField(ReferenceField(SiteGroup, reverse_delete_rule=PULL))
    global_slurmers = ListField(ReferenceField(SiteGroup, reverse_delete_rule=PULL))
    default_home = GenericReferenceField()


site_t = Union[Site, str]


def connect_to_database(config: MongoConfig, quiet: bool = False):
    if not quiet:
        console = Console(stderr=True)
        console.print(f'mongo config:')
        console.print(f'  uri: [green]{config.uri}:{config.port}')
        console.print(f'  user: [green]{config.user}')
        console.print(f'  db: [green]{config.database}')
        console.print(f'  tls: {config.tls}')
    return connect(config.database,
                   host=config.uri,
                   username=config.user,
                   password=config.password,
                   tls=config.tls)


def query_user_exists(username: str,
                sitename: Optional[str] = None,
                raise_exc: bool = False) -> bool:
    logger = logging.getLogger(__name__)
    try:
        if sitename is None:
            _ = GlobalUser.objects.get(username=username)
        else:
            _ = SiteUser.objects.get(username=username, sitename=sitename)
    except DoesNotExist:
        if raise_exc:
            logger.warning(f'User {username} at site {sitename} does not exist.')
            raise
        return False
    else:
        return True


def query_site_exists(sitename: str, raise_exc: bool = False) -> bool:
    logger = logging.getLogger(__name__)
    try:
        Site.objects.get(sitename=sitename) #type: ignore
    except DoesNotExist:
        if raise_exc:
            logger.error(f'Site {sitename} does not exist.')
            raise
        logger.info(f'Site {sitename} does not exist.')
        return False
    else:
        return True

def tag_comment(comment: str):
    return f'[{TIMESTAMP_NOW}]: {comment}'


def add_user_comment(username: str, comment: str):
    comment = tag_comment(comment)
    GlobalUser.objects(username=username).update_one(push__comments=comment)


def add_user_access(username: str, access_type: str, sitename: Optional[str] = None):
    if sitename is None:
        GlobalUser.objects(username=username).update_one(add_to_set__access=access_type,
                                                         ldap_synced=False) #type: ignore
    else:
        SiteUser.objects(username=username, #type: ignore
                         sitename=sitename).update_one(add_to_set___access=access_type,
                                                       ldap_synced=False)


def set_user_status(username: str,
                    status: str,
                    reason: str,
                    sitename: Optional[str] = None):

    scope = "global" if sitename is None else sitename
    comment = f'status={status}, scope={scope}, reason={reason}'
    add_user_comment(username, comment)

    if sitename is None:
        GlobalUser.objects(username=username).update_one(set__status=status,
                                                         ldap_synced=False) #type: ignore
    else:
        SiteUser.objects(username=username, #type: ignore
                         sitename=sitename).update_one(set___status=status,
                                                       ldap_synced=False)


def set_user_type(username: str,
                  usertype: str):
    GlobalUser.objects(username=username).update_one(set__type=usertype,
                                                     ldap_synced=False)


def create_group_from_sponsor(sponsor_user: SiteUser):
    logger = logging.getLogger(__name__)

    groupname = f'{sponsor_user.username}grp'
    gid = MIN_PIGROUP_GID + sponsor_user.parent.uid

    global_group = GlobalGroup(groupname=groupname, gid=gid)
    global_group.save()
    logger.info(f'Created GlobalGroup from sponsor {sponsor_user.username}: {global_group.to_dict()}')

    site_group = SiteGroup(groupname=groupname,
                           sitename=sponsor_user.sitename,
                           parent=global_group,
                           _members=[sponsor_user],
                           _sponsors=[sponsor_user],
                           _sudoers=[sponsor_user])
    site_group.save()
    logger.info(f'Created SiteGroup from sponsor {sponsor_user.username}: {site_group.to_dict()}')


def query_group_slurm_associations(sitename: str, group: site_group_t):
    if type(group) is str:
        group = SiteGroup.objects.get(sitename=sitename, groupname=group)

    users = (user for user in (set(group._members) | set(group._slurmers)) \
             if 'slurm' in user.access)
    associations = SiteSlurmAssociation.objects(sitename=sitename,
                                                group=group)
    for user in users:
        for assoc in associations:
            yield (user.username, assoc.group.groupname, assoc.partition.partitionname), assoc.qos.qosname


def query_user_groups(sitename: str, user: site_user_t):
    if type(user) is str:
        user = SiteUser.objects.get(sitename=sitename, username=user)
    qs = SiteGroup.objects(_members=user, sitename=sitename).only('groupname')
    for group in qs:
        yield group.groupname


def query_user_slurmership(sitename: str, user: site_user_t):
    if type(user) is str:
        user = SiteUser.objects.get(sitename=sitename, username=user)
    qs = SiteGroup.objects(_slurmers=user, sitename=sitename).only('groupname')
    for group in qs:
        yield group.groupname


def query_user_sudogroups(sitename: str, user: site_user_t):
    if type(user) is str:
        user = SiteUser.objects.get(sitename=sitename, username=user)
    qs = SiteGroup.objects(_sudoers=user, sitename=sitename).only('groupname')
    for group in qs:
        yield group.groupname


def query_user_slurm(sitename: str, user: site_user_t):
    if type(user) is str:
        user = SiteUser.objects.get(sitename=sitename, username=user)
    groups = SiteGroup.objects(Qv(_members=user, sitename=sitename)
                               | Qv(_slurmers=user, sitename=sitename))

    associations = SiteSlurmAssociation.objects(sitename=sitename,
                                                group__in=groups)
    for assoc in associations:
        yield assoc


def query_user_partitions(sitename: str, user: site_user_t):
    partitions = defaultdict(dict)
    for assoc in query_user_slurm(sitename, user):
        qos = removed(assoc.qos.to_dict(strip_empty=True), 'qosname')
        partitions[assoc.partition.partitionname][assoc.group.groupname] = qos
    return dict(partitions)


def query_user_sponsor_of(sitename: str, user: site_user_t):
    if type(user) is str:
        user = SiteUser.objects.get(sitename=sitename, username=user)
    qs = SiteGroup.objects(_sponsors=user, sitename=sitename).only('groupname')
    for group in qs:
        yield group.groupname


def query_admin_keys(sitename: Optional[str] = None):
    if sitename is not None:
        query = SiteUser.objects(sitename=sitename,
                                 parent__in=GlobalUser.objects(type='admin'))
    else:
        query = GlobalUser.objects(type='admin')
    
    keys = []
    for user in query:
        if 'root-ssh' in user.access:
            keys.extend(user.ssh_key)

    return keys


def query_user_home_storage(sitename: str, user: site_user_t):
    logger = logging.getLogger(__name__)
    if type(user) is SiteUser:
        user = user.username
    home_collection = NFSSourceCollection.objects.get(name='home',
                                                      sitename=sitename)
    home_sources = StorageMountSource.objects(collection=home_collection,
                                              name=user)
    #logger.info(f'{_ctx_name()}: {user}: {home_sources}')
    return Storage.objects.get(source__in=home_sources)
 

def query_user_storages(sitename: str, user: site_user_t):
    if type(user) is str:
        user = GlobalUser.objects.get(sitename=sitename, username=user)
    else:
        user = user.parent

    return Storage.objects(source__in=StorageMountSource.objects(sitename=sitename,
                                                                 owner=user))


def query_group_storages(sitename: str, group: global_group_t, tablename: Optional[str] = None):
    if type(group) is str:
        group = GlobalGroup.objects.get(groupname=group)
    if tablename:
        mounts = Automount.objects(map=AutomountMap.objects.get(sitename=sitename, tablename='group'))
        return Storage.objects(source__in=StorageMountSource.objects(sitename=sitename, group=group),
                               mount__in=mounts)
    else:
        return Storage.objects(source__in=StorageMountSource.objects(sitename=sitename, group=group))


def query_automap_storages(sitename: str, tablename: str):
    automap = AutomountMap.objects.get(sitename=sitename, tablename=tablename)
    mounts = Automount.objects(map=automap).only('id')

    return Storage.objects(mount__in=mounts)


def query_associated_storages(sitename: str, username: str):
    user = SiteUser.objects.get(sitename=sitename, username=username)
    sitegroups = SiteGroup.objects(sitename=sitename, _members=user).only('parent')
    globalgroups = [sg.parent for sg in sitegroups]

    sources = StorageMountSource.objects(Qv(sitename=sitename) &
                                         (Qv(group__in=globalgroups) | Qv(owner=user.parent)))
    return Storage.objects(source__in=sources)


def add_group_member(sitename: str, user: site_user_t, groupname: str):
    if type(user) is str:
        user = SiteUser.objects.get(sitename=sitename, username=user)
    logging.getLogger(__name__).info(f'Add user {user.username} to group {groupname} for site {sitename}')
    SiteGroup.objects(sitename=sitename,
                      groupname=groupname).update_one(add_to_set___members=user,
                                                      ldap_synced=False)


def add_group_sponsor(sitename: str, user: site_user_t, groupname: str):
    if type(user) is str:
        user = SiteUser.objects.get(sitename=sitename, username=user)
    logging.getLogger(__name__).info(f'Add sponsor {user.username} to group {groupname} for site {sitename}')
    SiteGroup.objects(sitename=sitename,
                      groupname=groupname).update_one(add_to_set___sponsors=user)


def add_group_sudoer(sitename: str, user: site_user_t, groupname: str):
    if type(user) is str:
        user = SiteUser.objects.get(sitename=sitename, username=user)
    logging.getLogger(__name__).info(f'Add sudoer {user.username} to group {groupname} for site {sitename}')
    SiteGroup.objects(sitename=sitename,
                      groupname=groupname).update_one(add_to_set___sudoers=user)


def add_group_slurmer(sitename: str, user: site_user_t, groupname: str):
    if type(user) is str:
        user = SiteUser.objects.get(sitename=sitename, username=user)
    logging.getLogger(__name__).info(f'Add slurmer {user.username} to group {groupname} for site {sitename}')

    SiteGroup.objects(sitename=sitename,
                      groupname=groupname).update_one(add_to_set___slurmers=user)


def add_site_slurmer(sitename: str, group: site_group_t):
    if type(group) is str:
        group = SiteGroup.objects.get(sitename=sitename, groupname=group)
    Site.objects(sitename=sitename).update_one(add_to_set__global_slurmers=group)
    Site.objects.get(sitename=sitename).save()


def add_site_group(sitename: str, group: site_group_t):
    if type(group) is str:
        group = SiteGroup.objects.get(sitename=sitename, groupname=group)
    Site.objects.get(sitename=sitename).update_one(add_to_set__global_groups=group,
                                                   ldap_synced=False)
    Site.objects.get(sitename=sitename).save()


def get_next_system_id() -> int:
    ids = set((u.uid for u in GlobalUser.objects(uid__gt=MIN_SYSTEM_UID, 
                                                 uid__lt=MIN_SYSTEM_UID+100000000))) \
        | set((g.gid for g in GlobalGroup.objects(gid__gt=MIN_SYSTEM_UID, 
                                                  gid__lt=MIN_SYSTEM_UID+100000000)))
    return max(ids) + 1


def create_home_storage(sitename: str, user: global_user_t):
    logger = logging.getLogger(__name__)
    if type(user) is str:
        user = GlobalUser.objects.get(username=user)
    group = GlobalGroup.objects.get(groupname=user.username)
    collection = ZFSSourceCollection.objects.get(sitename=sitename,
                                                 name='home')
    automap = AutomountMap.objects.get(sitename=sitename,
                                       tablename='home')

    source = ZFSMountSource(name=user.username,
                            sitename=sitename,
                            owner=user,
                            group=group,
                            collection=collection)
    source.save()

    mount = Automount(sitename=sitename,
                      name=user.username,
                      map=automap)
    try:
        mount.save()
    except Exception as e:
        logger.error(f'{_ctx_name()}: could not save mount {mount}')
        source.delete()
        raise

    storage = Storage(name=user.username,
                      source=source,
                      mount=mount)
    storage.save()


def create_system_group(groupname: str, sitenames: Optional[list[str]] = None):
    logger = logging.getLogger(__name__)
    global_group = GlobalGroup(groupname=groupname,
                               gid=get_next_system_id())
    global_group.save()
    logger.info(f'Created system GlobalGroup {groupname} gid={global_group.gid}')

    if sitenames is not None:
        for sitename in sitenames:
            site_group = SiteGroup(groupname=groupname,
                                   sitename=sitename,
                                   parent=global_group)
            site_group.save()
            logger.info(f'Created system SiteGroup {groupname} for site {sitename}')


def load_share_from_puppet(shares: dict[str, PuppetShareRecord],
                           sitename: str):
    logger = logging.getLogger(__name__)
    logger.info(f'Load share storages on site {sitename}')

    collection = ZFSSourceCollection.objects.get(sitename=sitename,
                                                 name='share')
    automap = AutomountMap.objects.get(sitename=sitename,
                                       tablename='share')

    for share_name, share in shares.items():
        owner = GlobalUser.objects.get(username=share.storage.owner)
        group = GlobalGroup.objects.get(groupname=share.storage.group)

        source_args = dict(name=share_name,
                           sitename=sitename,
                           _host_path=share.storage.autofs.path,
                           _host=share.storage.autofs.nas,
                           owner=owner,
                           group=group,
                           collection=collection)
        source_type = NFSMountSource

        if share.storage.zfs:
            source_type = ZFSMountSource
            source_args['_quota'] = share.storage.zfs.quota

        source = source_type(**source_args)

        try:
            source.save()
        except NotUniqueError:
            source = source_type.objects.get(sitename=sitename,
                                             name=share.storage.autofs.path)

        mount = Automount(sitename=sitename,
                          name=share_name,
                          map=automap,
                          _options=share.storage.autofs.split_options())
        try:
            mount.save()
        except NotUniqueError:
            mount = Automount.objects.get(sitename=sitename,
                                          name=share_name,
                                          map=automap)

        storage = Storage(name=share_name,
                          source=source,
                          mount=mount)

        try:
            storage.save()
        except NotUniqueError:
            pass


def load_group_storages_from_puppet(storages: List[PuppetGroupStorage],
                                    groupname: str,
                                    sitename: str):

    logger = logging.getLogger(__name__)
    logger.info(f'Load storages for group {groupname} on site {sitename}')

    collection = ZFSSourceCollection.objects.get(sitename=sitename,
                                                 name='group')
    automap = AutomountMap.objects.get(sitename=sitename,
                                       tablename='group')

    for storage in storages:

        owner = GlobalUser.objects.get(username=storage.owner)
        if storage.group:
            group = GlobalGroup.objects.get(groupname=storage.group)
        else:
            group = GlobalGroup.objects.get(groupname=groupname)
        if not storage.zfs:
            source = NFSMountSource(name=storage.name,
                                    sitename=sitename,
                                    _host_path=storage.autofs.path,
                                    _host=storage.autofs.nas,
                                    owner=owner,
                                    group=group,
                                    collection=collection)
            try:
                source.save()
            except NotUniqueError:
                source = NFSMountSource.objects.get(sitename=sitename,
                                                    _host=storage.autofs.nas,
                                                    _host_path=storage.autofs.path)
                                                    
        else:
            source = ZFSMountSource(name=storage.autofs.path,
                                    sitename=sitename,
                                    _host=storage.autofs.nas,
                                    _host_path=storage.autofs.path,
                                    owner=owner,
                                    group=group,
                                    _quota=storage.zfs.quota,
                                    collection=collection)
            try:
                source.save()
            except NotUniqueError:
                source = ZFSMountSource.objects.get(sitename=sitename,
                                                    _host=storage.autofs.nas,
                                                    _host_path=storage.autofs.path)

        mount = Automount(sitename=sitename,
                          map=automap,
                          name=storage.name,
                          _options=storage.autofs.split_options())
        try:
            mount.save()
        except NotUniqueError:
            mount = Automount.objects.get(sitename=sitename,
                                          map=automap,
                                          name=storage.name)
        
        record = Storage(name=storage.name,
                         source=source,
                         mount=mount,
                         globus=storage.globus)
        try:
            record.save()
        except NotUniqueError:
            pass


def create_slurm_partition(partitionname: str, sitename: str):
    logger = logging.getLogger(__name__)
    logger.info(f'Create partition {partitionname} on site {sitename}.')
    query_site_exists(sitename, raise_exc=True)

    try:
        partition = SiteSlurmPartition(sitename=sitename, partitionname=partitionname)
        partition.save()
    except Exception as e:
        logger.error(f'Error creating partition {partitionname} on site {sitename}: {e.__class__} {e}')
        partition = SiteSlurmPartition.objects.get(sitename=sitename, partitionname=partitionname)

    return partition


def create_slurm_qos(qosname: str,
                     sitename: str,
                     group_limits: Optional[SlurmTRES] = None,
                     user_limits: Optional[SlurmTRES] = None,
                     job_limits: Optional[SlurmTRES] = None,
                     priority: int = 0,
                     flags: Optional[set[str]] = None):

    logger = logging.getLogger(__name__)
    logger.info(f'Create QOS {qosname} on site {sitename}.')
    try:
        qos = SiteSlurmQOS(qosname=qosname,
                           sitename=sitename,
                           group_limits=group_limits,
                           user_limits=user_limits,
                           job_limits=job_limits,
                           priority=priority,
                           flags=list(flags) if flags is not None else None)
        qos.save()
    except Exception as e:
        logger.error(f'Error creating QOS {qosname}: {e.__class__} {e}')
        qos = SiteSlurmQOS.objects.get(qosname=qosname, sitename=sitename)

    return qos


def create_slurm_association(sitename: str, partitionname: str, groupname: str, qosname: str):
    logger = logging.getLogger(__name__)
    logger.info(f'Create Association ({groupname}, {partitionname}, {qosname}) on site {sitename}.')
    query_site_exists(sitename, raise_exc=True)

    partition = SiteSlurmPartition.objects.get(partitionname=partitionname, sitename=sitename)
    group = SiteGroup.objects.get(sitename=sitename, groupname=groupname)
    qos = SiteSlurmQOS.objects.get(qosname=qosname, sitename=sitename)

    try:
        assoc = SiteSlurmAssociation.objects.get(sitename=sitename,
                                                 qos=qos,
                                                 group=group,
                                                 partition=partition)
    except DoesNotExist:
        assoc = SiteSlurmAssociation(sitename=sitename,
                                     qos=qos,
                                     group=group,
                                     partition=partition)
        assoc.save()

    return assoc


def load_slurm_from_puppet(sitename: str, data: PuppetAccountMap):
    logger = logging.getLogger(__name__)
    from .slurm import get_qos_name

    partitions = set()
    qos_map = {}
    qos_references = []
    for group_name, group in data.group.items():
        if group.slurm is None or group.slurm.partitions is None:
            continue
        for partition_name, partition in group.slurm.partitions.items():
            partitions.add(partition_name)
            # If it is a reference to another QOS, it will be put in the map
            # where it is defined
            if type(partition.qos) is str:
                qos_references.append((group_name, partition_name, partition.qos))
                continue
            qos_name = get_qos_name(group_name, partition_name)
            qos_map[qos_name] = (group_name, partition_name, partition.qos)

    # Validate that all QOS references actually exist
    for group_name, partition_name, qos_name in qos_references:
        if qos_name not in qos_map:
            raise ValueError(f'{group_name} has invalid QoS for {partition_name}: {qos_name}')

    for partitionname in partitions:
        create_slurm_partition(partitionname=partitionname, sitename=sitename)

    for qosname, (groupname, partitionname, puppet_qos) in qos_map.items():
        qos = SiteSlurmQOS.from_puppet(qosname, sitename, puppet_qos)
        try:
            qos.save()
        except:
            logger.info(f'QOS {qosname} already exists')

        create_slurm_association(sitename, partitionname, groupname, qosname)

    for groupname, partitionname, qosname in qos_references:
        create_slurm_association(sitename, partitionname, groupname, qosname)

    for username, user in data.user.items():
        if user.slurm is not None and user.slurm.account is not None:
            for groupname in user.slurm.account:
                add_group_slurmer(sitename, username, groupname)


def slurm_qos_state(sitename: str) -> dict[str, SiteSlurmQOS]:
    return {s.qosname: s.to_puppet() for s in SiteSlurmQOS.objects(sitename=sitename)}


def slurm_association_state(sitename: str, ignore: set[str] = {'root'}):
    logger = logging.getLogger(__name__)

    state = dict(users={}, accounts={})

    for group in SiteGroup.objects(sitename=sitename,
                                   parent__in=GlobalGroup.objects(type='group')):

        assocs = False
        for assoc_tuple, qos_name in query_group_slurm_associations(sitename, group):
            state['users'][assoc_tuple] = qos_name
            assocs = True
        
        if assocs:
            account = SlurmAccount(max_user_jobs=group.slurm.max_user_jobs,
                                   max_submit_jobs=group.slurm.max_submit_jobs,
                                   max_group_jobs=group.slurm.max_group_jobs,
                                   max_job_length=group.slurm.max_job_length)
            state['accounts'][group.groupname] = account

    return state


def user_to_puppet(user: SiteUser):
    logger = logging.getLogger(__name__)
    memberships = set(query_user_groups(user.sitename, user))
    memberships.remove(user.username)
    memberships = sorted(memberships)
    slurmerships = sorted(list(query_user_slurmership(user.sitename, user)))
    group_sudo = sorted(list(query_user_sudogroups(user.sitename, user)))
    slurm = {'account': slurmerships} if slurmerships else None
    
    tags = set()
    if 'compute-ssh' in user.access:
        tags.add('ssh-tag')
    if 'root-ssh' in user.access:
        tags.add('root-ssh-tag')
    if 'sudo' in user.access:
        tags.add('sudo-tag')
    if user.type == 'system':
        tags.add('system-tag')

    try:
        home_storage = query_user_home_storage(user.sitename, user)
    except DoesNotExist:
        storage = None
    else:
        storage = dict(autofs={'nas': home_storage.host,
                               'path': str(home_storage.host_path.parent)},
                       zfs=get_puppet_zfs(home_storage))
    user_data = dict(fullname=user.fullname,
                     email=user.email,
                     uid=user.uid,
                     gid=user.gid,
                     groups=memberships,
                     group_sudo=group_sudo,
                     shell=user.shell,
                     tag=tags,
                     home=user.home_directory,
                     password=user.password,
                     expiry=str(user.expiry) if user.expiry else None,
                     slurm=slurm,
                     storage=storage)
    remove_nones(user_data)
    try:
        return PuppetUserRecord.load(user_data)
    except:
        logger.error(f'{_ctx_name()}: PuppetUserRecord validation error loading {user_data}')
        return None


def get_puppet_zfs(storage: Storage):
    if type(storage.source) is ZFSMountSource and storage.source.quota:
        return {'quota': storage.source.quota}
    else:
        return False


def group_to_puppet(group: SiteGroup):
    storages = query_group_storages(group.sitename,
                                    group.parent,
                                    tablename='group')
    storages_data = [
        {'name': s.name,
         'owner': s.owner,
         'group': s.group,
         'autofs': {
            'nas': s.host,
            'path': str(s.host_path),
            'options': ','.join(sorted(set(s.mount_options) - {'fstype=nfs'}, reverse=True))
         },
         'zfs': get_puppet_zfs(s),
         'globus': s.globus
        } for s in storages
    ]

    return PuppetGroupRecord.load(dict(gid=group.gid,
                                       sponsors=group.sponsors,
                                       storage=storages_data))


def share_to_puppet(share: Storage):
    storage_data = {
        'owner': share.owner,
        'group': share.group,
        'autofs': {
            'nas': share.host,
            'path': str(share.host_path)},
        'zfs': get_puppet_zfs(share)
    }

    return PuppetShareRecord.load(dict(storage=storage_data))


def site_to_puppet(sitename: str):
    users = {}
    for user in SiteUser.objects(sitename=sitename).order_by('username'):
        record = user_to_puppet(user)
        if record:
            users[user.username] = record

    groups = {}
    for group in SiteGroup.objects(sitename=sitename).order_by('groupname'):
        if group.parent.type == 'user':
            user = SiteUser.objects.get(username=group.groupname, sitename=sitename)
            if user.uid == group.gid:
                continue
        groups[group.groupname] = group_to_puppet(group)

    shares = {}
    storages = query_automap_storages(sitename, 'share')
    for share in storages.order_by('name'):
        shares[share.name] = share_to_puppet(share)

    return PuppetAccountMap(user=users,
                            group=groups,
                            share=shares)


def purge_database():
    prompt = input('WARNING: YOU ARE ABOUT TO PURGE THE DATABASE. TYPE "PURGE" TO CONTINUE: ')
    if prompt != 'PURGE':
        print('Aborting!')
        return

    for collection in (Site, GlobalUser, SiteUser, GlobalGroup, SiteGroup, HippoEvent,
                       SiteSlurmAssociation, SiteSlurmPartition, SiteSlurmQOS,
                       StorageMountSource, NFSMountSource, ZFSMountSource,
                       StorageMount, Automount, AutomountMap, Storage, NFSSourceCollection):
        collection.drop_collection()


#########################################
#
# Site commands: cheeto database site ...
#
#########################################


def add_site_args(parser: argparse.ArgumentParser):
    parser.add_argument('--site', '-s', default=None)


def add_site_args_req(parser: argparse.ArgumentParser):
    parser.add_argument('--site', '-s', default=None, required=True)


def parse_site_arg(site: str):
    try:
        Site.objects.get(sitename=site)
    except:
        site = Site.objects.get(fqdn=site)
        return site.sitename
    else:
        return site


@subcommand('add', add_site_args)
def site_add(args: argparse.Namespace):
    logger = logging.getLogger(__name__)


@subcommand('add-global-slurm',
            add_site_args,
            lambda parser: parser.add_argument('groups', nargs='+')) #type: ignore
def site_add_global_slurm(args: argparse.Namespace):
    logger = logging.getLogger(__name__)
    connect_to_database(args.config.mongo)

    for group in args.groups:
        add_site_slurmer(args.site, group)


@subcommand('to-puppet',
            add_site_args_req,
            lambda parser: parser.add_argument('puppet_yaml', type=Path))
def site_write_to_puppet(args: argparse.Namespace):
    logger = logging.getLogger(__name__)
    connect_to_database(args.config.mongo)

    puppet_map = site_to_puppet(args.site)
    puppet_map.save_yaml(args.puppet_yaml)


@subcommand('sync-old-puppet',
            add_site_args_req,
            lambda parser: parser.add_argument('repo', type=Path),
            lambda parser: parser.add_argument('--base-branch', default='main'),
            lambda parser: parser.add_argument('--push-merge', default=False, action='store_true'))
def site_sync_old_puppet(args: argparse.Namespace):
    connect_to_database(args.config.mongo)

    site = Site.objects.get(sitename=args.site)
    repo = GitRepo(args.repo, base_branch=args.base_branch)
    prefix = (args.repo / 'domains' / site.fqdn).absolute()
    yaml_path = prefix / 'merged' / 'all.yaml'
    puppet_map = site_to_puppet(args.site)

    with repo.commit(f'Update merged yaml for {args.site}',
                     clean=True,
                     push_merge=args.push_merge) as add:
        puppet_map.save_yaml(yaml_path)

        for user in SiteUser.objects(sitename=args.site).only('parent'):
            if not user.parent.ssh_key:
                continue
            keyfile = (args.repo / 'keys' / f'{user.parent.username}.pub').absolute()
            with keyfile.open('w') as fp:
                for key in user.parent.ssh_key:
                    print(key, file=fp)
        add(args.repo.absolute())


@subcommand('to-ldap',
            add_site_args_req,
            lambda parser: parser.add_argument('--force', '-f', default=False, action='store_true'))
def site_sync_to_ldap(args: argparse.Namespace):
    ldap_sync(args.site, args.config, force=args.force)


@subcommand('to-sympa',
            add_site_args_req,
            lambda parser: parser.add_argument('output_txt', type=Path),
            lambda parser: parser.add_argument('--ignore', nargs='+', default=['hpc-help@ucdavis.edu']))
def site_write_sympa(args: argparse.Namespace):
    connect_to_database(args.config.mongo)
    _site_write_sympa(args.site, args.output_txt, set(args.ignore))


def _site_write_sympa(sitename: str, output: Path, ignore: set):
    with output.open('w') as fp:
        for user in SiteUser.objects(sitename=sitename, 
                                     parent__in=GlobalUser.objects(type__in=['user', 'admin'])):
            if user.status != 'inactive' and user.email not in ignore:
                print(user.email, file=fp)


@subcommand('root-key',
            add_site_args_req,
            lambda parser: parser.add_argument('output_txt', type=Path))
def site_write_root_key(args: argparse.Namespace):
    connect_to_database(args.config.mongo)
    _site_write_root_key(args.site, args.output_txt)


def _site_write_root_key(sitename: str, output: Path):
    with output.open('w') as fp:
        for user in SiteUser.objects(sitename=sitename,
                                     parent__in=GlobalUser.objects(type='admin')):
            if 'root-ssh' in user.access:
                if user.ssh_key:
                    print(f'# {user.username} <{user.email}>', file=fp)
                for key in user.ssh_key:
                    print(key, file=fp)


@subcommand('sync-new-puppet',
            add_site_args_req,
            lambda parser: parser.add_argument('repo', type=Path),
            lambda parser: parser.add_argument('--ignore-emails', nargs='+',  default=['hpc-help@ucdavis.edu']),
            lambda parser: parser.add_argument('--push-merge', default=False, action='store_true'))
def site_sync_new_puppet(args: argparse.Namespace):
    connect_to_database(args.config.mongo)

    site = Site.objects.get(sitename=args.site)
    repo = GitRepo(args.repo)
    prefix = (args.repo / 'domains' / site.fqdn).absolute()

    with repo.commit(f'Update root.pub, storage.yaml, and sympa.txt for {site.fqdn}.',
                     clean=True,
                     push_merge=args.push_merge) as add:
        root_key_path = prefix / 'root.pub'
        _site_write_root_key(args.site, root_key_path)

        storage_path = prefix / 'storage.yaml'
        _storage_to_puppet(args.site, storage_path)

        sympa_path = prefix / 'sympa.txt'
        _site_write_sympa(args.site, sympa_path, args.ignore_emails)

        add(root_key_path, storage_path, sympa_path)


@subcommand('list')
def site_list(args: argparse.Namespace):
    connect_to_database(args.config.mongo)

    for site in Site.objects():
        print(site.sitename, site.fqdn)


@subcommand('show',
            lambda parser: parser.add_argument('sitename'))
def site_show(args: argparse.Namespace):
    connect_to_database(args.config.mongo)

    site = Site.objects.get(sitename=args.sitename)
    console = Console()
    console.print(site.fqdn)
    total_users = SiteUser.objects(sitename=args.sitename).count()
    reg_users = SiteUser.objects(sitename=args.sitename, parent__in=GlobalUser.objects(type='user')).count()
    admin_users = SiteUser.objects(sitename=args.sitename, parent__in=GlobalUser.objects(type='admin')).count()
    system_users = SiteUser.objects(sitename=args.sitename, parent__in=GlobalUser.objects(type='system')).count()

#########################################
#
# load commands: cheeto database load ...
#
#########################################


def add_database_load_args(parser: argparse.ArgumentParser):
    parser.add_argument('--system-groups', action='store_true', default=False)
    parser.add_argument('--nfs-yamls', nargs='+', type=Path)


@subcommand('load', add_repo_args, add_site_args, add_database_load_args)
def load(args: argparse.Namespace):
    logger = logging.getLogger(__name__)
    
    site_data = SiteData(args.site_dir,
                         common_root=args.global_dir,
                         key_dir=args.key_dir,
                         load=False)
    nfs_data = puppet_merge(*(parse_yaml(f) for f in args.nfs_yamls)).get('nfs', None)
    connect_to_database(args.config.mongo)

    with site_data.lock(args.timeout):
        site_data.load()

        site = Site(sitename=args.site,
                    fqdn=args.site_dir.name)
        logger.info(f'Updating Site: {site}')
        site.save()

        # Do automount tables
        if nfs_data:
            export_options = nfs_data['exports'].get('options', None)
            export_ranges = nfs_data['exports'].get('clients', None)
            for tablename, config in nfs_data['storage'].items():
                autofs = config['autofs']
                col_args = dict(sitename=args.site,
                                name=tablename,
                                _host=autofs.get('nas', None),
                                prefix=autofs.get('path', None),
                                _export_options=export_options,
                                _export_ranges=export_ranges,
                                _quota=config.get('zfs', {}).get('quota', None))
                collection = ZFSSourceCollection(**{k:v for k, v in col_args.items() if v is not None})
                try:
                    collection.save()
                except NotUniqueError:
                    pass

                if (raw_opts := config['autofs'].get('options', False)):
                    options = raw_opts.strip('-').split(',')
                else:
                    options = None

                mount_args = dict(sitename=args.site,
                                  prefix=f'/{tablename}',
                                  tablename=tablename,
                                  _options=options)
                mountmap = AutomountMap(**{k:v for k, v in mount_args.items() if v is not None})
                try:
                    mountmap.save()
                except NotUniqueError:
                    pass

        for group_name, group_record in site_data.iter_groups():
            #logger.info(f'{group_name}, {group_record}')
            if group_record.ensure == 'absent':
                continue
            global_record = GlobalGroup.from_puppet(group_name, group_record)
            global_record.save()

            site_record = SiteGroup.from_puppet(group_name,
                                                args.site,
                                                global_record,
                                                group_record)
            try:
                site_record.save()
            except:
                logger.info(f'{group_name} in {args.site} already exists, skipping.')
                site_record = SiteGroup.objects.get(groupname=group_name, sitename=args.site)
   
            #logger.info(f'Added {group_name}: {group_record}')
        logger.info(f'Processed {len(site_data.data.group)} groups.') #type: ignore

        group_memberships = defaultdict(set)
        group_sudoers = defaultdict(set)
        home_collection = ZFSSourceCollection.objects.get(sitename=args.site,
                                                          name='home')
        home_automap = AutomountMap.objects.get(sitename=args.site,
                                                tablename='home')
        for user_name, user_record in site_data.iter_users():
            if user_record.ensure == 'absent':
                continue
            ssh_key_path, ssh_key = args.key_dir / f'{user_name}.pub', None
            if ssh_key_path.exists():
                ssh_key = ssh_key_path.read_text().strip()

            global_record = GlobalUser.from_puppet(user_name, user_record, ssh_key=ssh_key)
            global_record.save()
            global_group = GlobalGroup(groupname=user_name,
                                       gid=user_record.gid)
            global_group.save()

            site_record = SiteUser.from_puppet(user_name,
                                               args.site,
                                               global_record,
                                               user_record)

            try:
                site_record.save()
            except:
                logger.info(f'{user_name} in {args.site} already exists, skipping.')
                site_record = SiteUser.objects.get(username=user_name, sitename=args.site)

            site_group = SiteGroup(sitename=args.site,
                                   groupname=user_name,
                                   parent=global_group,
                                   _members=[site_record])
            try:
                site_group.save()
            except NotUniqueError:
                logger.info(f'{_ctx_name()}: SiteGroup {user_name} already exists, adding user as member')
                add_group_member(args.site, site_record, user_name)
            except Exception as e:
                logger.warning(f'{_ctx_name()}: error saving SiteGroup for {user_name}: {e}')

            if global_record.type == 'system' and args.system_groups:
                global_group = GlobalGroup(groupname=user_name, gid=user_record.gid)
                global_group.save()
                
                site_group = SiteGroup(groupname=user_name, sitename=args.site, parent=global_group)
                try:
                    site_group.save()
                except:
                    logger.info(f'System group {user_name} in {args.site} already exists, skipping.')

            if user_record.groups is not None:
                for group_name in user_record.groups:
                    group_memberships[group_name].add(site_record)

            if user_record.group_sudo is not None:
                for group_name in user_record.group_sudo:
                    group_sudoers[group_name].add(site_record)
            
            if user_name == 'root':
                continue

            us = user_record.storage
            if us and us.autofs:
                source = NFSMountSource(name=user_name,
                                        sitename=args.site,
                                        _host=us.autofs.nas,
                                        _host_path=str(Path(us.autofs.path) / user_name),
                                        owner=global_record,
                                        group=global_group,
                                        collection=home_collection)
            elif us and not us.autofs and us.zfs and us.zfs.quota:
                source = ZFSMountSource(name=user_name,
                                        sitename=args.site,
                                        owner=global_record,
                                        group=global_group,
                                        _quota=us.zfs.quota,
                                        collection=home_collection)
            else:
                source = ZFSMountSource(name=user_name,
                                        sitename=args.site,
                                        owner=global_record,
                                        group=global_group,
                                        collection=home_collection)
            try:
                source.save()
            except NotUniqueError:
                source = StorageMountSource.objects.get(sitename=args.site,
                                                        name=user_name)

            options = us.autofs.split_options() if us is not None and us.autofs is not None else None
            mount = Automount(sitename=args.site,
                              name=user_name,
                              map=home_automap,
                              _options=options)
            try:
                mount.save()
            except NotUniqueError:
                mount = Automount.objects.get(sitename=args.site,
                                              name=user_name,
                                              map=home_automap)
            storage_record = Storage(name=user_name,
                                     source=source,
                                     mount=mount)
            try:
                storage_record.save()
            except NotUniqueError:
                pass

        # Now do sponsors and storages
        for groupname, group_record in site_data.iter_groups():
            if group_record.ensure == 'absent':
                continue
            if group_record.storage is not None:
                load_group_storages_from_puppet(group_record.storage,
                                                groupname,
                                                args.site)
            if group_record.sponsors is not None:
                for username in group_record.sponsors:
                    add_group_sponsor(args.site, username, groupname)
                    add_group_member(args.site, username, 'sponsors')

        logger.info(f'Added {len(site_data.data.user)} users.') #type: ignore

        for groupname, members in group_memberships.items():
            try:
                SiteGroup.objects(groupname=groupname,
                                  sitename=args.site).update_one(add_to_set___members=list(members)) #type: ignore
            except Exception as e:
                logger.info(f'{e}')
                logger.warning(f'Did not find group {groupname}, skip adding {[m.username for m in members]}')
                continue

        load_share_from_puppet(site_data.data.share, args.site)

        logger.info(f'Do slurm associations...')
        load_slurm_from_puppet(args.site, site_data.data)

        logger.info('Done.')



def add_query_args(parser: argparse.ArgumentParser):
    parser.add_argument('--in', dest='val_in', nargs='+', action='append')
    parser.add_argument('--not-in', dest='val_not_in', nargs='+', action='append')
    parser.add_argument('--contains', nargs='+', action='append')
    parser.add_argument('--gt', nargs=2, action='append')
    parser.add_argument('--lt', nargs=2, action='append')
    parser.add_argument('--match', '-m', nargs=2, action='append')
    parser.add_argument('--search', nargs='+', action='append')
    parser.add_argument('--all', default=False, action='store_true')


def validate_query_groups(groups, record_type, min_args=2, single=False):
    logger = logging.getLogger(__name__)
    for query_group in groups:
        field = query_group[0]
        if field not in record_type.field_names():
            logger.warning(f'Skipping query group, {field} is not valid for {record_type}: {query_group}.')
            continue
        deserialize = record_type.field_deserializer(field)
        if len(query_group) < min_args:
            raise RuntimeError(f'Query argument group needs at least {min_args} arguments, got: {query_group}.')
        if single:
            if len(query_group) != 2:
                raise RuntimeError(f'Query group should have exactly two arguments, got {query_group}.')
            value = query_group[1]
            yield field, deserialize(value)
        else:
            values = query_group[1:]
            yield field, list(map(deserialize, values))

'''
def build_simple_query(args: argparse.Namespace,
                       record_type: MongoModel):

    logger = logging.getLogger(__name__)
    query = defaultdict(dict)

    if args.search:
        for query_group in args.search:
            query['$text'] = {'$search': ' '.join(query_group)}
    else:
        if args.val_in:
for field, values in validate_query_groups(args.val_in, record_type):
                query[field]['$in'] = values
        if args.val_not_in:
            for field, values in validate_query_groups(args.val_not_in, record_type):
                query[field]['$nin'] = values
        if args.lt:
            for field, value in validate_query_groups(args.lt, record_type, single=True):
                query[field]['$lt'] = value
        if args.gt:
            for field, value in validate_query_groups(args.gt, record_type, single=True):
                query[field]['$gt'] = value
        if args.match:
            for field, value in validate_query_groups(args.match, record_type, single=True):
                query[field] = value
        if args.contains:
            for field, values in validate_query_groups(args.contains, record_type):
                if len(values) > 1:
                    query['$and'] = [{field: val} for val in values]
                else:
                    query[field] = values[0]

    logger.debug(f'Simple query: \n{dict(query)}')
    return dict(query)
'''


def build_filter_condition(args: argparse.Namespace,
                           field: str,
                           params: List[str]):
    pass


def add_query_user_args(parser: argparse.ArgumentParser):
    parser.add_argument('--fields', nargs='+')
    parser.add_argument('--list-fields', action='store_true', default=False)


@subcommand('query', add_site_args, add_query_args, add_query_user_args)
def query_users(args: argparse.Namespace):

    if args.list_fields:
        print('Global Fields:')
        describe_schema(GlobalUserRecord.Schema())
        print()
        print('Site Fields:')
        describe_schema(SiteUserRecord.Schema())
        return

    db = get_database(args.config.mongo)
    console = Console()

    records = []

    if args.site:
        if args.all:
            query = {'sitename': args.site}
            projection = {'users': True}
            results = db.sites.find_one(query, projection=projection)
            if results is None or 'users' not in results:
                return
            
            results = results['users']
            site_records = sorted([SiteUserRecord.load(user) for user in results], key=lambda r: r.username)
            
            site_usernames = [user.username for user in site_records]
            results = db.users.find({'username': {'$in': site_usernames}}).sort('username', ASCENDING)
            global_records = [GlobalUserRecord.load(user) for user in results]

            for global_record, site_record in zip(global_records, site_records):
                record = FullUserRecord.from_merge(global_record, site_record, args.site)
                records.append(record)
        else:
            query = {'sitename': args.site}
            projection = build_nested_filter_projection(args, 'users',
                                                        record_type=SiteUserRecord)
            results = db.sites.find(query, projection=projection)[0]['users']
            site_records = {record['_id']: SiteUserRecord.load(record) for record in results}

            query = build_simple_query(args, record_type=GlobalUserRecord)
            query['username'] = {'$in': list(site_records.keys())}
            results = db.users.find(query)
            global_records = [GlobalUserRecord.load(user) for user in results]

            for global_record in global_records:
                site_record = site_records[global_record.username]
                records.append(FullUserRecord.from_merge(global_record,
                                                         site_record,
                                                         args.site))
    else:
        if args.all:
            results = db.users.find()
            for record in results:
                records.append(GlobalUserRecord.load(record))
        else:
            query = build_simple_query(args, record_type=GlobalUserRecord)
            search = db.users.find(query)
            for record in search:
                records.append(GlobalUserRecord.load(record))
    
    if records:
        if args.fields:
            dumper = records[0].Schema(only=args.fields)
        else:
            dumper = records[0].Schema(exclude=['_id'])

        output_txt = dumper.dumps(records, many=True)
        output_txt = Syntax(output_txt,
                            'yaml',
                            theme='github-dark',
                            background_color='default')
        console.print(output_txt)


#########################################
#
# user commands: cheeto database user ...
#
#########################################


def add_show_user_args(parser: argparse.ArgumentParser):
    parser.add_argument('username')
    parser.add_argument('--verbose', action='store_true', default=False)


@subcommand('show', add_show_user_args, add_site_args)
def user_show(args: argparse.Namespace):
    logger = logging.getLogger(__name__)
    connect_to_database(args.config.mongo)

    try:
        if args.site is not None:
            user = SiteUser.objects.get(username=args.username,
                                        sitename=args.site).to_dict(strip_empty=True)
            if not args.verbose:
                user['parent'] = removed(user['parent'], 'ssh_key')
                del user['sitename']
                del user['parent']['gid']
            user['groups'] = list(query_user_groups(args.site, args.username))
            if args.verbose:
                user['slurm'] = list(map(lambda s: removed(s, 'sitename'),
                                         (s.to_dict() for s in query_user_slurm(args.site, args.username))))
            else:
                user['slurm'] = query_user_partitions(args.site, args.username)


        else:
            user = GlobalUser.objects.get(username=args.username).to_dict()
            user['sites'] = [su.sitename for su in SiteUser.objects(username=args.username)]
    except DoesNotExist:
        scope = 'Global' if args.site is None else args.site
        logger.info(f'User {args.username} with scope {scope} does not exist.')
    else:
        console = Console()
        output = dumps_yaml(user)
        console.print(highlight_yaml(output))


def add_user_status_args(parser: argparse.ArgumentParser):
    parser.add_argument('username')
    parser.add_argument('status', choices=list(USER_STATUSES))
    parser.add_argument('--reason', '-r', required=True)


@subcommand('set-status', add_user_status_args, add_site_args)
def user_set_status(args: argparse.Namespace):
    logger = logging.getLogger(__name__)
    connect_to_database(args.config.mongo)

    try:
        set_user_status(args.username, args.status, args.reason, sitename=args.site)
    except DoesNotExist:
        scope = 'Global' if args.site is None else args.site
        logger.info(f'User {args.username} with scope {scope} does not exist.')


def add_user_type_args(parser: argparse.ArgumentParser):
    parser.add_argument('username')
    parser.add_argument('type', choices=list(USER_TYPES))


@subcommand('set-type', add_user_type_args)
def user_set_type(args: argparse.Namespace):
    logger = logging.getLogger(__name__)
    connect_to_database(args.config.mongo)

    try:
        set_user_type(args.username, args.type)
    except GlobalUser.DoesNotExist:
        logger.info(f'User {args.username} does not exist.')


def add_user_membership_args(parser: argparse.ArgumentParser):
    parser.add_argument('users', nargs='+')


@subcommand('groups', add_user_membership_args, add_site_args_req)
def user_groups(args: argparse.Namespace):
    connect_to_database(args.config.mongo)
    console = Console()

    output = {}
    for username in args.users:
        output[username] = list(query_user_groups(args.site, username))
    dumped = dumps_yaml(output)
    console.print(highlight_yaml(dumped))


#########################################
#
# group commands: cheeto database group ...
#
#########################################


def add_query_group_args(parser: argparse.ArgumentParser):
    parser.add_argument('--fields', nargs='+')
    parser.add_argument('--list-fields', action='store_true', default=False)


@subcommand('query', add_query_args, add_query_group_args, add_site_args)
def query_groups(args: argparse.Namespace):
    if args.list_fields:
        print('Global Fields:')
        describe_schema(GlobalGroupRecord.Schema())
        print()
        print('Site Fields:')
        describe_schema(SiteGroupRecord.Schema())
        return

    db = get_database(args.config.mongo)
    console = Console()
    records = []

    if args.site:
        if args.all:
            query = {'sitename': args.site}
            projection = {'groups': True}
            results = db.sites.find_one(query, projection=projection)
            if results is None or 'groups' not in results:
                return
            
            results = results['groups']
            site_records = sorted([SiteGroupRecord.load(group) for group in results], key=lambda r: r.groupname)
            
            site_groupnames = [group.groupname for group in site_records]
            results = db.groups.find({'groupname': {'$in': site_groupnames}}).sort('groupname', ASCENDING)
            global_records = [GlobalGroupRecord.load(group) for group in results]

            for global_record, site_record in zip(global_records, site_records):
                record = FullGroupRecord.from_merge(global_record, site_record, args.site)
                records.append(record)
        else:
            query = {'sitename': args.site}
            projection = build_nested_filter_projection(args, 'groups',
                                                        record_type=SiteGroupRecord)
            results = db.sites.find(query, projection=projection)[0]['groups']
            site_records = {record['_id']: SiteGroupRecord.load(record) for record in results}

            query = build_simple_query(args, record_type=GlobalGroupRecord)
            query['groupname'] = {'$in': list(site_records.keys())}
            results = db.groups.find(query)
            global_records = [GlobalGroupRecord.load(group) for group in results]

            for global_record in global_records:
                site_record = site_records[global_record.groupname]
                records.append(FullGroupRecord.from_merge(global_record,
                                                         site_record,
                                                         args.site))
    else:
        if args.all:
            results = db.groups.find()
            for record in results:
                records.append(GlobalGroupRecord.load(record))
        else:
            query = build_simple_query(args, record_type=GlobalGroupRecord)
            search = db.groups.find(query)
            for record in search:
                records.append(GlobalGroupRecord.load(record))

    if records:
        if args.fields:
            dumper = records[0].Schema(only=args.fields)
        else:
            dumper = records[0].Schema(exclude=['_id'])

        output_txt = dumper.dumps(records, many=True)
        output_txt = Syntax(output_txt,
                            'yaml',
                            theme='github-dark',
                            background_color='default')
        console.print(output_txt)


def add_show_group_args(parser: argparse.ArgumentParser):
    parser.add_argument('groupname')


@subcommand('show', add_show_group_args, add_site_args)
def show_group(args: argparse.Namespace):
    logger = logging.getLogger(__name__)
    connect_to_database(args.config.mongo)

    try:
        if args.site is not None:
            group = SiteGroup.objects.get(groupname=args.groupname, sitename=args.site).to_dict()
        else:
            group = GlobalGroup.objects.get(groupname=args.groupname).to_dict()
            group['sites'] = [sg.sitename for sg in SiteGroup.objects(groupname=args.groupname)]
    except DoesNotExist:
        scope = 'Global' if args.site is None else args.site
        logger.info(f'Group {args.groupname} with scope {scope} does not exist.')
    else:
        console = Console()
        output = dumps_yaml(group)
        console.print(highlight_yaml(output))


def add_group_add_member_args(parser: argparse.ArgumentParser):
    parser.add_argument('--users', '-u', nargs='+', required=True)
    parser.add_argument('--groups', '-g', nargs='+', required=True)


@subcommand('add-user', add_group_add_member_args, add_site_args_req)
def group_add_user(args: argparse.Namespace):
    connect_to_database(args.config.mongo)
    console = Console()

    for username in args.users:
        for groupname in args.groups:
            add_group_member(args.site, username, groupname)


@subcommand('add-sponsor', add_group_add_member_args, add_site_args_req)
def group_add_sponsor(args: argparse.Namespace):
    connect_to_database(args.config.mongo)
    console = Console()

    for username in args.users:
        for groupname in args.groups:
            add_group_sponsor(args.site, username, groupname)


def add_group_new_system_args(parser: argparse.ArgumentParser):
    parser.add_argument('--sites', nargs='+', default='all')
    parser.add_argument('groupname')


@subcommand('new-system', add_group_new_system_args)
def group_new_system(args: argparse.Namespace):
    connect_to_database(args.config.mongo)
    console = Console()

    if args.sites == 'all':
        sites = [s.sitename for s in Site.objects()]
    else:
        sites = args.sites

    create_system_group(args.groupname, sitenames=sites)


#########################################
#
# storage commands: cheeto database storage ...
#
#########################################


def add_storage_args(parser: argparse.ArgumentParser):
    parser.add_argument('--name', required=True)
    parser.add_argument('--owner', required=True)
    parser.add_argument('--group', required=True)
    parser.add_argument('--host', required=True)
    parser.add_argument('--path', required=True)
    parser.add_argument('--table', required=True)
    parser.add_argument('--collection')
    parser.add_argument('--quota', help='Override default quota from the collection')
    parser.add_argument('--options', help='Override mount options from automount map for this mount')
    parser.add_argument('--add-options', help='Add mount options to automount map options for this mount')
    parser.add_argument('--remove-options', help='Remove mount options from automount map options for this mount')
    parser.add_argument('--globus', type=bool, default=False)


@subcommand('add', add_site_args_req, add_storage_args)
def storage_add(args: argparse.Namespace):
    logger = logging.getLogger(__name__)
    connect_to_database(args.config.mongo)

    if args.collection is None:
        args.collection = args.table
    
    automap = AutomountMap.objects.get(sitename=args.site,
                                       tablename=args.table)
    collection = ZFSSourceCollection.objects.get(sitename=args.site,
                                                 name=args.collection)
    owner = GlobalUser.objects.get(username=args.owner)
    group = GlobalGroup.objects.get(groupname=args.group)

    source = ZFSMountSource(name=args.name,
                            sitename=args.site,
                            _host=args.host,
                            _host_path=args.path,
                            owner=owner,
                            group=group,
                            collection=collection,
                            _quota=args.quota)

    mount = Automount(name=args.name,
                      sitename=args.site,
                      map=automap,
                      _options=args.options.split(',') if args.options else None,
                      _add_options=args.add_options.split(',') if args.add_options else None,
                      _remove_options=args.remove_options.split(',') if args.remove_options else None)

    try:
        source.save()
    except NotUniqueError:
        logger.error(f'ZFSMountSource with name {args.name} on site {args.site} already exists')
        return ExitCode.NOT_UNIQUE

    try:
        mount.save()
    except NotUniqueError:
        logger.error(f'ZFSMountSource with name {args.name} on site {args.site} already exists')
        source.delete()
        return ExitCode.NOT_UNIQUE
    except:
        source.delete()
        raise

    storage = Storage(name=args.name,
                      source=source,
                      mount=mount,
                      globus=args.globus)
    storage.save()


@subcommand('to-puppet',
            add_site_args_req, 
            lambda parser: parser.add_argument('puppet_yaml', type=Path))
def storage_to_puppet(args: argparse.Namespace):
    connect_to_database(args.config.mongo)
    _storage_to_puppet(args.site, args.puppet_yaml)


def _storage_to_puppet(sitename: str, output: Path):

    zfs = dict(group=defaultdict(list), user=defaultdict(list))
    nfs = dict(group=defaultdict(list), user=defaultdict(list))


    for s in query_automap_storages(sitename, 'group'):
        data = dict(name=s.name,
                    owner=s.owner, 
                    group=s.group, 
                    path=str(s.host_path), 
                    export_options=s.source.export_options, 
                    export_ranges=s.source.export_ranges)
        if s.source._cls == 'StorageMountSource.NFSMountSource.ZFSMountSource':
            data['quota'] = s.quota
            data['permissions'] = '2770'
            zfs['group'][s.host].append(data)
        else:
            nfs['group'][s.host].append(data)

    for s in query_automap_storages(sitename, 'home'):
        data = dict(name=s.name,
                    owner=s.owner, 
                    group=s.group, 
                    path=str(s.host_path),
                    export_options=s.source.export_options, 
                    export_ranges=s.source.export_ranges)
        if s.source._cls == 'StorageMountSource.NFSMountSource.ZFSMountSource':
            data['quota'] = s.quota
            data['permissions'] = '0770'
            zfs['user'][s.host].append(data)
        else:
            nfs['user'][s.host].append(data)

    puppet = {'zfs': zfs}
    if nfs:
        puppet['nfs'] = nfs

    with output.open('w') as fp:
        print(dumps_yaml(puppet), file=fp)


#########################################
#
# hippoapi commands: cheeto database hippoapi ...
#
#########################################


def add_hippoapi_event_args(parser: argparse.ArgumentParser):
    parser.add_argument('--post-back', '-p', default=False, action='store_true')
    parser.add_argument('--id', default=None, dest='event_id', type=int)
    parser.add_argument('--type', choices=list(HIPPO_EVENT_ACTIONS))


@subcommand('process', add_hippoapi_event_args)
def hippoapi_process(args: argparse.Namespace):
    connect_to_database(args.config.mongo)
    console = Console()
    process_hippoapi_events(args.config.hippo,
                            event_type=args.type,
                            event_id=args.event_id,
                            post_back=args.post_back)


@subcommand('events', add_hippoapi_event_args)
def hippoapi_events(args: argparse.Namespace):
    logger = logging.getLogger(__name__)
    console = Console()
    connect_to_database(args.config.mongo)
    with hippoapi_client(args.config.hippo) as client:
        events = event_queue_pending_events.sync(client=client)
   
    if not events:
        return

    for event in filter_events(events, event_type=args.type, event_id=args.event_id):
        console.print(event)


def filter_events(events: List[QueuedEventModel],
                  event_type: Optional[str] = None,
                  event_id: Optional[str] = None):

    if event_type is event_id is None:
        yield from events
    else:
        for event in events:
            if event.id == event_id:
                yield event
            elif event.action == event_type:
                yield event


def hippoapi_client(config: HippoConfig, quiet: bool = False):
    if not quiet:
        console = Console(stderr=True)
        console.print('hippo config:')
        console.print(f'  uri: {config.base_url}')
    return AuthenticatedClient(follow_redirects=True,
                               base_url=config.base_url,
                               token=config.api_key,
                               #httpx_args={"event_hooks": {"request": [log_request]}},
                               auth_header_name='X-API-Key',
                               prefix='')


def process_hippoapi_events(config: HippoConfig,
                            post_back: bool = False,
                            event_type: Optional[str] = None,
                            event_id: Optional[str] = None):
    logger = logging.getLogger(__name__)
    with hippoapi_client(config) as client:
        events = event_queue_pending_events.sync(client=client)

        if events:
            _process_hippoapi_events(filter_events(events,
                                                   event_type=event_type,
                                                   event_id=event_id),
                                     client,
                                     config,
                                     post_back=post_back)
        else:
            logger.warning(f'Got no events to process.')


def _process_hippoapi_events(events: Iterable[QueuedEventModel],
                             client: AuthenticatedClient,
                             config: HippoConfig,
                             post_back: bool = False):
    logger = logging.getLogger(__name__)

    for event in events:
        logger.info(f'Process hippoapi {event.action} id={event.id}')
        if event.status != 'Pending':
            logger.info(f'Skipping hippoapi id={event.id} because status is {event.status}')

        event_record = HippoEvent.objects(hippo_id=event.id).modify(upsert=True, #type: ignore
                                                                    set_on_insert__action=event.action, 
                                                                    set_on_insert__data=event.to_dict(),
                                                                    new=True)
        if post_back and event_record.status == 'Complete':
            logger.info(f'Event id={event.id} already marked complete, attempting postback')
            postback_event_complete(event.id, client)
            continue

        try:
            match event.action:
                case 'CreateAccount':
                    process_createaccount_event(event.data, config)
                case 'AddAccountToGroup':
                    process_addaccounttogroup_event(event.data, config)
                case 'UpdateSshKey':
                    process_updatesshkey_event(event.data, config)
        except Exception as e:
            event_record.modify(inc__n_tries=True)
            logger.error(f'Error processing event id={event.id}, n_tries={event_record.n_tries}: {e}')
            if event_record.n_tries >= config.max_tries:
                logger.warning(f'Event id={event.id} failed {event_record.n_tries}, postback Failed.')
                event_record.modify(set__status='Failed')
                postback_event_failed(event.id, client)
                
        else:
            event_record.modify(inc__n_tries=True, set__status='Complete')
            logger.info(f'Event id={event.id} completed.')
            if post_back:
                logger.info(f'Event id={event.id}: attempt postback')
                postback_event_complete(event.id, client)


def postback_event_complete(event_id: int,
                            client: AuthenticatedClient):
    update = QueuedEventUpdateModel(status='Complete', id=event_id)
    response = event_queue_update_status.sync_detailed(client=client, body=update)


def postback_event_failed(event_id: int,
                         client: AuthenticatedClient):
    update = QueuedEventUpdateModel(status='Failed', id=event_id)
    response = event_queue_update_status.sync_detailed(client=client, body=update)


def process_updatesshkey_event(event: QueuedEventDataModel,
                               config: HippoConfig):
    logger = logging.getLogger(__name__)
    hippo_account = event.accounts[0]
    sitename = config.site_aliases.get(event.cluster, event.cluster).lower()
    username = hippo_account.kerberos
    ssh_key = hippo_account.key

    logger.info(f'Process UpdateSshKey for user {username}')
    global_user = GlobalUser.objects.get(username=username)
    global_user.ssh_key = [ssh_key]
    global_user.save()

    logger.info(f'Add login-ssh access to user {username}, site {sitename}')
    add_user_access(username, 'login-ssh', sitename=sitename)


def process_createaccount_event(event: QueuedEventDataModel,
                                config: HippoConfig):

    logger = logging.getLogger(__name__)

    hippo_account = event.accounts[0]
    sitename = config.site_aliases.get(event.cluster, event.cluster).lower()
    username = hippo_account.kerberos

    logger.info(f'Process CreateAccount for site {sitename}, event: {event}')

    if not query_user_exists(username):
        logger.info(f'GlobalUser for {username} does not exist, creating.')
        global_user = GlobalUser.from_hippo(hippo_account)
        global_user.save()
        global_group = GlobalGroup(groupname=username,
                                   gid=global_user.gid,
                                   type='user')
        global_group.save()
    else:
        logger.info(f'GlobalUser for {username} exists, checking status.')
        global_user = GlobalUser.objects.get(username=username) #type: ignore
        global_group = GlobalGroup.objects.get(groupname=username)
        if global_user.status != 'active':
            logger.info(f'GlobalUser for {username} has status {global_user.status}, setting to "active"')
            set_user_status(username, 'active', 'Activated from HiPPO')

    if query_user_exists(username, sitename=sitename):
        site_user = SiteUser.objects.get(username=username, sitename=sitename) #type: ignore
        logger.info(f'SiteUser for {username} exists, checking status.')
        if site_user.status != 'active':
            logger.info(f'SiteUser for {username}, site {sitename} has status {site_user.status}, setting to "active"')
            set_user_status(username, 'active', 'Activated from HiPPO', sitename=sitename)
    else:
        logger.info(f'SiteUser for user {username}, site {sitename} does not exist, creating.')
        site_user = SiteUser(username=username,
                             sitename=sitename,
                             parent=global_user,
                             _access=hippo_to_cheeto_access(hippo_account.access_types) | {'slurm'}) #type: ignore
        site_user.save()
        site_group = SiteGroup(groupname=username,
                               sitename=sitename,
                               parent=global_group,
                               _members=[site_user])
        site_group.save()

    try:
        create_home_storage(sitename, global_user)
    except Exception:
        logger.error(f'{_ctx_name()}: error creating home storage for {username} on site {sitename}')

    for group in event.groups:
        add_group_member(sitename, username, group.name)

    if any((group.name == 'sponsors' for group in event.groups)):
        create_group_from_sponsor(site_user)


def process_addaccounttogroup_event(event: QueuedEventDataModel,
                                    config: HippoConfig):
    logger = logging.getLogger(__name__)
    hippo_account = event.accounts[0]
    sitename = config.site_aliases.get(event.cluster, event.cluster).lower()
    logger.info(f'Process AddAccountToGroup for site {sitename}, event: {event}')

    for group in event.groups:
        add_group_member(sitename, hippo_account.kerberos, group.name)

    if any((group.name == 'sponsors' for group in event.groups)):
        site_user = SiteUser.objects.get(sitename=sitename, username=hippo_account.kerberos)
        create_group_from_sponsor(site_user)


def ldap_sync(sitename: str, config: Config, force: bool = False):
    connect_to_database(config.mongo)
    ldap_mgr = LDAPManager(config.ldap['hpccf'], pool_keepalive=15, pool_lifetime=30)
    site = Site.objects.get(sitename=sitename)

    for user in SiteUser.objects(sitename=sitename):
        ldap_sync_globaluser(user.parent, ldap_mgr, force=force)

    for group in SiteGroup.objects(sitename=sitename):
        ldap_sync_group(group, ldap_mgr, force=force)

    for user in SiteUser.objects(sitename=sitename):
        ldap_sync_siteuser(user, ldap_mgr, force=force)

    for storage in query_automap_storages(sitename, 'home'):
        host = f'{storage.host}${{HOST_SUFFIX}}'
        ldap_mgr.connection.delete(ldap_mgr.get_automount_dn(storage.owner,
                                                             'home',
                                                             sitename))
        ldap_mgr.add_home_automount(storage.owner,
                                    sitename,
                                    host,
                                    storage.host_path,
                                    '-' + ','.join(storage.mount_options))

    for storage in query_automap_storages(sitename, 'group'):
        host = f'{storage.host}${{HOST_SUFFIX}}'
        ldap_mgr.connection.delete(ldap_mgr.get_automount_dn(storage.name,
                                                             'group',
                                                             sitename))
        ldap_mgr.add_group_automount(storage.name,
                                     sitename,
                                     host,
                                     storage.host_path,
                                     '-' + ','.join(storage.mount_options))


def ldap_sync_group(group: SiteGroup, mgr: LDAPManager, force: bool = False):
    logger = logging.getLogger(__name__)
    if not force and (group.ldap_synced and group.parent.ldap_synced):
        logger.info(f'{_ctx_name()}: Group {group.groupname} does not need to be synced')
        return

    if force:
        logger.info(f'{_ctx_name()}: force sync {group.groupname}, deleting existing dn')
        mgr.delete_dn(mgr.get_group_dn(group.groupname, group.sitename))
    
    logger.info(f'{_ctx_name()}: sync {group.groupname}') 

    if not mgr.group_exists(group.groupname, group.sitename):
        mgr.add_group(LDAPGroup.load(dict(groupname=group.groupname,
                                          gid=group.parent.gid,
                                          members=group.members)),
                      group.sitename)
        return

    special_groups = set(mgr.config.user_access_groups.values()) | set(mgr.config.user_status_groups.values())
    if group.groupname in special_groups:
        logger.info(f'{_ctx_name()}: Skip sync for special group {group.groupname}')
        return

    ldap_group = mgr.query_group(group.groupname, group.sitename)
    to_remove = ldap_group.members - group.members
    to_add = group.members - ldap_group.members

    mgr.remove_users_from_group(to_remove, group.groupname, group.sitename)
    mgr.add_user_to_group(to_add, group.groupname, group.sitename)

    group.ldap_synced = True
    group.parent.ldap_synced = True

    group.save()
    group.parent.save()


def ldap_sync_globaluser(user: GlobalUser, mgr: LDAPManager, force: bool = False):
    logger = logging.getLogger(__name__)
    if not force and user.ldap_synced:
        logger.info(f'{_ctx_name()}: GlobalUser {user.username} does not need to be synced.')
        return
    logger.info(f'{_ctx_name()}: sync {user.username}')

    if force:
        logger.info(f'{_ctx_name()}: force sync {user.username}, deleting existing dn')
        mgr.delete_user(user.username)

    data = dict(email=user.email,
                uid=user.uid,
                gid=user.gid,
                shell=user.shell,
                fullname=user.fullname,
                surname=user.fullname.split()[-1]) #type: ignore

    if user.ssh_key:
        data['ssh_keys'] = user.ssh_key
    
    try:
        if mgr.user_exists(user.username):
            mgr.update_user(user.username, **data)
        else:
            data['username'] = user.username
            mgr.add_user(LDAPUser.load(data))
    except LDAPCommitFailed as e:
        logger.error(f'{_ctx_name()}: Failed to sync GlobalUser {user.username}: {e}')
    else:
        user.ldap_synced = True
        user.save()


def ldap_sync_siteuser(user: SiteUser, mgr: LDAPManager, force: bool = False):
    logger = logging.getLogger(__name__)
    if not force and (user.ldap_synced and user.parent.ldap_synced):
        logger.info(f'{_ctx_name()}: SiteUser {user.username} does not need to be synced.')
        return

    if not mgr.user_exists(user.username):
        ldap_sync_globaluser(user.parent, mgr, force=force)

    ldap_groups = mgr.query_user_memberships(user.username, user.sitename)

    for status, groupname in mgr.config.user_status_groups.items():
        if status == user.status and groupname not in ldap_groups:
            logger.info(f'{_ctx_name()}: add status {status} for {user.username}')
            mgr.add_user_to_group(user.username, groupname, user.sitename)
        if status != user.status and groupname in ldap_groups:
            logger.info(f'{_ctx_name()}: remove status {status} for {user.username}')
            mgr.remove_users_from_group([user.username], groupname, user.sitename)

    for access, groupname in mgr.config.user_access_groups.items():
        if access in user.access and groupname not in ldap_groups:
            logger.info(f'{_ctx_name()}: add access {access} for {user.username}')
            mgr.add_user_to_group(user.username, groupname, user.sitename)
        if access not in user.access and groupname in ldap_groups:
            logger.info(f'{_ctx_name()}: remove access {access} for {user.username}')
            mgr.remove_users_from_group([user.username], groupname, user.sitename)

    if user.type == 'system':
        keys = query_admin_keys(sitename=user.sitename)
        mgr.update_user(user.username, ssh_keys=keys)

    user.ldap_synced = True
    user.save()
