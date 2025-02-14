#!/usr/bin/env python
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023-2025
# (c) The Regents of the University of California, Davis, 2023-2025
# File   : database/user.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 29.01.2025

from codecs import ignore_errors
import logging
from typing import Iterable, Optional, Self, no_type_check

from mongoengine import (EmailField,
                         StringField, 
                         ListField, 
                         BooleanField, 
                         ReferenceField, 
                         DateField, 
                         CASCADE,
                         signals)

from ..hippoapi.models import QueuedEventAccountModel
from ..utils import make_ngrams
from ..puppet import PuppetUserRecord
from ..types import DEFAULT_SHELL, hippo_to_cheeto_access

from .base import BaseDocument, handler, SyncQuerySet
from .fields import (POSIXNameField,
                     POSIXIDField,
                     UserTypeField,
                     UserStatusField,
                     UserAccessField,
                     ShellField)


class GlobalUser(BaseDocument):
    username = POSIXNameField(required=True, primary_key=True)
    uid = POSIXIDField(required=True, unique=True)
    gid = POSIXIDField(required=True, unique=True)
    email = EmailField(required=True)
    fullname = StringField(required=True)
    shell = ShellField(required=True)
    home_directory = StringField(required=True)
    type = UserTypeField(required=True)
    status = UserStatusField(required=True, default='active')
    password = StringField()
    ssh_key = ListField(StringField())
    access = ListField(UserAccessField(), default=lambda: ['login-ssh'])
    comments = ListField(StringField())

    iam_has_entry = BooleanField(default=True)
    iam_id = POSIXIDField()
    colleges = ListField(StringField())

    ldap_synced = BooleanField(default=False)
    iam_synced = BooleanField(default=False)

    meta = {
        'queryset_class': SyncQuerySet,
        'indexes': [
            {
                'fields': ['$username', '$email', '$fullname'],
                'default_language': 'english'
            },
            {
                'fields': ['username', 'uid', 'gid'],
                'unique': True
            }
        ]
    }

    @no_type_check
    def full_ngrams(self):
        return make_ngrams(self.username) + \
               make_ngrams(self.fullname) + \
               make_ngrams(self.email)

    @no_type_check
    def prefix_ngrams(self):
        return make_ngrams(self.username, prefix=True) + \
               make_ngrams(self.fullname, prefix=True) + \
               make_ngrams(self.email, prefix=True)

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

    def _pretty(self, *args, **kwargs):
        if 'order' not in kwargs:
            kwargs['order'] = ['username', 'email', 'fullname', 'uid', 'gid', 'type', 'status', 'access']
        return super()._pretty(*args, **kwargs)


class UserSearch(BaseDocument):
    user = ReferenceField(GlobalUser, required=True, reverse_delete_rule=CASCADE)
    full_ngrams = StringField(required=True)
    prefix_ngrams = StringField(required=True)

    meta = {
        'indexes': [
            {
                'fields': ['$full_ngrams', '$prefix_ngrams'],
                'default_language': 'english',
                'weights': {
                    'prefix_ngrams': 200,
                    'full_ngrams': 100
                }
            }
        ]
    }

    @classmethod
    def update_index(cls, user: GlobalUser):
        UserSearch.objects(user=user).update_one(
            prefix_ngrams=' '.join(user.prefix_ngrams()),
            full_ngrams=' '.join(user.full_ngrams()),
            upsert=True
        )


global_user_t = GlobalUser | str


def handle_global_user(user: global_user_t) -> GlobalUser:
    if type(user) is str:
        return GlobalUser.objects.get(username=user)
    else:
        return user


@handler(signals.post_save)
def user_apply_globals(sender, document, **kwargs):
    from .site import Site
    logger = logging.getLogger(__name__)
    site = Site.objects().get(sitename=document.sitename)
    if site.global_groups:
        for site_group in site.global_groups:
            logger.info(f'Splat {site.sitename}: Add member {document.username} to group {site_group.groupname}')
            site_group.update(add_to_set___members=document)
    if site.global_slurmers:
        for site_group in site.global_slurmers:
            logger.info(f'Splat {site.sitename}: Add slurmer {document.username} to group {site_group.groupname}')
            site_group.update(add_to_set___slurmers=document)



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
        ],
        'queryset_class': SyncQuerySet
    }

    @property
    def email(self):
        return self.parent.email # type: ignore

    @property
    def uid(self):
        return self.parent.uid # type: ignore

    @property
    def gid(self):
        return self.parent.gid # type: ignore

    @property
    def fullname(self):
        return self.parent.fullname # type: ignore

    @property
    def shell(self):
        return self.parent.shell # type: ignore

    @property
    def home_directory(self):
        return self.parent.home_directory # type: ignore

    @property
    def password(self):
        return self.parent.password # type: ignore

    @property
    def type(self):
        return self.parent.type # type: ignore

    @property
    def status(self):
        if self.parent.status != 'active': # type: ignore
            return self.parent.status # type: ignore
        else:
            return self._status

    @property
    def ssh_key(self):
        return self.parent.ssh_key # type: ignore

    @property
    @no_type_check
    def access(self):
        return set(self._access) | set(self.parent.access)

    @property
    def comments(self):
        return self.parent.comments # type: ignore

    @classmethod
    def from_puppet(cls, username: str,
                         sitename: str,
                         parent: GlobalUser,
                         puppet_record: PuppetUserRecord) -> Self:

        puppet_data = puppet_record.to_dict()
        access = set()
        if 'tag' in puppet_data:
            tags = puppet_data['tag'] # type: ignore
            if 'root-ssh-tag' in tags:
                access.add('root-ssh')
            if 'ssh-tag' in tags:
                access.add('compute-ssh')
            if 'sudo-tag' in tags:
                access.add('sudo')
        
        return cls(
            username = username,
            sitename = sitename,
            expiry = puppet_data.get('expiry', None), # type: ignore
            _access = access,
            _status = puppet_record.status,
            parent = parent
        )

    def to_dict(self, strip_id=True, strip_empty=False, raw=False, **kwargs):
        data = super().to_dict(strip_id=strip_id, strip_empty=strip_empty, raw=raw, **kwargs)
        if not raw:
            data['parent'] = self.parent.to_dict(strip_id=strip_id, raw=raw, strip_empty=strip_empty, **kwargs) #type: ignore
        return data
    
    def _pretty(self, *args, **kwargs):
        if 'order' not in kwargs:
            kwargs['order'] = ['username', 'email', 'fullname', 'uid', 'gid', 'type', 'status', 'access']
        return super()._pretty(*args, **kwargs)


site_user_t = SiteUser | str
User = SiteUser | GlobalUser


def handle_site_user(sitename: str, user: site_user_t) -> site_user_t:
    if type(user) is str:
        return SiteUser.objects.get(sitename=sitename, username=user)
    return user


def handle_site_users(sitename: str, users: Iterable[site_user_t]):
    logger = logging.getLogger(__name__)
    to_query = []
    for user in users:
        if type(user) is SiteUser:
            yield user
        else:
            to_query.append(user)
    logger.debug(f'to_query: {to_query}')
    for user in SiteUser.objects(sitename=sitename, username__in=to_query):
        yield user


class DuplicateUser(ValueError):
    def __init__(self, username):
        super().__init__(f'User {username} already exists.')


class DuplicateGlobalUser(DuplicateUser):
    pass


class DuplicateSiteUser(DuplicateUser):
    def __init__(self, username, sitename):
        super().__init__(f'User {username} already exists in site {sitename}.')


class NonExistentGlobalUser(ValueError):
    def __init__(self, username):
        super().__init__(f'User {username} does not exist.')


class NonExistentSiteUser(ValueError):
    def __init__(self, username, sitename):
        super().__init__(f'User {username} does not exist in site {sitename}.')


class InvalidUser(RuntimeError):
    pass
