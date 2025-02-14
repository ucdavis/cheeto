#!/usr/bin/env python
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023-2025
# (c) The Regents of the University of California, Davis, 2023-2025
# File   : database/group.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 29.01.2025

from typing import Self

from mongoengine import (BooleanField,
                         EmbeddedDocument,
                         EmbeddedDocumentField, 
                         IntField,
                         ListField, 
                         ReferenceField, 
                         StringField, 
                         CASCADE, 
                         PULL)

from ..puppet import PuppetGroupRecord

from .base import BaseDocument, SyncQuerySet
from .fields import (POSIXNameField,
                     POSIXIDField,
                     GroupTypeField)
from .user import GlobalUser, SiteUser


class GlobalGroup(BaseDocument):
    groupname = POSIXNameField(required=True, primary_key=True)
    gid = POSIXIDField(required=True, unique=True)
    type = GroupTypeField(required=True, default='group')
    user = ReferenceField(GlobalUser, reverse_delete_rule=CASCADE)

    ldap_synced = BooleanField(default=False)
    iam_synced = BooleanField(default=False)

    meta = {
        'queryset_class': SyncQuerySet,
        'indexes': [
            {
                'fields': ['groupname', 'gid'],
                'unique': True
            }
        ]
    }
    
    @classmethod
    def from_puppet(cls, groupname: str, puppet_record: PuppetGroupRecord) -> Self:
        return cls(
            groupname = groupname,
            gid = puppet_record.gid
        )
    
    def _pretty(self, *args, **kwargs):
        extra = {'sites': [sg.sitename for sg in SiteGroup.objects(groupname=self.groupname)]}
        return super()._pretty(*args, **kwargs, extra=extra)


global_group_t = GlobalGroup | str


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
        ],
        'queryset_class': SyncQuerySet
    }

    @property
    def gid(self):
        return self.parent.gid

    @property
    def members(self):
        return sorted({m.username for m in self._members}) #type: ignore

    @property
    def sponsors(self):
        return sorted({s.username for s in self._sponsors}) #type: ignore

    @property
    def sudoers(self):
        return sorted({s.username for s in self._sudoers}) #type: ignore

    @property
    def slurmers(self):
        return sorted({s.username for s in self._slurmers}) #type: ignore

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
            data['parent'] = self.parent.to_dict(raw=False) #type: ignore
            data.pop('_members', None)
            if 'members' in data:
                data['members'] = self.members #type: ignore
            data.pop('_sponsors', None)
            if 'sponsors' in data:
                data['sponsors'] = self.sponsors
            data.pop('_sudoers', None)
            if 'sudoers' in data:
                data['sudoers'] = self.sudoers
            data.pop('_slurmers', None)
            if 'slurmers' in data:
                data['slurmers'] = self.slurmers
        return data
    
    def _pretty(self, *args, **kwargs):
        formatters = {'_members': '{data.username}',
                      '_sponsors': '{data.username}',
                      '_sudoers': '{data.username}',
                      '_slurmers': '{data.username}'}
        if 'formatters' in kwargs and kwargs['formatters'] is not None:
            formatters.update(kwargs['formatters'])
        kwargs['formatters'] = formatters
        return super()._pretty(*args, **kwargs)


site_group_t = SiteGroup | str


class DuplicateGroup(ValueError):
    def __init__(self, groupname):
        super().__init__(f'Group {groupname} already exists.')

class DuplicateGlobalGroup(DuplicateGroup):
    pass


class DuplicateSiteGroup(DuplicateGroup):
    def __init__(self, groupname, sitename):
        super().__init__(f'Group {groupname} already exists in site {sitename}.')


class NonExistentGroup(ValueError):
    def __init__(self, groupname):
        super().__init__(f'Group {groupname} does not exist.')


class NonExistentGlobalGroup(NonExistentGroup):
    pass

class NonExistentSiteGroup(NonExistentGroup):
    def __init__(self, groupname, sitename):
        super().__init__(f'Group {groupname} does not exist in site {sitename}.')