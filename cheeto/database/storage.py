#!/usr/bin/env python
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023-2025
# (c) The Regents of the University of California, Davis, 2023-2025
# File   : database/storage.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 29.01.2025

from pathlib import Path

from mongoengine import (BooleanField,
                         Document,
                         ListField,
                         GenericReferenceField,
                         ReferenceField,
                         StringField,
                         CASCADE)

from ..types import MOUNT_OPTS
from ..yaml import dumps as dumps_yaml



from .base import BaseDocument
from .fields import (DataQuotaField,
                     )
from .user import GlobalUser
from .group import GlobalGroup


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

    def _pretty(self, *args, **kwargs):
        formatters = {'owner': '{data.username}',
                      'group': '{data.groupname}'}
        if 'formatters' in kwargs and kwargs['formatters'] is not None:
            formatters.update(kwargs['formatters'])
        kwargs['formatters'] = formatters
        return super()._pretty(*args, **kwargs)


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
        raise ValueError(f'{option} not a valid mount option')


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
    tablename = StringField(required=True,
                            unique_with=['sitename', 'prefix'])


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
            return list(self._options)

        options = set(self.map._options)
        if self._remove_options:
            options = options - set(self._remove_options)
        if self._add_options:
            options = options | set(self._add_options)

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
    def sitename(self):
        return self.source.sitename

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

    def pretty(self) -> str:
        data = {}
        data['name'] = self.name
        source = {}
        source['name'] = self.source.name
        source['site'] = self.sitename
        source['type'] = type(self.source).__name__
        source['owner'] = self.owner
        source['group'] = self.group
        if self.quota:
            source['quota'] = self.quota
        source['host'] = self.host
        source['host_path'] = str(self.host_path)
        data['source'] = source
        mount = {}
        mount['name'] = self.mount.name
        mount['site'] = self.mount.sitename
        mount['path'] = str(self.mount_path)
        mount['options'] = self.mount_options
        data['mount'] = mount
        return dumps_yaml(data)
