#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023-2024
# (c) The Regents of the University of California, Davis, 2023-2024
# File   : puppet.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 19.07.2024

from dataclasses import field
import os
from typing import List, Mapping, Optional, Set, Tuple, Union

from ldap3 import (AttrDef, Connection, Entry, ObjectDef, Reader, Server, ServerPool,
                   Writer)
from ldap3 import ALL_ATTRIBUTES, FIRST, ROUND_ROBIN, SAFE_SYNC, MOCK_SYNC, SYNC
from ldap3.utils.dn import safe_dn, escape_rdn
from marshmallow import post_load
from marshmallow_dataclass import dataclass

from .config import LDAPConfig
from .types import DEFAULT_SHELL, BaseModel, KerberosID, LinuxGID, LinuxUID, SEQUENCE_FIELDS, is_listlike
from .utils import require_kwargs


def sort_on_attr(entries, attr: str = 'uid'):
    entries.sort(key=lambda e: getattr(e, attr).value)


@dataclass(frozen=True)
class LDAPRecord(BaseModel):

    @classmethod
    def entry_getter(cls, to_key, from_key, entry):
        to_field = cls.Schema().fields[to_key]
        if type(to_field) in SEQUENCE_FIELDS:
            return entry[from_key].values
        else:
            return entry[from_key].value

    @classmethod
    def from_entry(cls, entry: Entry, attrs: Mapping[str, str]):
        return cls.Schema().load(
            {**{to_key: cls.entry_getter(to_key, from_key, entry) for to_key, from_key in attrs.items()},
             'dn': entry.entry_dn}
        )


@require_kwargs
@dataclass(frozen=True)
class LDAPUser(LDAPRecord):
    uid: KerberosID
    email: str
    uid_number: LinuxUID
    gid_number: LinuxGID
    fullname: str
    surname: str
    home_directory: str = field(default='')
    shell: str = field(default=DEFAULT_SHELL)

    dn: Optional[str] = None

    @post_load
    def default_home_directory(self, in_data, **kwargs):
        if not in_data['home_directory']:
            in_data['home_directory'] = os.path.join('/home', in_data['uid'])
        return in_data


@require_kwargs
@dataclass(frozen=True)
class LDAPGroup(LDAPRecord):
    gid: KerberosID
    gid_number: LinuxGID
    members: Optional[Set[KerberosID]] = field(default_factory=set)

    dn: Optional[str] = None


class LDAPExpectedSingleResult(Exception):
    pass


class LDAPExpectedMultipleResults(Exception):
    pass


class LDAPCommitFailed(Exception):
    pass

class LDAPInvalidUser(Exception):
    pass


class LDAPManager:

    def __init__(self, config: LDAPConfig,
                       servers: Optional[List[Server]] = None,  
                       strategy: Optional[str] = SYNC,
                       auto_bind: bool = True,
                       **connection_kwargs):

        if servers is None:
            self.servers = [Server(uri, get_info='ALL') for uri in config.servers] #type: ignore
        else:
            self.servers = servers

        self.config = config
        self.login_dn = safe_dn(config.login_dn) if config.login_dn is not None else None
        self.pool = ServerPool(self.servers, FIRST) #, active=True, exhaust=True)
        self.connection = Connection(self.servers,
                                     user=self.login_dn,
                                     password=config.password,
                                     client_strategy=strategy, #type: ignore
                                     auto_bind=auto_bind,
                                     **connection_kwargs)

    def searchbase(self, *prefixes: Tuple[str, str]) -> str:
        prefix = ','.join((f'{k}={escape_rdn(v)}' for k, v in prefixes))
        if prefix:
            return f'{prefix},{self.config.searchbase}'
        else:
            return self.config.searchbase

    def _search_user(self, uids: List[str], attributes: List = [ALL_ATTRIBUTES]):
        safe_uids = [escape_rdn(uid) for uid in uids]
        if len(safe_uids) == 1:
            query = f'(uid={safe_uids[0]})'
        else:
            uids_query = ''.join((f'(uid={uid})' for uid in safe_uids))
            query = f'(|{uids_query})'
        status = self.connection.search(self.config.searchbase, query,
                                        attributes=attributes)
        return status, self.connection.response

    @property
    def user_def(self):
        return ObjectDef(self.config.user_classes, self.connection)

    def user_reader(self, *ous: str, query: str = '', object_def: Optional[ObjectDef] = None) -> Reader:
        searchbase = self.searchbase(*(('ou', escape_rdn(ou)) for ou in ous))
        return Reader(self.connection,
                      self.user_def if object_def is None else object_def,
                      searchbase,
                      query=query)

    def _userid_query(self, uid: Union[List[str], str]) -> str:
        if not is_listlike(uid):
            safe = escape_rdn(uid) #type: ignore
        else:
            safe = '; '.join((escape_rdn(u) for u in uid))
        return f'userId: {safe}'

    def verify_user(self, uid: str) -> bool:
        user_def = ObjectDef(self.config.user_classes)
        user_def.add_attribute(AttrDef('uid'))
        found, _ = self._search_user([uid], attributes=[])
        return found

    def _query_user(self, uid: Union[List[str], str]) -> Reader:
        query = self._userid_query(uid)
        reader = self.user_reader(query=query)
        reader.search()
        return reader

    def query_user(self, uid: Union[List[str], str]) -> List[LDAPUser]:
        cursor = self._query_user(uid)
        if cursor.entries:
            return [LDAPUser.from_entry(entry, self.config.user_attrs) for entry in cursor.entries] #type: ignore
        else:
            return []

    def add_user(self, user: LDAPUser):
        reader = self._query_user(user.uid)
        writer = Writer.from_cursor(reader)
        
        dn = f'uid={escape_rdn(user.uid)},{self.config.user_base}' \
            if user.dn is None \
            else user.dn
        entry = writer.new(dn)
        entry.cn = user.fullname
        for from_key, to_key in self.config.user_attrs.items():
            value = getattr(user, from_key)
            if value is not None:
                entry[to_key] = value
        status = entry.entry_commit_changes()
        if status is False:
            raise LDAPCommitFailed(f'Failed to add user {user.uid}.')
        return entry

    @property
    def group_def(self):
        return ObjectDef(self.config.group_classes, self.connection)

    def group_reader(self, *ous: str, query: str = '', ) -> Reader:
        searchbase = self.searchbase(*(('ou', escape_rdn(ou)) for ou in ous))
        return Reader(self.connection, self.group_def, searchbase, query=query)

    def _query_group(self, gid: str, *ous: str) -> Reader:
        safe_gid = escape_rdn(gid)
        query = f'cn: {safe_gid}'
        reader = self.group_reader(query=query, *ous)
        reader.search()
        return reader

    def query_group(self, gid: str, *ous: str) -> Optional[LDAPGroup]:
        cursor = self._query_group(gid, *ous)
        if cursor.entries:
            return LDAPGroup.from_entry(cursor.entries[0], self.config.group_attrs) #type: ignore
        else:
            return None

    def add_user_to_group(self, uid: str, gid: str, *ous: str, verify_uid: bool = True):
        cursor = self._query_group(gid, *ous)
        if len(cursor.entries) > 1:
            raise LDAPExpectedSingleResult()
        if not cursor.entries:
            return None
        if verify_uid and not self.verify_user(uid):
            raise LDAPInvalidUser(f'User {uid} does not exist in LDAP tree.')
        group_entry = cursor.entries[0].entry_writable()
        group_entry.memberUid += escape_rdn(uid)
        status = group_entry.entry_commit_changes()
        if status is False:
            raise LDAPCommitFailed(f'Could not add {uid} to group {gid}.')
        return group_entry

    def add_group(self, group: LDAPGroup, cluster: str):
        reader = self._query_group(group.gid, cluster)
        writer = Writer.from_cursor(reader)

        dn = self.searchbase(('cn', group.gid), ('ou', 'groups'), ('ou', cluster)) \
            if group.dn is None \
            else group.dn
        entry = writer.new(dn)
        entry.cn = group.gid
        for from_key, to_key in self.config.group_attrs.items():
            value = getattr(group, from_key)
            if value is not None:
                entry[to_key] = value
        status = entry.entry_commit_changes()
        if status is False:
            raise LDAPCommitFailed(f'Failed to add group: {dn}.')
        return entry

