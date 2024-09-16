#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023-2024
# (c) The Regents of the University of California, Davis, 2023-2024
# File   : puppet.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 19.07.2024

from dataclasses import field
import logging
import os
from typing import List, Mapping, Optional, Set, Tuple, Union

from ldap3 import (AttrDef, Connection, Entry, ObjectDef, Reader, Server, ServerPool,
                   Writer, set_config_parameter, get_config_parameter)
from ldap3 import ALL_ATTRIBUTES, BASE, FIRST, SYNC
from ldap3.abstract import STATUS_PENDING_CHANGES
from ldap3.abstract.entry import Entry
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
    username: KerberosID
    email: str
    uid: LinuxUID
    gid: LinuxGID
    fullname: str
    surname: str
    home_directory: str = field(default='')
    shell: str = field(default=DEFAULT_SHELL)
    
    ssh_keys: Optional[List[str]] = None
    password: Optional[str] = None
    dn: Optional[str] = None

    @post_load
    def default_home_directory(self, in_data, **kwargs):
        if not in_data['home_directory']:
            in_data['home_directory'] = os.path.join('/home', in_data['username'])
        return in_data


@require_kwargs
@dataclass(frozen=True)
class LDAPGroup(LDAPRecord):
    groupname: KerberosID
    gid: LinuxGID
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

    def __init__(self, 
                 config: LDAPConfig,
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
        self.logger = logging.getLogger(self.__class__.__name__)

    def get_user_dn(self, username: str):
        return f'uid={escape_rdn(username)},{self.config.user_base}'

    def get_group_dn(self, groupname: str, sitename: str):
        return self.searchbase(('cn', groupname),
                               ('ou', 'groups'),
                               ('ou', sitename))

    def get_automount_dn(self, mountname: str, mapname: str, sitename: str) -> str:
        return self.searchbase(('automountKey', mountname),
                               ('automountMapName', f'auto.{mapname}'),
                               ('ou', 'automount'),
                               ('ou', sitename))

    def dn_reader(self,
                  dn: str,
                  object_def: ObjectDef):
    
        return Reader(self.connection,
                      object_def,
                      dn)

    def dn_exists(self,
                  dn: str) -> bool:
        return self.connection.search(dn, '(objectClass=*)', BASE)

    def searchbase(self,
                   *prefixes: Tuple[str, str]) -> str:

        prefix = ','.join((f'{k}={escape_rdn(v)}' for k, v in prefixes))
        if prefix:
            return f'{prefix},{self.config.searchbase}'
        else:
            return self.config.searchbase

    def _search_user(self,
                     uids: List[str],
                     attributes: List = [ALL_ATTRIBUTES]):

        safe_uids = [escape_rdn(uid) for uid in uids]
        if len(safe_uids) == 1:
            query = f'(uid={safe_uids[0]})'
        else:
            uids_query = ''.join((f'(uid={uid})' for uid in safe_uids))
            query = f'(|{uids_query})'
        status = self.connection.search(self.config.searchbase,
                                        query,
                                        attributes=attributes)
        return status, self.connection.response

    @property
    def user_def(self):
        odef = ObjectDef(self.config.user_classes, self.connection)
        return odef

    def user_reader(self,
                    query: str = '',
                    object_def: Optional[ObjectDef] = None) -> Reader:

        return Reader(self.connection,
                      self.user_def if object_def is None else object_def,
                      self.config.user_base,
                      query=query)

    def _userid_query(self,
                      username: Union[List[str], str]) -> str:

        if not is_listlike(username):
            safe = escape_rdn(username) #type: ignore
        else:
            safe = '; '.join((escape_rdn(u) for u in username))
        return f'uid: {safe}'

    def user_exists(self,
                    username: str) -> bool:

        return self.dn_exists(self.get_user_dn(username))

    def _query_user(self, 
                    username: Union[List[str], str]) -> Reader:

        query = self._userid_query(username)
        reader = self.user_reader(query=query)
        reader.search()
        return reader

    def query_user(self, 
                   username: Union[List[str], str]) -> List[LDAPUser]:

        cursor = self._query_user(username)
        if cursor.entries:
            return [LDAPUser.from_entry(entry, self.config.user_attrs) for entry in cursor.entries] #type: ignore
        else:
            return []

    def add_user(self, 
                 user: LDAPUser):

        reader = self._query_user(user.username)
        writer = Writer.from_cursor(reader)
        
        dn = self.get_user_dn(user.username) \
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
            raise LDAPCommitFailed(f'Failed to add user {user.username}.')
        self.logger.info(f'add_user: {entry}')
        return entry

    def update_user(self,
                    username: str,
                    **attrs):
        reader = self.dn_reader(self.get_user_dn(username), self.user_def)
        cursor = reader.search()
        if len(cursor) != 1:
            raise LDAPCommitFailed(f'Should have gotten a single cursor result, got {len(cursor)}')
        entry = cursor[0].entry_writable()

        for from_key, value in attrs.items():
            to_key = self.config.user_attrs[from_key]
            entry[to_key] = value

        status = entry.entry_commit_changes()
        if status is False and entry.entry_status == STATUS_PENDING_CHANGES:
            raise LDAPCommitFailed(f'Failed to update user {username}')
        self.logger.info(f'update_user: {entry}')
        return entry
        
    def delete_user(self, 
                    username: str):

        dn = self.get_user_dn(username)
        self.logger.info(f'delete_user: {dn}')
        self.connection.delete(dn)

    def delete_dn(self,
                  dn: str):
        self.logger.info(f'delete_dn: {dn}')
        self.connection.delete(dn)

    @property
    def group_def(self):
        return ObjectDef(self.config.group_classes, self.connection)

    def group_reader(self, 
                     sitename: str,
                     query: str = '',  
                     object_def: Optional[ObjectDef] = None) -> Reader:

        searchbase = self.searchbase(('ou', escape_rdn(sitename)))
        return Reader(self.connection,
                      self.group_def if object_def is None else object_def,
                      searchbase,
                      query=query)

    def _query_group(self, 
                     groupname: str, 
                     sitename: str,
                     object_def: Optional[ObjectDef] = None) -> Reader:

        safe_gid = escape_rdn(groupname)
        query = f'cn: {safe_gid}'
        reader = self.group_reader(sitename, query=query, object_def=object_def)
        reader.search()
        return reader

    def query_group(self,
                    groupname: str,
                    sitename: str) -> Optional[LDAPGroup]:

        cursor = self._query_group(groupname, sitename)
        if cursor.entries:
            return LDAPGroup.from_entry(cursor.entries[0], self.config.group_attrs) #type: ignore
        else:
            return None

    def query_user_memberships(self,
                               username: str,
                               sitename: str) -> set[str]:

        safe_username = escape_rdn(username)
        query = f'memberUid: {safe_username}'
        group_def = ObjectDef(self.config.group_classes)
        group_def += AttrDef('cn')
        group_def += AttrDef('memberUid')
        reader = self.group_reader(sitename, query=query, object_def=group_def)

        return {g.cn.value for g in reader.search()}

    def group_exists(self, 
                     groupname: str,
                     sitename: str) -> bool:

        return self.dn_exists(self.get_group_dn(groupname, sitename))

    def add_user_to_group(self, 
                          username: Union[List[str], str], 
                          groupname: str,
                          sitename: str, 
                          verify_user: bool = True):

        cursor = self._query_group(groupname, sitename)
        if len(cursor.entries) > 1:
            raise LDAPExpectedSingleResult()
        usernames = [username] if not is_listlike(username) else username
        if not len(usernames):
            return None
        if not cursor.entries:
            return None
        if verify_user:
            for username in usernames:
                if not self.user_exists(username):
                    raise LDAPInvalidUser(f'User {username} does not exist in LDAP tree.')

        group_entry = cursor.entries[0]
        group_entry = group_entry.entry_writable()
        
        for username in usernames:
            if username in group_entry.memberUid:
                self.logger.info(f'add_user_to_group: {username} already in {groupname} on {sitename}')
            else:
                group_entry.memberUid += escape_rdn(username)
        status = group_entry.entry_commit_changes()
        if status is False and group_entry.entry_status == STATUS_PENDING_CHANGES:
            raise LDAPCommitFailed(f'Could not add {usernames} to group {groupname}: {group_entry}')
        self.logger.info(f'add_user_to_group: {usernames} in {groupname} on {sitename}')
        return group_entry

    def remove_users_from_group(self, 
                                usernames: List[str], 
                                groupname: str,
                                sitename: str):

        if not len(usernames):
            return None
        cursor = self._query_group(groupname, sitename)
        if len(cursor.entries) > 1:
            raise LDAPExpectedSingleResult()
        if not cursor.entries:
            return None
        group_entry = cursor.entries[0].entry_writable()
        try:
            group_entry.memberUid -= usernames
        except Exception as e:
            self.logger.error(f'Failed to remove {usernames} from {group_entry.dn}: {e}')
        status = group_entry.entry_commit_changes()
        if status is False and group_entry.entry_status == STATUS_PENDING_CHANGES:
            raise LDAPCommitFailed(f'Could not remove {usernames} from {group_entry.entry_dn}: {group_entry}')
        self.logger.info(f'remove_users_from_group: {usernames} from {groupname} on {sitename}')
        return group_entry

    def add_group(self, 
                  group: LDAPGroup, 
                  sitename: str):

        reader = self._query_group(group.groupname, sitename)
        writer = Writer.from_cursor(reader)

        dn = self.get_group_dn(group.groupname, sitename) \
             if group.dn is None \
             else group.dn
        entry = writer.new(dn)
        entry.cn = group.groupname
        for from_key, to_key in self.config.group_attrs.items():
            value = getattr(group, from_key)
            if value is not None and not (is_listlike(value) and len(value) == 0):
                entry[to_key] = value
        status = entry.entry_commit_changes()
        if status is False and entry.entry_status == STATUS_PENDING_CHANGES:
            self.logger.error(f'Failed to commit changes: {entry}')
            raise LDAPCommitFailed(f'Failed to add group: {dn}.')
        self.logger.info(f'add_group: {entry}')
        return entry

    @property
    def automount_def(self):
        return ObjectDef(['autoMount'], self.connection)

    def automount_reader(self,
                         sitename: str,
                         query: str = '',
                         object_def: Optional[ObjectDef] = None):

        searchbase = self.searchbase(('ou', 'automount'), ('ou', escape_rdn(sitename)))

        return Reader(self.connection,
                      self.automount_def if object_def is None else object_def,
                      searchbase,
                      query=query)

    def automount_exists(self,
                         mountname: str,
                         mapname: str,
                         sitename: str) -> bool:
        return self.dn_exists(self.get_automount_dn(mountname, mapname, sitename))

    def add_automount(self,
                      mountname: str,
                      mapname: str,
                      sitename: str,
                      mount_host: str,
                      mount_path: str,
                      mount_options: str) -> bool:

        dn = self.get_automount_dn(mountname, mapname, sitename)
        reader = self.dn_reader(dn, self.automount_def)
        if reader.search():
            return False

        writer = Writer.from_cursor(reader)
        entry = writer.new(dn)
        entry.automountKey = mountname
        entry.automountInformation = f'{mount_options} {mount_host}:{mount_path}'
        #entry.automountInformation = f'in_network(192.168.0.0/16);type:=nfs;rhost:={mount_host}-ib;rfs:={mount_path} in_network(10.0.0.0/8);type:=nfs;rhost:={mount_host};rfs:={mount_path}'
        
        status = entry.entry_commit_changes()
        if status is False and entry.entry_status == STATUS_PENDING_CHANGES:
            self.logger.error(f'add_automount: failed to commit {entry}')
            raise LDAPCommitFailed(f'failed to commit {dn}')

        self.logger.info(f'add_automount: {entry}')
        return True

    def add_home_automount(self,
                           username: str,
                           sitename: str,
                           mount_host: str,
                           mount_path: str,
                           mount_options: str) -> bool:

        return self.add_automount(username,
                                  'home', 
                                  sitename, 
                                  mount_host, 
                                  mount_path, 
                                  mount_options)

    def add_group_automount(self,
                            storagename: str,
                            sitename: str,
                            mount_host: str,
                            mount_path: str,
                            mount_options: str) -> bool:

        return self.add_automount(storagename,
                                  'group',
                                  sitename,
                                  mount_host,
                                  mount_path,
                                  mount_options)

        
    def update_home_automount(self,
                              username: str,
                              sitename: str,
                              mount_host: str,
                              mount_path: str,
                              mount_options: str) -> bool:

        dn = self.get_automount_dn(username, 'home', sitename) 
        reader = self.dn_reader(dn, self.automount_def)
        if not reader.search():
            return False

