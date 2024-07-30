#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023-2024
# (c) The Regents of the University of California, Davis, 2023-2024
# File   : puppet.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 19.07.2024

from typing import List, Optional, Union

from ldap3 import (Connection, ObjectDef, Reader, Server, ServerPool,
                   Writer)
from ldap3 import FIRST, ROUND_ROBIN, SAFE_SYNC, MOCK_SYNC
from ldap3.utils.dn import safe_dn, escape_rdn

from .config import LDAPConfig


def sort_on_attr(entries, attr: str = 'uid'):
    entries.sort(key=lambda e: getattr(e, attr).value)


class LDAPManager:

    def __init__(self, config: LDAPConfig,
                       servers: Optional[List[Server]] = None,  
                       strategy: Optional[str] = SAFE_SYNC,
                       auto_bind: bool = True):

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
                                     auto_bind=auto_bind)

    @property
    def user_def(self):
        return ObjectDef(self.config.user_classes, self.connection)

    def user_reader(self, query: str = ''):
        return Reader(self.connection, self.user_def, self.config.searchbase, query=query)

    def query_user(self, uid: str):
        safe_uid = escape_rdn(uid)
        reader = self.user_reader(query = f'userId: {safe_uid}')
        reader.search()
        return reader

    def query_users(self, uids: List[str]):
        safe_uids = '; '.join((escape_rdn(uid) for uid in uids))
        reader = self.user_reader(query=f'userId: {safe_uids}')
        reader.search()
        return reader
