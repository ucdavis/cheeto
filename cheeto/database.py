#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023-2024
# (c) The Regents of the University of California, Davis, 2023-2024
# File   : database.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 09.08.2024

import argparse
from dataclasses import field
import logging
from pathlib import Path
from typing import List, Mapping, Optional, Set, Tuple, TypedDict, Union

from marshmallow import post_load
from marshmallow_dataclass import dataclass
from pymongo import MongoClient, ASCENDING
from pymongo.collection import Collection
from pymongo.database import Database as Database
from rich import print

from cheeto.yaml import puppet_merge

from .args import subcommand
from .config import MongoConfig
from .puppet import PuppetGroupRecord, PuppetUserRecord, SlurmRecord, add_repo_args, SiteData
from .types import (DEFAULT_SHELL, BaseModel, Date, KerberosID, LinuxGID, LinuxUID, 
                    SEQUENCE_FIELDS, is_listlike, Shell, UserType)
from .utils import require_kwargs, __pkg_dir__
from cheeto import puppet


@dataclass(frozen=True)
class GlobalUserRecord(BaseModel):
    username: KerberosID
    email: str
    uid: LinuxUID
    gid: LinuxGID
    fullname: str
    shell: Shell
    home_directory: str
    type: UserType

    _id: Optional[str] = None

    @post_load
    def _set_id(self, in_data, **kwargs):
        in_data['_id'] = in_data['username']
        return in_data

    @classmethod
    def from_puppet(cls, username: str, puppet_record: PuppetUserRecord):
        if puppet_record.groups is not None and 'hpccfgrp' in puppet_record.groups: #type: ignore
            usertype = 'admin'
        elif puppet_record.uid > 3000000000:
            usertype = 'system'
        else:
            usertype = 'user'

        return cls.Schema().load(dict(
            username = username,
            email = puppet_record.email,
            uid = puppet_record.uid,
            gid = puppet_record.gid,
            fullname = puppet_record.fullname,
            shell = puppet_record.shell if puppet_record.shell else DEFAULT_SHELL,
            home_directory = f'/home/{username}',
            type = usertype
        ))


@dataclass(frozen=True)
class SiteUserRecord(BaseModel):
    username: KerberosID
    expiry: Optional[Date] = None
    slurm: Optional[SlurmRecord] = None
    tag: Optional[Set[str]] = None

    _id: Optional[str] = None

    @post_load
    def _set_id(self, in_data, **kwargs):
        in_data['_id'] = in_data['username']
        return in_data

    @classmethod
    def from_puppet(cls, username: str, puppet_record: PuppetUserRecord):
        return cls.Schema().load(dict(
            username = username,
            expiry = puppet_record.expiry,
            slurm = puppet_record.slurm,
            tag = puppet_record.tag
        ))


@dataclass(frozen=True)
class FullUserRecord(BaseModel):
    username: KerberosID
    sitename: str
    email: str
    uid: LinuxUID
    gid: LinuxGID
    fullname: str
    shell: Shell
    home_directory: str
    type: UserType
    expiry: Optional[Date] = None
    slurm: Optional[SlurmRecord] = None
    tag: Optional[Set[str]] = None

    _id: Optional[str] = None

    @post_load
    def _set_id(self, in_data, **kwargs):
        in_data['_id'] = in_data['username']
        return in_data


@dataclass(frozen=True)
class GlobalGroupRecord(BaseModel):
    groupname: KerberosID
    gid: LinuxGID

    _id: Optional[str] = None

    @post_load
    def _set_id(self, in_data, **kwargs):
        in_data['_id'] = in_data['groupname']
        return in_data

    @classmethod
    def from_puppet(cls, groupname: str, puppet_record: PuppetGroupRecord):
        return cls.Schema().load(dict(
            groupname = groupname,
            gid = puppet_record.gid
        ))


@dataclass(frozen=True)
class SiteGroupRecord(BaseModel):
    groupname: KerberosID
    members: Optional[Set[KerberosID]] = field(default_factory=set)
    sponsors: Optional[Set[KerberosID]] = field(default_factory=set)
    slurm: Optional[SlurmRecord] = None

    _id: Optional[str] = None

    @post_load
    def _set_id(self, in_data, **kwargs):
        in_data['_id'] = in_data['groupname']
        return in_data

    @classmethod
    def from_puppet(cls, groupname: str, puppet_record: PuppetGroupRecord):
        return cls.Schema().load(dict(
            groupname = groupname,
            sponsors = puppet_record.sponsors,
            slurm = puppet_record.slurm.to_dict() if puppet_record.slurm is not None else None
        ))


@dataclass(frozen=True)
class SiteRecord(BaseModel):
    sitename: str
    fqdn: str
    users: List[SiteUserRecord]
    groups: List[SiteGroupRecord]

    _id: Optional[str] = None

    @post_load
    def _set_id(self, in_data, **kwargs):
        in_data['_id'] = in_data['sitename']
        return in_data


@dataclass(frozen=True)
class Collections(BaseModel):
    users: List[GlobalUserRecord]
    groups: List[GlobalGroupRecord]
    sites: List[SiteRecord]


def get_database(config: MongoConfig):
    client = MongoClient(config.uri,
                         username=config.user,
                         password=config.password)
    return client[config.database]


def bootstrap_database(db: Database, bootstrap_yaml: Optional[Path] = None):
    logger = logging.getLogger(__name__)
    if bootstrap_yaml is None:
        bootstrap_yaml = __pkg_dir__ / 'templates' / 'mongodb' / 'bootstrap.yaml'
    data = Collections.load_yaml(bootstrap_yaml)
    print(f'Loaded', data)

    sites = db['sites']
    for site in data.sites:
        add_site(db, site)
    sites.create_index('sitename', unique=True)
    sites.create_index('users.username')
    sites.create_index('groups.groupname')
    sites.create_index({'users.tag': 'text', 'groups.sponsors': 'text', 'groups.members': 'text'})

    users = db['users']
    for user in data.users:
        users.insert_one(user.to_dict())
    users.create_index('username', unique=True)
    users.create_index({'fullname': 'text',
                        'email': 'text', 'type': 'text', 'home_directory': 'text'})

    groups = db['groups']
    for group in data.groups:
        groups.insert_one(group.to_dict())
    groups.create_index('groupname', unique=True)
    

def add_site(db: Database, site: SiteRecord):
    sites = db['sites']
    sites.insert_one(site.to_dict())


def upsert_global_user(db: Database, user: GlobalUserRecord):
    db.users.update_one({'username': user.username},
                        {'$set': user.to_dict()},
                        upsert=True)


def query_global_user(db: Database, username: str):
    result = db.users.find_one({'username': username})
    if result is not None:
        return GlobalUserRecord.load(result)
    return result


def upsert_site_user(db: Database, sitename: str, user: SiteUserRecord):
    result = db.sites.update_one({'sitename': sitename, 'users': {'$elemMatch': {'username': user.username}}},
                                 {'$set': { 'users.$': user.to_dict() }})
    if result.modified_count == 0:
        db.sites.update_one({'sitename': sitename}, 
                            {'$addToSet': {'users': user.to_dict()}},
                            upsert=True)


def _query_site_user_q(sitename: str, username: str):
    return ({'sitename': sitename}, 
            {'users': {'$filter': 
                      {'input': '$users', 
                       'as': 'user', 
                       'cond': {'$eq': ['$$user.username', username] }}}})


def query_site_user(db: Database, sitename: str, username: str,
                    full=True):
    query, projection = _query_site_user_q(sitename, username)
    result = db.sites.find_one(query, projection=projection)
    if result is None or not result['users']:
        return None

    site_user = SiteUserRecord.load(result['users'].pop())

    if full:
        global_user = query_global_user(db, username)
        return FullUserRecord.load(puppet_merge(global_user.to_dict(),
                                                site_user.to_dict(), 
                                                {'sitename': sitename}))
    else:
        return site_user


def upsert_global_group(db: Database, group: GlobalGroupRecord):
    db.groups.update_one({'groupname': group.groupname},
                         {'$set': group.to_dict()},
                         upsert=True)


def upsert_site_group(db: Database, sitename: str, group: SiteGroupRecord):
    result = db.sites.update_one({'sitename': sitename,
                                  'groups': {'$elemMatch': {'groupname': group.groupname}}},
                                 {'$set': { 'groups.$': group.to_dict() }})
    if result.modified_count == 0:
        db.sites.update_one({'sitename': sitename}, 
                            {'$addToSet': {'groups': group.to_dict()}},
                            upsert=True)


def add_load_args(parser: argparse.ArgumentParser):
    parser.add_argument('--sitename', required=True)


@subcommand('load', add_repo_args, add_load_args)
def load(args: argparse.Namespace):
    logger = logging.getLogger(__name__)
    
    site_data = SiteData(args.site_dir,
                         common_root=args.global_dir,
                         key_dir=args.key_dir,
                         load=False)
    db = get_database(args.config.mongo)

    with site_data.lock(args.timeout):
        site_data.load()

        for group_name, group_record in site_data.iter_groups():
            global_record = GlobalGroupRecord.from_puppet(group_name, group_record)
            upsert_global_group(db, global_record) #type: ignore

            site_record = SiteGroupRecord.from_puppet(group_name, group_record)
            print(site_record)
            upsert_site_group(db, args.sitename, site_record) #type: ignore

        for user_name, user_record in site_data.iter_users():
            global_record = GlobalUserRecord.from_puppet(user_name, user_record)
            upsert_global_user(db, global_record) #type: ignore

            site_record = SiteUserRecord.from_puppet(user_name, user_record)
            print(site_record)
            upsert_site_user(db, args.sitename, site_record) #type: ignore


def add_query_args(parser: argparse.ArgumentParser):
    parser.add_argument('--in', dest='val_in', nargs='+', action='append')
    parser.add_argument('--not-in', dest='val_not_in', nargs='+', action='append')
    parser.add_argument('--search', '-s', nargs='+', action='append')
    parser.add_argument('--all', default=False, action='store_true')


def build_query(args: argparse.Namespace):
    query = {}
    if args.search:
        for query_group in args.search:
            query['$text'] = {'$search': ' '.join(query_group)}
    else:
        if args.val_in:
            for query_group in args.val_in:
                #if query_group[0] not in record_type.field_names():
                #    raise ValueError(f'Query key must be one of: {record_type.field_names()}')
                query[query_group[0]] = {'$in': query_group[1:]}
        if args.val_not_in:
            for query_group in args.val_not_in:
                #if query_group[0] not in record_type.field_names():
                #    raise ValueError(f'Query key must be one of: {record_type.field_names()}')
                query[query_group[0]] = {'$nin': query_group[1:]}
    return query



def add_query_user_args(parser: argparse.ArgumentParser):
    parser.add_argument('--site')


@subcommand('users', add_query_args)
def query_users(args: argparse.Namespace):
    db = get_database(args.config.mongo)

    if args.all:
        search = db.users.find()
        for record in search:
            record = GlobalUserRecord.Schema().load(record)
            print(record)
    else:
        query = build_query(args)
        search = db.users.find(query)
        for record in search:
            record = GlobalUserRecord.Schema().load(record)
            print(record)
        
