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
from dataclasses import field
import logging
from pathlib import Path
import sys
from typing import List, Mapping, Optional, Set, Tuple, TypedDict, Union

from marshmallow import post_load
from marshmallow_dataclass import dataclass
from pyasn1.codec import der
from rich.console import Console
from rich.syntax import Syntax
from pymongo import MongoClient, ASCENDING
from pymongo.collection import Collection
from pymongo.database import Database as Database
from rich import print as pprint

from cheeto.yaml import puppet_merge

from .args import subcommand
from .config import MongoConfig
from .errors import ExitCode
from .puppet import PuppetGroupRecord, PuppetUserRecord, SlurmRecord, add_repo_args, SiteData
from .types import (DEFAULT_SHELL, DISABLED_SHELLS, AccessType, BaseModel, Date, KerberosID, LinuxGID, LinuxUID, 
                    SEQUENCE_FIELDS, describe_schema, is_listlike, Shell, UserType, UserStatus)
from .utils import TIMESTAMP_NOW, require_kwargs, __pkg_dir__
from cheeto import puppet


MONGO_UPDATE_OPERATORS = {
    '$set',
    '$push',
    '$inc',
    '$addToSet'
}


class InvalidUser(RuntimeError):
    pass


@dataclass(frozen=True)
class MongoModel(BaseModel):

    @classmethod
    def validate_db_update(cls, updates: dict):
        for update_op, op_updates in updates.items():
            if update_op not in MONGO_UPDATE_OPERATORS:
                raise RuntimeError(f'{update_op} not a support Mongo update operator.')
            for field_name, new_value in op_updates.items():
                if field_name not in cls.Schema().fields:
                    raise RuntimeError(f'{field_name} not a valid field on {cls}')
                validate = cls.Schema().fields[field_name].validate
                if validate is not None:
                    validate(new_value) #type: ignore

    def __hash__(self):
        return self._id #type: ignore


@dataclass(frozen=True)
class GlobalUserRecord(MongoModel):
    username: KerberosID
    email: str
    uid: LinuxUID
    gid: LinuxGID
    fullname: str
    shell: Shell
    home_directory: str
    type: UserType
    status: UserStatus

    ssh_key: Optional[str] = None
    access: Optional[AccessType] = 'ssh'
    comments: Optional[List[str]] = None
    _id: Optional[str] = None

    @post_load
    def _set_id(self, in_data, **kwargs):
        in_data['_id'] = in_data['username']
        return in_data

    @classmethod
    def from_puppet(cls, username: str, 
                         puppet_record: PuppetUserRecord,
                         ssh_key: Optional[str] = None):
        if puppet_record.groups is not None and 'hpccfgrp' in puppet_record.groups: #type: ignore
            usertype = 'admin'
        elif puppet_record.uid > 3000000000:
            usertype = 'system'
        else:
            usertype = 'user'

        shell = puppet_record.shell if puppet_record.shell else DEFAULT_SHELL

        if shell in DISABLED_SHELLS and usertype in ('admin', 'user'):
            userstatus = 'inactive'
        else:
            userstatus = 'active'

        return cls.Schema().load(dict(
            username = username,
            email = puppet_record.email,
            uid = puppet_record.uid,
            gid = puppet_record.gid,
            fullname = puppet_record.fullname,
            shell = shell,
            home_directory = f'/home/{username}',
            type = usertype,
            status = userstatus,
            ssh_key = ssh_key,
            access = 'ondemand' if ssh_key is None else 'ssh'
        ))


@dataclass(frozen=True)
class SiteUserRecord(MongoModel):
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
class FullUserRecord(MongoModel):
    username: KerberosID
    sitename: str
    email: str
    uid: LinuxUID
    gid: LinuxGID
    fullname: str
    shell: Shell
    home_directory: str
    type: UserType
    status: UserStatus
    expiry: Optional[Date] = None
    slurm: Optional[SlurmRecord] = None
    tag: Optional[Set[str]] = None
    access: Optional[AccessType] = 'ssh'
    ssh_key: Optional[str] = None

    comments: Optional[List[str]] = None

    _id: Optional[str] = None

    @classmethod
    def from_merge(cls, global_record: GlobalUserRecord, site_record: SiteUserRecord, sitename: str):
        return FullUserRecord.load(puppet_merge(global_record.to_dict(),
                                                site_record.to_dict(),
                                                {'sitename': sitename}))


@dataclass(frozen=True)
class GlobalGroupRecord(MongoModel):
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
class SiteGroupRecord(MongoModel):
    groupname: KerberosID
    members: Optional[Set[KerberosID]] = field(default_factory=set)
    sponsors: Optional[Set[KerberosID]] = field(default_factory=set)
    sudoers: Optional[Set[KerberosID]] = field(default_factory=set)
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
class FullGroupRecord(MongoModel):
    groupname: KerberosID
    gid: LinuxGID
    sitename: str
    members: Optional[Set[KerberosID]] = field(default_factory=set)
    sponsors: Optional[Set[KerberosID]] = field(default_factory=set)
    slurm: Optional[SlurmRecord] = None

    _id: Optional[str] = None

    @classmethod
    def from_merge(cls, global_record: GlobalGroupRecord, site_record: SiteGroupRecord, sitename: str):
        return FullGroupRecord.load(puppet_merge(global_record.to_dict(),
                                                   site_record.to_dict(),
                                                   {'sitename': sitename}))


@dataclass(frozen=True)
class SiteRecord(MongoModel):
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
class Collections(MongoModel):
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


def add_indices(db: Database):
    sites = db['sites']
    sites.create_index('sitename', unique=True)
    sites.create_index('users.username')
    sites.create_index('groups.groupname')
    sites.create_index({'users.tag': 'text', 'groups.sponsors': 'text', 'groups.members': 'text'})

    users = db['users']
    users.create_index('username', unique=True)
    users.create_index({'fullname': 'text',
                        'email': 'text', 'type': 'text', 'home_directory': 'text'})
   
    groups = db['groups']
    groups.create_index('groupname', unique=True)


def add_site(db: Database, site: SiteRecord):
    sites = db['sites']
    sites.insert_one(site.to_dict())


def upsert_global_user(db: Database, user: GlobalUserRecord):
    db.users.update_one({'username': user.username},
                        {'$set': user.to_dict()},
                        upsert=True)


def query_global_user(db: Database, username: str) -> (GlobalUserRecord | None):
    result = db.users.find_one({'username': username})
    if result is not None:
        return GlobalUserRecord.load(result)
    return result


def replace_global_user(db: Database, user: GlobalUserRecord) -> bool:
    result = db.users.update_one({'username': user.username},
                                 {'$set': user.to_dict()})
    if result.modified_count == 0:
        return False
    else:
        return True


def add_user_comment(db: Database, username: str, comment: str) -> bool:
    comment = tag_comment(comment)
    result = db.users.update_one({'username': username},
                                 {'$push': {'comments': comment}})
    if result.modified_count == 0:
        return False
    else:
        return True


def user_exists(db: Database, username: str, sitename: Optional[str] = None) -> bool:
    if sitename is not None:
        return db.sites.count_documents({'sitename': sitename, 
                                         'users': {'$elemMatch': {'username': username}}}) > 0
    else:
        return db.users.count_documents({'username': username}) > 0


def update_global_user(db: Database, username: str, updates: dict, comment: str = ''):
    _comment = tag_comment(str(updates))
    if comment:
        _comment += f', {comment}'
    if '$push' in updates:
        updates['$push']['comments'] = _comment
    else:
        updates['$push'] = {'comments': _comment}

    GlobalUserRecord.validate_db_update(updates)
    result = db.users.update_one({'username': username},
                                 updates)

    if result.modified_count == 0:
        return False
    else:
        return True

def upsert_site_user(db: Database, sitename: str, user: SiteUserRecord):
    result = db.sites.update_one({'sitename': sitename, 'users': {'$elemMatch': {'username': user.username}}},
                                 {'$set': { 'users.$': user.to_dict() }})
    if result.modified_count == 0:
        db.sites.update_one({'sitename': sitename}, 
                            {'$addToSet': {'users': user.to_dict()}},
                            upsert=True)


def _query_site_user_q(sitename: str, usernames: List[str],
                       op: Optional[str] = '$eq'):
    match op:
        case '$in':
            cond = {'$in': ['$$user.username', usernames] }
        case '$nin':
            cond = {'$nin': ['$$user.username', usernames] }
        case _:
            cond = {'$eq': ['$$user.username', usernames[0]] }

    return ({'sitename': sitename}, 
            {'users': {'$filter': 
                      {'input': '$users', 
                       'as': 'user', 
                       'cond': cond}}})


def query_site_user(db: Database, sitename: str, username: str,
                    full=True):
    query, projection = _query_site_user_q(sitename, [username])
    result = db.sites.find_one(query, projection=projection)
    if result is None or not result['users']:
        return None

    site_user = SiteUserRecord.load(result['users'].pop())

    if full:
        global_user = query_global_user(db, username)
        return FullUserRecord.from_merge(global_user, site_user, sitename)
    else:
        return site_user


def query_global_group(db: Database, groupname: str) -> (GlobalGroupRecord | None):
    result = db.groups.find_one({'groupname': groupname})
    if result is not None:
        return GlobalGroupRecord.load(result)
    return result


def _query_site_group_q(sitename: str, groupnames: List[str],
                        op: Optional[str] = '$eq'):
    match op:
        case '$in':
            cond = {'$in': ['$$group.groupname', groupnames] }
        case '$nin':
            cond = {'$nin': ['$$group.groupname', groupnames] }
        case _:
            cond = {'$eq': ['$$group.groupname', groupnames[0]] }

    return ({'sitename': sitename}, 
            {'groups': {'$filter': 
                      {'input': '$groups', 
                       'as': 'group', 
                       'cond': cond}}})


def query_site_group(db: Database, sitename: str, groupname: str,
                     full=True):
    query, projection = _query_site_group_q(sitename, [groupname])
    result = db.sites.find_one(query, projection=projection)
    if result is None or not result['groups']:
        return None

    site_group = SiteGroupRecord.load(result['groups'].pop())

    if full:
        global_group = query_global_group(db, groupname)
        return FullGroupRecord.from_merge(global_group, site_group, sitename)
    else:
        return site_group


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


def add_user_to_group(db: Database, sitename: str, username: str, groupname: str):
    if not user_exists(db, username, sitename=sitename):
        raise InvalidUser(f'User {username} not a site user in {sitename}.')

    db.sites.update_one({'sitename': sitename, 'groups': {'$elemMatch': {'groupname': groupname}}},
                        {'$addToSet': {'groups.$.members': username}})


def add_sponsor_to_group(db: Database, sitename: str, username: str, groupname: str):
    if not user_exists(db, username, sitename=sitename):
        raise InvalidUser(f'User {username} not a site user in {sitename}.')

    db.sites.update_one({'sitename': sitename, 'groups': {'$elemMatch': {'groupname': groupname}}},
                        {'$addToSet': {'groups.$.sponsors': username}})


def add_sudoer_to_group(db: Database, sitename: str, username: str, groupname: str):
    if not user_exists(db, username, sitename=sitename):
        raise InvalidUser(f'User {username} not a site user in {sitename}.')

    db.sites.update_one({'sitename': sitename, 'groups': {'$elemMatch': {'groupname': groupname}}},
                        {'$addToSet': {'groups.$.sudoers': username}})


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
            ssh_key_path, ssh_key = args.key_dir / f'{user_name}.pub', None
            if ssh_key_path.exists():
                ssh_key = ssh_key_path.read_text().strip()
            global_record = GlobalUserRecord.from_puppet(user_name, user_record, ssh_key=ssh_key)
            upsert_global_user(db, global_record) #type: ignore

            site_record = SiteUserRecord.from_puppet(user_name, user_record)
            print(site_record)
            upsert_site_user(db, args.sitename, site_record) #type: ignore

            if user_record.groups is not None:
                for group_name in user_record.groups:
                    add_user_to_group(db, args.sitename, user_name, group_name)

            if user_record.group_sudo is not None:
                for group_name in user_record.group_sudo:
                    add_sudoer_to_group(db, args.sitename, user_name, group_name)


@subcommand('index')
def do_index(args: argparse.Namespace):
    db = get_database(args.config.mongo)
    add_indices(db)


def add_query_args(parser: argparse.ArgumentParser):
    parser.add_argument('--in', dest='val_in', nargs='+', action='append')
    parser.add_argument('--not-in', dest='val_not_in', nargs='+', action='append')
    parser.add_argument('--gt', nargs=2, action='append')
    parser.add_argument('--lt', nargs=2, action='append')
    parser.add_argument('--search', '-s', nargs='+', action='append')
    parser.add_argument('--all', default=False, action='store_true')


def build_simple_query(args: argparse.Namespace,
                       record_type: MongoModel):
    query = defaultdict(dict)

    if args.search:
        for query_group in args.search:
            query['$text'] = {'$search': ' '.join(query_group)}
    else:
        if args.val_in:
            for query_group in args.val_in:
                field = query_group[0]
                if field in record_type.field_names():
                    deserialize = record_type.Schema().fields[field].deserialize
                    query[field]['$in'] = [deserialize(v) for v in query_group[1:]]
        if args.val_not_in:
            for query_group in args.val_not_in:
                field = query_group[0]
                if field in record_type.field_names():
                    deserialize = record_type.Schema().fields[field].deserialize
                    query[field]['$nin'] = [deserialize(v) for v in query_group[1:]]
        if args.lt:
            for query_group in args.lt:
                field, val = query_group
                if field in record_type.field_names():
                    deserialize = record_type.Schema().fields[field].deserialize
                    query[field]['$lt'] = deserialize(val)
        if args.gt:
            for query_group in args.gt:
                field, val = query_group
                if field in record_type.field_names():
                    deserialize = record_type.Schema().fields[field].deserialize
                    query[field]['$gt'] = deserialize(val)

    return dict(query)



def build_filter_condition(args: argparse.Namespace,
                           field: str,
                           params: List[str]):
    pass


def build_nested_filter_projection(args: argparse.Namespace,
                                   nested_name: str,
                                   record_type: MongoModel):
    
    conditions = {}
    if args.val_in:
        for query_group in args.val_in:
            field, values = query_group[0], query_group[1:]
            if field in record_type.field_names():
                deserialize = record_type.Schema().fields[field].deserialize
                conditions['$in'] = [f'$$item.{field}', list(map(deserialize, values))]
    if args.val_not_in:
        for query_group in args.val_not_in:
            field, values = query_group[0], query_group[1:]
            if field in record_type.field_names():
                deserialize = record_type.Schema().fields[field].deserialize
                conditions['$nin'] = [f'$$item.{field}', list(map(deserialize, values))]
    if args.lt:
        for query_group in args.lt:
            field, val = query_group
            if field in record_type.field_names():
                deserialize = record_type.Schema().fields[field].deserialize
                conditions['$lt'] = [f'$$item.{field}', deserialize(val)]
    if args.gt:
        for query_group in args.gt:
            field, val = query_group
            if field in record_type.field_names():
                deserialize = record_type.Schema().fields[field].deserialize
                conditions['$gt'] = [f'$$item.{field}', deserialize(val)]

    return {nested_name: {'$filter': {'input': f'${nested_name}',
                                      'as': 'item',
                                      'cond': conditions}}}

def add_query_user_args(parser: argparse.ArgumentParser):
    parser.add_argument('--site')
    parser.add_argument('--fields', nargs='+', choices=FullUserRecord.field_names())
    parser.add_argument('--list-fields', action='store_true', default=False)


@subcommand('query', add_query_args, add_query_user_args)
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
            dumper = records[0].Schema(only=args.fields)# .dumps(records, many=True))
        else:
            dumper = records[0].Schema(exclude=['_id'])# .dumps(records, many=True))

        output_txt = dumper.dumps(records, many=True)
        output_txt = Syntax(output_txt,
                            'yaml',
                            theme='github-dark',
                            background_color='default')
        console.print(output_txt)


def add_user_modify_args(parser: argparse.ArgumentParser):
    parser.add_argument('--user', '-u', required=True)
    parser.add_argument('--comment', '-c', default='')


def tag_comment(comment: str):
    return f'[{TIMESTAMP_NOW}]: {comment}'


@subcommand('enable', add_user_modify_args)
def enable_user(args: argparse.Namespace):
    db = get_database(args.config.mongo)
    comment = f'enable, {args.comment}' if args.comment else 'enable'

    update_global_user(db, args.user, {'$set': {'status': 'active'}}, comment=comment)


@subcommand('disable', add_user_modify_args)
def disable_user(args: argparse.Namespace):
    logger = logging.getLogger(__name__)
    db = get_database(args.config.mongo)

    if not args.comment:
        logger.error('Must supply a comment when disabling a user.')
        sys.exit(ExitCode.BAD_CMDLINE_ARGS)
    comment = f'disable, {args.comment}'

    update_global_user(db, args.user, {'$set': {'status': 'disabled'}}, comment=comment)


@subcommand('deactivate', add_user_modify_args)
def deactivate_user(args: argparse.Namespace):
    logger = logging.getLogger(__name__)
    db = get_database(args.config.mongo)

    if not args.comment:
        logger.error('Must supply a comment when deactivating a user.')
        sys.exit(ExitCode.BAD_CMDLINE_ARGS)
    comment = f'deactivate, {args.comment}'

    update_global_user(db, args.user, {'$set': {'status': 'inactive'}}, comment=comment)


def add_query_group_args(parser: argparse.ArgumentParser):
    parser.add_argument('--site')
    parser.add_argument('--fields', nargs='+', choices=FullGroupRecord.field_names())
    parser.add_argument('--list-fields', action='store_true', default=False)


@subcommand('query', add_query_args, add_query_group_args)
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
            site_records = []
            query = {'sitename': args.site}
            projection = build_nested_filter_projection(args, 'groups')
            results = db.sites.find(query, projection=projection)[0]['groups']
            
            for record in results:
                site_records.append(SiteGroupRecord.load(record))

            for site_record in site_records:
                global_record = query_global_group(db, site_record.groupname)
                records.append(FullGroupRecord.from_merge(global_record,
                                                          site_record,
                                                          args.site))
    else:
        if args.all:
            results = db.groups.find()
            for record in results:
                records.append(GlobalGroupRecord.load(record))
        else:
            query = build_simple_query(args)
            search = db.groups.find(query)
            for record in search:
                records.append(GlobalGroupRecord.load(record))

    if records:
        if args.fields:
            dumper = records[0].Schema(only=args.fields)# .dumps(records, many=True))
        else:
            dumper = records[0].Schema(exclude=['_id'])# .dumps(records, many=True))

        output_txt = dumper.dumps(records, many=True)
        output_txt = Syntax(output_txt,
                            'yaml',
                            theme='github-dark',
                            background_color='default')
        console.print(output_txt)
