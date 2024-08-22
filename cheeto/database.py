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
import re
import sys
from typing import List, Mapping, Optional, Set, Tuple, Type, TypedDict, Union

from marshmallow import post_load
from marshmallow_dataclass import _field_for_union_type, dataclass
from pyasn1.codec import der
from pygments.lexer import default
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
from .yaml import dumps as dumps_yaml, highlight_yaml


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

    status: Optional[UserStatus] = 'active'
    ssh_key: Optional[str] = None
    access: Optional[Set[AccessType]] = field(default_factory=lambda: {'ssh'})
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

        return cls.Schema().load(dict(
            username = username,
            email = puppet_record.email,
            uid = puppet_record.uid,
            gid = puppet_record.gid,
            fullname = puppet_record.fullname,
            shell = shell,
            home_directory = f'/home/{username}',
            type = usertype,
            status = puppet_record.status,
            ssh_key = ssh_key,
            access = access
        ))


@dataclass(frozen=True)
class SiteUserRecord(MongoModel):
    username: KerberosID
    expiry: Optional[Date] = None
    slurm: Optional[SlurmRecord] = None
    status: Optional[UserStatus] = 'active'
    access: Optional[Set[AccessType]] = field(default_factory=set)

    _id: Optional[str] = None

    @post_load
    def _set_id(self, in_data, **kwargs):
        in_data['_id'] = in_data['username']
        return in_data

    @classmethod
    def from_puppet(cls, username: str, puppet_record: PuppetUserRecord):
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
        
        return cls.Schema().load(dict(
            username = username,
            expiry = puppet_data.get('expiry', None),
            slurm = puppet_data.get('slurm', None),
            access = access,
            status = puppet_record.status
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
    access: Optional[Set[AccessType]] = field(default_factory=set)
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
    sudoers: Optional[Set[KerberosID]] = field(default_factory=set)
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


def build_update_record(updates: dict, record_type: Type[MongoModel]):
    update_op = {}
    for field, date in updates.items():
        pass


def update_site_document(db: Database,
                         sitename: str,
                         parent: str,
                         document_id: str,
                         updates: dict,
                         merge_arrays: bool = True):

    match_op = {'sitename': sitename,
                parent: {'$elemMatch': {'_id': document_id}}}

    update_op = defaultdict(dict)
    for field, value in updates.items():
        if is_listlike(value) and merge_arrays:
            update_op['$addToSet'][f'{parent}.{field}'] = value
        else:
            update_op['$set'][f'{parent}.{field}'] = value

    return db.sites.update_one(match_op, update_op)


def add_site(db: Database, site: SiteRecord):
    sites = db['sites']
    sites.update_one({'sitename': site.sitename},
                     {'$set': site.to_dict()},
                     upsert=True)


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


def pipeline_groups_with_subset(db: Database, site: str, subfield: str, subset: Set[str],
                                as_array=True, objects=False):
    pipeline = [
        {'$match': {'sitename': site}},
        {
            '$project': {
                'groups': {
                    '$filter': {
                        'input': '$groups',
                        'as': 'item',
                        'cond': {
                            '$and': [
                                {'$ne': [{'$type': f'$$item.{subfield}'}, 'missing']},
                                {'$setIsSubset': [list(subset), f'$$item.{subfield}']}
                            ]
                        }
                    }
                }
            }
        },
        #{'$project': {'groups.groupname': True, '_id': False}},
        {'$unwind': '$groups'},
        {'$group': {'_id': None, 'groupnames': {'$push': '$groups.groupname'}}},
        {'$project': {'_id': False, 'groupnames': True}}
    ]

    return pipeline


def query_user_groups(db: Database, sitename: str, username: str) -> List[str]:
    pipeline = pipeline_groups_with_subset(db, sitename, 'members', {username})
    try:
        result = db.sites.aggregate(pipeline).next()
    except StopIteration:
        return []
    else:
        return result['groupnames']


def query_user_sponsor_of(db: Database, sitename: str, username: str) -> List[str]:
    pipeline = pipeline_groups_with_subset(db, sitename, 'sponsors', {username})
    try:
        result = db.sites.aggregate(pipeline).next()
    except StopIteration:
        return []
    else:
        return result['groupnames']


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

        site = SiteRecord(sitename=args.sitename,
                          fqdn=args.site_dir.name,
                          users=[],
                          groups=[])
        logger.info(f'Updating Site: {site}')
        add_site(db, site)

        logger.info(f'Adding {len(site_data.data.group)} groups.')
        for group_name, group_record in site_data.iter_groups():
            global_record = GlobalGroupRecord.from_puppet(group_name, group_record)
            upsert_global_group(db, global_record) #type: ignore

            site_record = SiteGroupRecord.from_puppet(group_name, group_record)
            #print(site_record)
            upsert_site_group(db, args.sitename, site_record) #type: ignore

        logger.info(f'Adding {len(site_data.data.user)} users.')
        for user_name, user_record in site_data.iter_users():
            ssh_key_path, ssh_key = args.key_dir / f'{user_name}.pub', None
            if ssh_key_path.exists():
                ssh_key = ssh_key_path.read_text().strip()
            global_record = GlobalUserRecord.from_puppet(user_name, user_record, ssh_key=ssh_key)
            upsert_global_user(db, global_record) #type: ignore

            site_record = SiteUserRecord.from_puppet(user_name, user_record)
            #print(site_record)
            upsert_site_user(db, args.sitename, site_record) #type: ignore

            if user_record.groups is not None:
                for group_name in user_record.groups:
                    add_user_to_group(db, args.sitename, user_name, group_name)

            if user_record.group_sudo is not None:
                for group_name in user_record.group_sudo:
                    add_sudoer_to_group(db, args.sitename, user_name, group_name)
        logger.info('Done.')


@subcommand('index')
def do_index(args: argparse.Namespace):
    db = get_database(args.config.mongo)
    add_indices(db)


def add_query_args(parser: argparse.ArgumentParser):
    parser.add_argument('--in', dest='val_in', nargs='+', action='append')
    parser.add_argument('--not-in', dest='val_not_in', nargs='+', action='append')
    parser.add_argument('--contains', nargs='+', action='append')
    parser.add_argument('--gt', nargs=2, action='append')
    parser.add_argument('--lt', nargs=2, action='append')
    parser.add_argument('--match', '-m', nargs=2, action='append')
    parser.add_argument('--search', '-s', nargs='+', action='append')
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



def build_filter_condition(args: argparse.Namespace,
                           field: str,
                           params: List[str]):
    pass


def build_nested_filter_projection(args: argparse.Namespace,
                                   nested_name: str,
                                   record_type: MongoModel):

    logger = logging.getLogger(__name__)
    
    conditions = []
    if args.val_in:
        for field, values in validate_query_groups(args.val_in, record_type):
            deserialize = record_type.field_deserializer(field)
            conditions.append({'$in': [f'$$item.{field}', values]})
    if args.val_not_in:
        for field, values in validate_query_groups(args.val_not_in, record_type):
            conditions.append({'$nin': [f'$$item.{field}', values]})
    if args.lt:
        for field, value in validate_query_groups(args.lt, record_type, single=True):
            conditions.append({'$lt': [f'$$item.{field}', value]})
    if args.gt:
        for field, value in validate_query_groups(args.gt, record_type, single=True):
            conditions.append({'$gt': [f'$$item.{field}', value]})

    if args.contains:
        for field, values in validate_query_groups(args.contains, record_type):
            conditions.append({'$ne': [{'$type': f'$$item.{field}'}, 'missing']})
            conditions.append({'$setIsSubset': [values, f'$$item.{field}']})

    if len(conditions) == 1:
        condexpr = conditions[0]
    else:
        condexpr = {'$and': conditions}

    projection = {nested_name: {'$filter': {'input': f'${nested_name}',
                                            'as': 'item',
                                            'cond': condexpr}}}
    logger.debug(f'Filter projection: \n{projection}')
    return projection


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
            dumper = records[0].Schema(only=args.fields)
        else:
            dumper = records[0].Schema(exclude=['_id'])

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


def add_user_membership_args(parser: argparse.ArgumentParser):
    parser.add_argument('--site', '-s', required=True)
    parser.add_argument('users', nargs='+')


@subcommand('groups', add_user_membership_args)
def user_groups(args: argparse.Namespace):
    db = get_database(args.config.mongo)
    console = Console()

    output = {}
    for username in args.users:
        output[username] = query_user_groups(db, args.site, username)

    dumped = dumps_yaml(output)
    console.print(highlight_yaml(dumped))


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


def add_group_add_member_args(parser: argparse.ArgumentParser):
    parser.add_argument('--site', '-s', required=True)
    parser.add_argument('--users', '-u', nargs='+', required=True)
    parser.add_argument('--groups', '-g', nargs='+', required=True)


@subcommand('add-user', add_group_add_member_args)
def group_add_user(args: argparse.Namespace):
    db = get_database(args.config.mongo)
    console = Console()

    for username in args.users:
        for groupname in args.groups:
            add_user_to_group(db, args.site, username, groupname)


@subcommand('add-sponsor', add_group_add_member_args)
def group_add_sponsor(args: argparse.Namespace):
    db = get_database(args.config.mongo)
    console = Console()

    for username in args.users:
        for groupname in args.groups:
            add_sponsor_to_group(db, args.site, username, groupname)
