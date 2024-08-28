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
from contextlib import contextmanager
import logging
from typing import List, Mapping, Optional, Set, Tuple, Type, TypedDict, Union

from mongoengine import *
from pymongo.common import CONNECT_TIMEOUT

from cheeto import hippo

from .args import subcommand
from .config import HippoConfig, LDAPConfig, MongoConfig, Config

from .hippoapi.api.event_queue import (event_queue_pending_events,
                                       event_queue_update_status)
from .hippoapi.client import AuthenticatedClient
from .hippoapi.models import (QueuedEventAccountModel,
                              QueuedEventModel,
                              QueuedEventDataModel,
                              QueuedEventUpdateModel)
from .ldap import LDAPManager, LDAPUser, LDAPGroup
from .log import Console
from .puppet import MIN_PIGROUP_GID, PuppetGroupRecord, PuppetUserRecord, SlurmRecord, add_repo_args, SiteData
from .types import (DEFAULT_SHELL, DISABLED_SHELLS, ENABLED_SHELLS, HIPPO_EVENT_ACTIONS, HIPPO_EVENT_STATUSES, hippo_to_cheeto_access,  
                    is_listlike, UINT_MAX, USER_TYPES,
                    USER_STATUSES, ACCESS_TYPES)
from .utils import TIMESTAMP_NOW, __pkg_dir__
from .yaml import dumps as dumps_yaml, highlight_yaml, puppet_merge


def POSIXNameField(**kwargs):
    return StringField(regex=r'[a-z_]([a-z0-9_-]{0,31}|[a-z0-9_-]{0,30}\$)',
                       min_length=1,
                       max_length=32,
                       **kwargs)

def POSIXIDField(**kwargs):
    return LongField(min_value=0, 
                     max_value=UINT_MAX,
                     **kwargs)

def UserTypeField(**kwargs): 
    return StringField(choices=USER_TYPES,
                       **kwargs)

def UserStatusField(**kwargs):
    return StringField(choices=USER_STATUSES,
                       **kwargs)

def UserAccessField(**kwargs):
    return StringField(choices=ACCESS_TYPES,
                       **kwargs)

def ShellField(**kwargs):
    return StringField(choices=ENABLED_SHELLS | DISABLED_SHELLS,
                       default=DEFAULT_SHELL,
                       **kwargs)


class InvalidUser(RuntimeError):
    pass


class BaseDocument(Document):
    meta = {
        'abstract': True
    }

    def to_dict(self, strip_id=True):
        data = self.to_mongo(use_db_field=False).to_dict()
        if strip_id:
            data.pop('id', None)
            data.pop('_id', None)
        return data


class HippoEvent(BaseDocument):
    hippo_id = LongField(required=True, unique=True)
    action = StringField(required=True,
                         choices=HIPPO_EVENT_ACTIONS)
    n_tries = IntField(required=True, default=0)
    status = StringField(required=True,
                         default='Pending',
                         choices=HIPPO_EVENT_STATUSES)


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
    ssh_key = ListField(StringField())
    access = ListField(UserAccessField(), default=lambda: ['login-ssh'])
    comments = ListField(StringField())

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

        return cls(
            username = username,
            email = puppet_record.email,
            uid = puppet_record.uid,
            gid = puppet_record.gid,
            fullname = puppet_record.fullname,
            shell = shell,
            home_directory = f'/home/{username}',
            type = usertype,
            status = puppet_record.status,
            ssh_key = [] if ssh_key is None else ssh_key.split('\n'),
            access = access
        )

    @classmethod
    def from_hippo(cls, hippo_data: QueuedEventAccountModel):
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


class SiteUser(BaseDocument):
    username = POSIXNameField(required=True)
    sitename = StringField(required=True, unique_with='username')
    parent = ReferenceField(GlobalUser, required=True, reverse_delete_rule=CASCADE)
    expiry = DateField()
    status = UserStatusField(default='active')
    #slurm: Optional[SlurmRecord] = None
    access = ListField(UserAccessField(), default=lambda: ['login-ssh'])

    meta = {
        'indexes': [
            {
                'fields': ('username', 'sitename'),
                'unique': True
            }
        ]
    }

    @classmethod
    def from_puppet(cls, username: str,
                         sitename: str,
                         parent: GlobalUser,
                         puppet_record: PuppetUserRecord):

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
            access = access,
            status = puppet_record.status,
            parent = parent
        )

    def to_dict(self):
        data = super().to_dict()
        data['parent'] = self.parent.to_dict()
        return data


class GlobalGroup(BaseDocument):
    groupname = POSIXNameField(required=True, primary_key=True)
    gid = POSIXIDField(required=True)
    
    @classmethod
    def from_puppet(cls, groupname: str, puppet_record: PuppetGroupRecord):
        return cls(
            groupname = groupname,
            gid = puppet_record.gid
        )



class SiteGroup(BaseDocument):
    groupname = POSIXNameField(required=True)
    sitename = StringField(required=True, unique_with='groupname')
    parent = ReferenceField(GlobalGroup, required=True, reverse_delete_rule=CASCADE)
    _members = ListField(ReferenceField(SiteUser, reverse_delete_rule=PULL))
    _sponsors = ListField(ReferenceField(SiteUser, reverse_delete_rule=PULL))
    _sudoers = ListField(ReferenceField(SiteUser, reverse_delete_rule=PULL))
    #slurm: Optional[SlurmRecord] = None

    @classmethod
    def from_puppet(cls, groupname: str,
                         sitename: str,
                         parent: GlobalGroup,
                         puppet_record: PuppetGroupRecord):
        return cls(
            groupname = groupname,
            sitename = sitename,
            parent = parent
            #slurm = puppet_record.slurm.to_dict() if puppet_record.slurm is not None else None
        )

    def to_dict(self, raw=False):
        data = super().to_dict()
        if not raw:
            data['parent'] = self.parent.to_dict()
            data.pop('_members')
            data['members'] = self.members
            data.pop('_sponsors')
            data['sponsors'] = self.sponsors
            data.pop('_sudoers')
            data['sudoers'] = self.sudoers
        return data

    @property
    def members(self):
        return [m.username for m in self._members]

    @property
    def sponsors(self):
        return [s.username for s in self._sponsors]

    @property
    def sudoers(self):
        return [s.username for s in self._sudoers]



class Site(BaseDocument):
    sitename = StringField(required=True, primary_key=True)
    fqdn = StringField(required=True)


#@contextmanager
#def log_db_exceptions():
#    logger = logging.getLogger(__name__)
#    try:
#        yield
#    except GlobalUser.DoesNotExist


def connect_to_database(config: MongoConfig):
    connect(config.database,
            host=config.uri,
            username=config.user,
            password=config.password)


def user_exists(username: str, sitename: Optional[str] = None) -> bool:
    try:
        if sitename is None:
            _ = GlobalUser.objects.get(username=username)
        else:
            _ = SiteUser.objects.get(username=username, sitename=sitename)
    except DoesNotExist:
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
        GlobalUser.objects(username=username).update_one(add_to_set__access=access_type) #type: ignore
    else:
        SiteUser.objects(username=username, #type: ignore
                         sitename=sitename).update_one(add_to_set__access=access_type)


def set_user_status(username: str,
                    status: str,
                    reason: str,
                    sitename: Optional[str] = None):

    scope = "global" if sitename is None else sitename
    comment = f'status={status}, scope={scope}, reason={reason}'
    add_user_comment(username, comment)

    if sitename is None:
        GlobalUser.objects(username=username).update_one(set__status=status) #type: ignore
    else:
        SiteUser.objects(username=username, #type: ignore
                         sitename=sitename).update_one(set__status=status)


def set_user_type(username: str,
                  usertype: str):
    GlobalUser.objects(username=username).update_one(set__type=usertype)


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


def query_user_groups(sitename: str, username: str):
    user = SiteUser.objects.get(sitename=sitename, username=username)
    qs = SiteGroup.objects(_members=user, sitename=sitename).only('groupname')
    for group in qs:
        yield group.groupname


def query_user_sponsor_of(sitename: str, username: str):
    user = SiteUser.objects.get(sitename=sitename, username=username)
    qs = SiteGroup.objects(_sponsors=user, sitename=sitename).only('groupname')
    for group in qs:
        yield group.groupname


def add_group_member(sitename: str, username: str, groupname: str):
    logging.getLogger(__name__).info(f'Add user {username} to group {groupname} for site {sitename}')
    user = SiteUser.objects.get(sitename=sitename, username=username)
    SiteGroup.objects(sitename=sitename,
                      groupname=groupname).update_one(add_to_set___members=user)


def add_group_sponsor(sitename: str, username: str, groupname: str):
    logging.getLogger(__name__).info(f'Add sponsor {username} to group {groupname} for site {sitename}')
    user = SiteUser.objects.get(sitename=sitename, username=username)
    SiteGroup.objects(sitename=sitename,
                      groupname=groupname).update_one(add_to_set___sponsors=user)


def add_group_sudoer(sitename: str, username: str, groupname: str):
    logging.getLogger(__name__).info(f'Add sudoer {username} to group {groupname} for site {sitename}')
    user = SiteUser.objects.get(sitename=sitename, username=username)
    SiteGroup.objects(sitename=sitename,
                      groupname=groupname).update_one(add_to_set___sudoers=user)


def add_site_args(parser: argparse.ArgumentParser):
    parser.add_argument('--site', '-s', default=None)


def add_site_args_req(parser: argparse.ArgumentParser):
    parser.add_argument('--site', '-s', default=None, required=True)


@subcommand('load', add_repo_args, add_site_args)
def load(args: argparse.Namespace):
    logger = logging.getLogger(__name__)
    
    site_data = SiteData(args.site_dir,
                         common_root=args.global_dir,
                         key_dir=args.key_dir,
                         load=False)
    connect_to_database(args.config.mongo)

    with site_data.lock(args.timeout):
        site_data.load()

        site = Site(sitename=args.site,
                    fqdn=args.site_dir.name)
        logger.info(f'Updating Site: {site}')
        site.save()

        for group_name, group_record in site_data.iter_groups():
            #logger.info(f'{group_name}, {group_record}')
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
        for user_name, user_record in site_data.iter_users():
            ssh_key_path, ssh_key = args.key_dir / f'{user_name}.pub', None
            if ssh_key_path.exists():
                ssh_key = ssh_key_path.read_text().strip()

            global_record = GlobalUser.from_puppet(user_name, user_record, ssh_key=ssh_key)
            global_record.save()

            site_record = SiteUser.from_puppet(user_name,
                                               args.site,
                                               global_record,
                                               user_record)
            try:
                site_record.save()
            except:
                logger.info(f'{user_name} in {args.site} already exists, skipping.')
                site_record = SiteUser.objects.get(username=user_name, sitename=args.site)

            if global_record.type == 'system':
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

        # Now do sponsors
        for groupname, group_record in site_data.iter_groups():
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

'''
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
'''

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


def add_show_user_args(parser: argparse.ArgumentParser):
    parser.add_argument('username')


@subcommand('show', add_show_user_args, add_site_args)
def user_show(args: argparse.Namespace):
    logger = logging.getLogger(__name__)
    connect_to_database(args.config.mongo)

    try:
        if args.site is not None:
            user = SiteUser.objects.get(username=args.username, sitename=args.site).to_dict()
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


@subcommand('add-user', add_group_add_member_args, add_site_args)
def group_add_user(args: argparse.Namespace):
    connect_to_database(args.config.mongo)
    console = Console()

    for username in args.users:
        for groupname in args.groups:
            add_group_member(args.site, username, groupname)


@subcommand('add-sponsor', add_group_add_member_args)
def group_add_sponsor(args: argparse.Namespace):
    connect_to_database(args.config.mongo)
    console = Console()

    for username in args.users:
        for groupname in args.groups:
            add_group_sponsor(args.site, username, groupname)


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


def hippoapi_client(config: HippoConfig):
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
    sitename = config.site_aliases.get(event.cluster, event.cluster)
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
    sitename = config.site_aliases.get(event.cluster, event.cluster)
    username = hippo_account.kerberos

    logger.info(f'Process CreateAccount for site {sitename}, event: {event}')

    if not user_exists(username):
        logger.info(f'GlobalUser for {username} does not exist, creating.')
        global_user = GlobalUser.from_hippo(hippo_account)
        global_user.save()
    else:
        logger.info(f'GlobalUser for {username} exists, checking status.')
        global_user = GlobalUser.objects.get(username=username) #type: ignore
        if global_user.status != 'active':
            logger.info(f'GlobalUser for {username} has status {global_user.status}, setting to "active"')
            set_user_status(username, 'active', 'Activated from HiPPO')

    if user_exists(username, sitename=sitename):
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
                             access=hippo_to_cheeto_access(hippo_account.access_types)) #type: ignore
        site_user.save()

    for group in event.groups:
        add_group_member(sitename, username, group.name)

    if any((group.name == 'sponsors' for group in event.groups)):
        create_group_from_sponsor(site_user)


def process_addaccounttogroup_event(event: QueuedEventDataModel,
                                    config: HippoConfig):
    logger = logging.getLogger(__name__)
    hippo_account = event.accounts[0]
    sitename = config.site_aliases.get(event.cluster, event.cluster)
    logger.info(f'Process AddAccountToGroup for site {sitename}, event: {event}')

    for group in event.groups:
        add_group_member(sitename, hippo_account.kerberos, group.name)

    if any((group.name == 'sponsors' for group in event.groups)):
        site_user = SiteUser.objects.get(sitename=sitename, username=hippo_account.kerberos)
        create_group_from_sponsor(site_user)



def ldap_sync(sitename: str, config: Config):
    connect_to_database(config.mongo)
    ldap_mgr = LDAPManager(config.ldap['hpccf'], pool_keepalive=15, pool_lifetime=30)

    for user in SiteUser.objects(sitename=sitename):
        if user.parent.type == 'system' or user.status != 'active' \
           or user.parent.status != 'active':
            continue
        if not ldap_mgr.verify_user(user.username):
            print(user.to_dict())
            ldap_user = LDAPUser.Schema().load(dict(uid=user.username,
                                                    email=user.parent.email,
                                                    uid_number=user.parent.uid,
                                                    gid_number=user.parent.gid,
                                                    fullname=user.parent.fullname,
                                                    surname=user.parent.fullname.split()[-1]))
            ldap_mgr.add_user(ldap_user)

            access = (set(user.access) | set(user.parent.access))
            if 'login-ssh' in access:
                ldap_mgr.add_user_to_group(user.username, 'cluster-users', sitename)
            if 'compute-ssh' in access or user.parent.type == 'admin':
                ldap_mgr.add_user_to_group(user.username, 'compute-ssh-users', sitename)

    for group in SiteGroup.objects(sitename=sitename):
        if not ldap_mgr.verify_group(group.groupname, sitename):
            print(group.to_dict())
            ldap_group = LDAPGroup.load(dict(gid=group.groupname,
                                             gid_number=group.parent.gid,
                                             members=group.members))
            ldap_mgr.add_group(ldap_group, sitename)
                                             
