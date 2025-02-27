#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023-2024
# (c) The Regents of the University of California, Davis, 2023-2024
# File   : cmds/database.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 04.11.2024

from argparse import Namespace
from collections import defaultdict
import csv
import logging
from pathlib import Path
import statistics as stat
import sys
from venv import logger

from mongoengine import NotUniqueError, DoesNotExist
from ponderosa import ArgParser, arggroup
from pymongo.errors import DuplicateKeyError

from cheeto.config import IAMConfig
from ..database.user import DuplicateGlobalUser, DuplicateUser
from cheeto.iam import IAMAPI, sync_user_iam

from . import commands
from .puppet import repo_args
from ..database import *
from ..database import _storage_to_puppet
from ..args import regex_argtype
from ..encrypt import generate_password, get_mcf_hasher
from ..errors import ExitCode
from ..git import GitRepo
from ..log import Emotes, Console
from ..puppet import  SiteData
from ..types import (USER_TYPES,
                     USER_STATUSES,
                     ENABLED_SHELLS,
                     ACCESS_TYPES,
                     QOS_TRES_REGEX,
                     DATA_QUOTA_REGEX,
                     parse_qos_tres)
from ..utils import slugify, removed, make_ngrams
from ..yaml import highlight_yaml, parse_yaml, puppet_merge, dumps as dumps_yaml


@commands.register('database',
                   aliases=['db'],
                   help='Operations on the cheeto MongoDB')
def database_cmd(args: Namespace):
    pass

@database_cmd.args(common=True)
def database_args(parser: ArgParser):
    pass

@database_args.postprocessor(priority=50)
def _(args: Namespace):
    args.db = connect_to_database(args.config.mongo, quiet=args.quiet)


#########################################
#
# Site commands: cheeto database site ...
#
#########################################

@commands.register('database', 'site',
                   aliases=['s'],
                   help='Operations on sites')
def site_cmd(args: Namespace):
    pass


@arggroup('site')
def site_args(parser: ArgParser,
              required: bool = False,
              single: bool = True):
    args = ('--site', '-s')
    if single:
        parser.add_argument(*args,
                            default=None,
                            required=required,
                            help='Sitename or site FQDN')
    else:
        parser.add_argument(*args,
                            nargs='+',
                            default='all',
                            required=required,
                            help='Sitename or site FQDN; "all" for all sites')


@site_args.postprocessor()
def parse_site_arg(args: Namespace):
    if args.site is not None:
        if type(args.site) is list:
            args.site = [query_sitename(s) for s in args.site]
        else:
            args.site = query_sitename(args.site)


@commands.register('database', 'site', 'new',
                   help='Add a new site')
def site_new(args: Namespace):
    logger = logging.getLogger(__name__)
    create_site(args.sitename, args.fqdn)


@site_new.args()
def _(parser: ArgParser):
    parser.add_argument('--sitename', '-s', required=True)
    parser.add_argument('--fqdn', required=True)



@site_args.apply()
@commands.register('database', 'site', 'add-global-slurm',
                   help='Add a group for which users are made slurmers') #type: ignore
def add_global_slurm(args: Namespace):
    for group in args.groups:
        add_site_global_slurmer(args.site, group)


@add_global_slurm.args()
def _(parser: ArgParser):
    parser.add_argument('groups', nargs='+')


@site_args.apply(required=True)
@commands.register('database', 'site', 'to-puppet',
                   help='Dump site in puppet.hpc YAML format')
def write_to_puppet(args: Namespace):
    puppet_map = site_to_puppet(args.site)
    puppet_map.save_yaml(args.puppet_yaml)


@write_to_puppet.args()
def _(parser: ArgParser):
    parser.add_argument('puppet_yaml', type=Path)


@site_args.apply(required=True)
@commands.register('database', 'site', 'sync-old-puppet',
                    help='Fully sync site from database to puppet.hpc YAML repo')
def sync_old_puppet(args: Namespace):
    site = Site.objects.get(sitename=args.site)
    repo = GitRepo(args.repo, base_branch=args.base_branch)
    prefix = (args.repo / 'domains' / site.fqdn).absolute()
    yaml_path = prefix / 'merged' / 'all.yaml'
    puppet_map = site_to_puppet(args.site)

    with repo.commit(f'Update merged yaml for {args.site}',
                     clean=True,
                     push_merge=args.push_merge) as add:
        puppet_map.save_yaml(yaml_path)

        for user in SiteUser.objects(sitename=args.site).only('parent'):
            if not user.parent.ssh_key:
                continue
            keyfile = (args.repo / 'keys' / f'{user.parent.username}.pub').absolute()
            with keyfile.open('w') as fp:
                for key in user.parent.ssh_key:
                    print(key, file=fp)
        add(args.repo.absolute())


@sync_old_puppet.args()
def _(parser: ArgParser):
    parser.add_argument('repo', type=Path)
    parser.add_argument('--base-branch', default='main')
    parser.add_argument('--push-merge', default=False, action='store_true')


@site_args.apply(required=True)
@commands.register('database', 'site', 'to-ldap',
                   help='Sync site to LDAP server')
def sync_to_ldap(args: Namespace):
    ldap_sync(args.site, args.config, force=args.force)


@sync_to_ldap.args()
def _(parser: ArgParser):
    parser.add_argument('--force', '-f', default=False, action='store_true')


@site_args.apply(required=True)
@commands.register('database', 'site', 'to-sympa',
                   help='Dump site emails in Sympa format')
def site_write_sympa(args: Namespace):
    _site_write_sympa(args.site, args.output_txt, set(args.ignore))


@site_write_sympa.args()
def _(parser: ArgParser):
    parser.add_argument('output_txt', type=Path)
    parser.add_argument('--ignore', nargs='+', default=['hpc-help@ucdavis.edu'])


def _site_write_sympa(sitename: str, output: Path, ignore: set):
    with output.open('w') as fp:
        for user in SiteUser.objects(sitename=sitename, 
                                     parent__in=GlobalUser.objects(type__in=['user', 'admin'])):
            if user.status != 'inactive' and user.email not in ignore:
                print(user.email, file=fp)


@site_args.apply(required=True)
@commands.register('database', 'site', 'root-key',
                   help='Write admin public keys for a site to a file')
def site_write_root_key(args: Namespace):
    _site_write_root_key(args.site, args.output_txt)


@site_write_root_key.args()
def _(parser: ArgParser):
    parser.add_argument('output_txt', type=Path)


def _site_write_root_key(sitename: str, output: Path):
    with output.open('w') as fp:
        for user in SiteUser.objects(sitename=sitename,
                                     parent__in=GlobalUser.objects(type='admin')):
            if 'root-ssh' in user.access:
                if user.ssh_key:
                    print(f'# {user.username} <{user.email}>', file=fp)
                for key in user.ssh_key:
                    print(key, file=fp)


@site_args.apply(required=True)
@commands.register('database', 'site', 'sync-new-puppet',
                   help='Sync to new puppet.hpc format')
def site_sync_new_puppet(args: Namespace):
    site = Site.objects.get(sitename=args.site)
    repo = GitRepo(args.repo)
    prefix = (args.repo / 'domains' / site.fqdn).absolute()

    with repo.commit(f'Update root.pub, storage.yaml, and sympa.txt for {site.fqdn}.',
                     clean=True,
                     push_merge=args.push_merge) as add:
        root_key_path = prefix / 'root.pub'
        _site_write_root_key(args.site, root_key_path)

        storage_path = prefix / 'storage.yaml'
        _storage_to_puppet(args.site, storage_path)

        sympa_path = prefix / 'sympa.txt'
        _site_write_sympa(args.site, sympa_path, args.ignore_emails)

        add(root_key_path, storage_path, sympa_path)


@site_sync_new_puppet.args()
def _(parser: ArgParser):
    parser.add_argument('repo', type=Path)
    parser.add_argument('--ignore-emails', nargs='+',  default=['hpc-help@ucdavis.edu'])
    parser.add_argument('--push-merge', default=False, action='store_true')


@commands.register('database', 'site', 'list',
                   help='List all sites and their FQDNs')
def site_list(args: Namespace):
    for site in Site.objects():
        print(site.sitename, site.fqdn)


#@site_args.apply(required=True)
#@commands.register('database', 'site', 'show')
def site_show(args: Namespace):
    site = Site.objects.get(sitename=args.site)
    console = Console()
    console.print(site.fqdn)
    total_users = SiteUser.objects(sitename=args.site).count()
    reg_users = SiteUser.objects(sitename=args.site, parent__in=GlobalUser.objects(type='user')).count()
    admin_users = SiteUser.objects(sitename=args.site, parent__in=GlobalUser.objects(type='admin')).count()
    system_users = SiteUser.objects(sitename=args.site, parent__in=GlobalUser.objects(type='system')).count()



@arggroup('load')
def database_load_args(parser: ArgParser):
    parser.add_argument('--system-groups', action='store_true', default=False)
    parser.add_argument('--nfs-yamls', nargs='+', type=Path)
    parser.add_argument('--mount-source-site')


@repo_args.apply()
@site_args.apply(required=True)
@database_load_args.apply()
@commands.register('database', 'site', 'load',
                   help='Load database from puppet.hpc YAML')
def cmd_site_from_puppet(args: Namespace):
    logger = logging.getLogger(__name__)
    
    site_data = SiteData(args.site_dir,
                         common_root=args.global_dir,
                         key_dir=args.key_dir,
                         load=False)
    nfs_data = puppet_merge(*(parse_yaml(f) for f in args.nfs_yamls)).get('nfs', None)
    if nfs_data:
        logger.info(f'Got NFS data')

    if args.mount_source_site is not None:
        query_site_exists(args.mount_source_site, raise_exc=True)

    with site_data.lock(args.timeout):
        site_data.load()

        site = Site.objects.get(sitename=args.site)

        # Do automount tables
        if nfs_data:
            export_options = nfs_data['exports'].get('options', None)
            export_ranges = nfs_data['exports'].get('clients', None)
            for tablename, config in nfs_data['storage'].items():
                logger.info(f'Set up Storage for table {tablename} with config {config}') 
                autofs = config['autofs']

                if args.mount_source_site is None:
                    col_args = dict(sitename=args.site,
                                    name=tablename,
                                    _host=autofs.get('nas', None),
                                    prefix=autofs.get('path', None),
                                    _export_options=export_options,
                                    _export_ranges=export_ranges,
                                    _quota=config.get('zfs', {}).get('quota', None))
                    collection = ZFSSourceCollection(**{k:v for k, v in col_args.items() if v is not None})
                    try:
                        collection.save()
                    except NotUniqueError:
                        pass

                if (raw_opts := config['autofs'].get('options', False)):
                    options = raw_opts.strip('-').split(',')
                else:
                    options = None

                mount_args = dict(sitename=args.site,
                                  prefix=f'/{tablename}',
                                  tablename=tablename,
                                  _options=options)
                mountmap = AutomountMap(**{k:v for k, v in mount_args.items() if v is not None})
                try:
                    mountmap.save()
                except NotUniqueError:
                    logger.warning(f'NotUniqueError on AutomountMap save: {mount_args}')

        for group_name, group_record in site_data.iter_groups():
            #logger.info(f'{group_name}, {group_record}')
            if group_record.ensure == 'absent':
                continue

            if args.mount_source_site is None:
                global_record = GlobalGroup.from_puppet(group_name, group_record)
                global_record.save()
            else:
                global_record = GlobalGroup.objects.get(groupname=group_name)

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
        
        if args.mount_source_site is None:
            home_collection = ZFSSourceCollection.objects.get(sitename=args.site,
                                                              name='home')
        else:
            home_collection = ZFSSourceCollection.objects.get(sitename=args.mount_source_site,
                                                              name='home')
        home_automap = AutomountMap.objects.get(sitename=args.site,
                                                tablename='home')
        for user_name, user_record in site_data.iter_users():
            if user_record.ensure == 'absent':
                continue
            ssh_key_path, ssh_key = args.key_dir / f'{user_name}.pub', None
            if ssh_key_path.exists():
                ssh_key = ssh_key_path.read_text().strip()

            if args.mount_source_site is None:
                global_record = GlobalUser.from_puppet(user_name, user_record, ssh_key=ssh_key)
                global_record.save()
                global_group = GlobalGroup(groupname=user_name,
                                           gid=user_record.gid,
                                           type='user',
                                           user=global_record)
                global_group.save()
            else:
                global_record = GlobalUser.objects.get(username=user_name)
                global_group = GlobalGroup.objects.get(groupname=user_name)

            site_record = SiteUser.from_puppet(user_name,
                                               args.site,
                                               global_record,
                                               user_record)

            try:
                site_record.save()
            except:
                logger.info(f'{user_name} in {args.site} already exists, skipping.')
                site_record = SiteUser.objects.get(username=user_name, sitename=args.site)

            site_group = SiteGroup(sitename=args.site,
                                   groupname=user_name,
                                   parent=global_group,
                                   _members=[site_record])
            try:
                site_group.save()
            except NotUniqueError:
                logger.info(f'SiteGroup {user_name} already exists, adding user as member')
                add_group_member(args.site, site_record, user_name)
            except Exception as e:
                logger.warning(f'error saving SiteGroup for {user_name}: {e}')

            if global_record.type == 'system' and args.system_groups:
                if args.mount_source_site is None:
                    global_group = GlobalGroup(groupname=user_name, gid=user_record.gid)
                    global_group.save()
                else:
                    global_group = GlobalGroup.objects.get(groupname=user_name)
                
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
            
            if user_name == 'root':
                continue

            us = user_record.storage
            if args.mount_source_site is None:
                if us and us.autofs:
                    source = NFSMountSource(name=user_name,
                                            sitename=args.site,
                                            _host=us.autofs.nas,
                                            _host_path=str(Path(us.autofs.path) / user_name),
                                            owner=global_record,
                                            group=global_group,
                                            collection=home_collection)
                elif us and not us.autofs and us.zfs and us.zfs.quota:
                    source = ZFSMountSource(name=user_name,
                                            sitename=args.site,
                                            owner=global_record,
                                            group=global_group,
                                            _quota=us.zfs.quota,
                                            collection=home_collection)
                else:
                    source = ZFSMountSource(name=user_name,
                                            sitename=args.site,
                                            owner=global_record,
                                            group=global_group,
                                            collection=home_collection)
                try:
                    source.save()
                except NotUniqueError:
                    source = StorageMountSource.objects.get(sitename=args.site,
                                                            name=user_name)
            else:
                source = NFSMountSource.objects.get(name=user_name,
                                                    sitename=args.mount_source_site)

            options = us.autofs.split_options() if us is not None and us.autofs is not None else None
            mount = Automount(sitename=args.site,
                              name=user_name,
                              map=home_automap,
                              _options=options)
            try:
                mount.save()
            except NotUniqueError:
                mount = Automount.objects.get(sitename=args.site,
                                              name=user_name,
                                              map=home_automap)
            storage_record = Storage(name=user_name,
                                     source=source,
                                     mount=mount)
            try:
                storage_record.save()
            except NotUniqueError:
                pass

        # Now do sponsors and storages
        for groupname, group_record in site_data.iter_groups():
            if group_record.ensure == 'absent':
                continue
            if group_record.storage is not None:
                load_group_storages_from_puppet(group_record.storage,
                                                groupname,
                                                args.site,
                                                mount_source_site=args.mount_source_site)
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

        load_share_from_puppet(site_data.data.share,
                               args.site,
                               mount_source_site=args.mount_source_site)

        if args.mount_source_site is None:
            logger.info(f'Do slurm associations...')
            load_slurm_from_puppet(args.site,
                                   site_data.data)

        logger.info('Done.')


#########################################
#
# user commands: cheeto database user ...
#
#########################################


@commands.register('database', 'user',
                   aliases=['u'],
                   help='Operations on users')
def user_cmd(args: Namespace):
    pass


@arggroup('user')
def user_args(parser: ArgParser,
              required: bool = False,
              single: bool = False):
    args = ('--user', '-u')
    if single:
        parser.add_argument(*args, required=required)
    else:
        parser.add_argument(*args, nargs='+', required=required)


def process_user_args(args: Namespace):
    if args.site:
        return SiteUser.objects(sitename=args.site, username__in=args.user)
    else:
        return GlobalUser.objects(username__in=args.user)


def _show_siteuser(user: SiteUser, verbose: bool = False) -> dict:
    if verbose:
        user_data = user._pretty()
    else:
        user_data = user._pretty(lift=['parent'],
                                 skip=('ssh_key', 'sitename', 'password', 'iam_synced', 'gid'))
    user_data['groups'] = list(query_user_groups(user.sitename, user.username))
    if 'slurm' in user.access:
        if verbose:
            user_data['slurm'] = list(map(lambda s: removed(s, 'sitename'),
                                     (s.to_dict() for s in query_user_slurm(user.sitename, user.username))))
        else:
            user_data['slurm'] = query_user_partitions(user.sitename, user.username)
    return user_data


def _show_globaluser(user: GlobalUser, verbose: bool = False) -> dict:
    if verbose:
        user_data = user._pretty()
    else:
        user_data = user._pretty(skip=('ssh_key', 'sitename', 'password', 'iam_synced'))
    user_data['sites'] = [su.sitename for su in SiteUser.objects(username=user.username)]
    return user_data


def _show_user(user: User, verbose: bool = False) -> dict:
    return _show_globaluser(user, verbose=verbose) if isinstance(user, GlobalUser) \
        else _show_siteuser(user, verbose=verbose)


@site_args.apply()
@user_args.apply(single=True)
@commands.register('database', 'user', 'show',
                   help='Show user data, with Slurm associations if they exist and user has `slurm` access type')
def user_show(args: Namespace):
    logger = logging.getLogger(__name__)
    console = Console()
    stdout = Console(stderr=False)  

    users: list[User] = []

    if args.user:
        try:
            users.append(query_user(username=args.user, sitename=args.site))
        except DoesNotExist:
            scope = 'Global' if args.site is None else args.site
            logger.info(f'User {args.user} with scope {scope} does not exist.')
    elif args.uid:
        try:
            users.append(query_user(uid=args.uid, sitename=args.site))
        except DoesNotExist:
            scope = 'Global' if args.site is None else args.site
            logger.info(f'User {args.uid} with scope {scope} does not exist.')
    elif args.type:
        users.extend(query_user_type(args.type, sitename=args.site))
    elif args.access:
        users.extend(query_user_access(args.access, sitename=args.site))
    elif args.status:
        users.extend(query_user_status(args.status, sitename=args.site))
    elif args.find:
        query = ' '.join(make_ngrams(args.find))
        results = UserSearch.objects.search_text(query).only('user').order_by('$text_score')[:10]
        if not args.no_filter and len(results) > 4:
            scores = [r.get_text_score() for r in results]
            console.warn(f'More than 5 results for search "{args.find}", filtering...')
            mean = stat.mean(scores)
            stdev = stat.stdev(scores) or sys.float_info.min
            filtered = [r for r, s in zip(results, scores) if ((s - mean) / stdev) > 2]
            results = filtered if filtered else [r for r, s in zip(results, scores) if s > mean]
        
        if args.site:
            users.extend(SiteUser.objects(parent__in=[r.user for r in results], sitename=args.site))
        else:
            users.extend((result.user for result in results))
    else:
        if args.site:
            users.extend(SiteUser.objects(sitename=args.site))

    if args.list:
        stdout.print(' '.join((u.username for u in users)))
    else:
        for user in users:
            output = dumps_yaml(_show_user(user, verbose=args.verbose))
            stdout.print(highlight_yaml(output))
            #console.print(_show_user(user, verbose=args.verbose))


@user_show.args()
def _(parser: ArgParser):
    parser.add_argument('--uid', type=int, help='Show the user with the specified UID')
    parser.add_argument('--type', '-t', nargs='+',
                        help=f'Show users of these types. Options: {USER_TYPES}')
    parser.add_argument('--access', '-a', nargs='+',
                        help=f'Show users with these accesses. Options: {ACCESS_TYPES}')
    parser.add_argument('--email')
    parser.add_argument('--status', nargs='+')
    parser.add_argument('--find', '-f', help='''Find a user with text search. Searches over username,
                                             fullname, and email. If there are more than 5 results,
                                             returns only results with a text score more than 2 standard
                                             deviations above the mean. If there are no such results,
                                             returns all results with text score greater than the mean.''')
    parser.add_argument('--no-filter', action='store_true', default=False,
                        help='Do not filter search results')
    parser.add_argument('--list', '-l', default=False, action='store_true',
                        help='Only list usernames instead of full user info')
    parser.add_argument('--verbose', action='store_true', default=False)


@commands.register('database', 'user', 'new', 'system',
                   help='Create a new system user within the system ID range on the provided sites')
def user_new_system(args: Namespace):
    console = Console()

    if args.fullname is None:
        args.fullname = f'HPCCF {args.username}'

    password = None
    if args.password:
        password = generate_password()

    try:
        create_system_user(args.username,
                           args.email,
                           args.fullname,
                           password=password)
    except DuplicateGlobalUser as e:
        console.print(f'[red]{e}')
    else:
        console.print(f'Created user {args.username}, use `cheeto db user add site` to add to sites and create home storages.')
        if password is not None:
            console.print(f'Password: {password}')
            console.print('[red] Make sure to save this password in 1password!')


@user_new_system.args()
def _(parser: ArgParser):
    parser.add_argument('--email', default='hpc-help@ucdavis.edu')
    parser.add_argument('--fullname', help='Default: "HPCCF $username"')
    parser.add_argument('--password', action='store_true', default=False,
                        help='Generate a password for the new user')
    parser.add_argument('username')


@user_args.apply(required=True)
@site_args.apply()
@commands.register('database', 'user', 'set', 'status',
                   help='Set the status for a user, globally or per-site if --site is provided')
def user_set_status(args: Namespace):
    logger = logging.getLogger(__name__)

    for username in args.user:
        try:
            set_user_status(username, args.status, args.reason, sitename=args.site)
        except DoesNotExist:
            scope = 'Global' if args.site is None else args.site
            logger.info(f'User {args.username} with scope {scope} does not exist.')


@user_set_status.args()
def _(parser: ArgParser):
    parser.add_argument('status', choices=list(USER_STATUSES))
    parser.add_argument('--reason', '-r', required=True)


@user_args.apply(required=True)
@commands.register('database', 'user', 'set', 'shell',
                   help='Set the shell for a user')
def set_shell(args: Namespace):
    for user in args.user:
        set_user_shell(user, args.shell)


@set_shell.args()
def _(parser: ArgParser):
    parser.add_argument('--shell', required=True, choices=ENABLED_SHELLS)


@commands.register('database', 'user', 'set', 'password',
                   help='Set a (plaintext) password for a user; hashes it with yescrypt.')
def set_password(args: Namespace):
    hasher = get_mcf_hasher()
    console = Console()

    if len(args.password) < 20:
        console.print(f'[red] Password must be at least 20 characters')
        return ExitCode.BAD_CMDLINE_ARGS

    try:
        set_user_password(args.user, args.password, hasher)
    except DoesNotExist:
        console.print(f'[red]User {args.user} does not exist.')
        return ExitCode.DOES_NOT_EXIST


@set_password.args()
def password_args(parser: ArgParser):
    parser.add_argument('-u', '--user', required=True)
    parser.add_argument('--password', required=True)


@user_args.apply(required=True)
@commands.register('database', 'user', 'generate-passwords',
                   help='Generate passwords for the given users and output the results in CSV')
def generate_passwords(args: Namespace):
    console = Console()
    hasher = get_mcf_hasher()

    with args.file.open('w') as fp:
        for user in args.user:
            password = generate_password()
            try:
                set_user_password(user, password, hasher)
            except DoesNotExist:
                console.print(f'[red] User {user} does not exist, skipping.')
            else:
                fp.write(f'{user} {password}\n')


@generate_passwords.args()
def _(parser: ArgParser):
    parser.add_argument('--file', type=Path, default='/dev/stdout')



@arggroup()
def access_args(parser: ArgParser):
    parser.add_argument('access', choices=list(ACCESS_TYPES))


@site_args.apply()
@user_args.apply(required=True)
@access_args.apply()
@commands.register('database', 'user', 'add', 'access',
                   help='Add an access type to user(s), globally or per-site if --site is provided')
def user_add_access(args: Namespace):
    for user in process_user_args(args):
        add_user_access(user, args.access)


@user_args.apply(required=True)
@site_args.apply()
@access_args.apply()
@commands.register('database', 'user', 'remove', 'access',
                   help='Remove an access type from user(s), globally or per-site if --site is provided')
def user_remove_access(args: Namespace):
    for user in process_user_args(args):
        remove_user_access(user, args.access)


@user_args.apply(required=True)
@site_args.apply(required=True)
@commands.register('database', 'user', 'add', 'site',
                   help='Add user(s) to site')
def user_add_site(args):
    logger = logging.getLogger(__name__)
    for user in args.user:
        try:
            add_site_user(args.site, user)
        except DuplicateSiteUser:
            logger.info(f'User {user} already exists in site {args.site}.')
        if args.create_storage:
            logger.info(f'Creating home storage for {user} in site {args.site}.')
            try:
                create_home_storage(args.site, user)
            except (NotUniqueError, DuplicateKeyError):
                logger.info(f'Home storage for {user} in site {args.site} already exists.')


@user_add_site.args()
def _(parser: ArgParser):
    parser.add_argument('--create-storage', action='store_true', default=False)


@user_args.apply(required=True)
@site_args.apply(required=True)
@commands.register('database', 'user', 'remove', 'site',
                   help='Remove user(s) from site')
def user_remove_site(args):
    logger = logging.getLogger(__name__)
    for user in args.user:
        try:
            remove_site_user(args.site, user)
        except NonExistentSiteUser:
            logger.warning(f'User {user} does not exist in site {args.site}.')


@arggroup()
def user_type_args(parser: ArgParser):
    parser.add_argument('type', choices=list(USER_TYPES))


@user_args.apply(required=True)
@user_type_args.apply()
@commands.register('database', 'user', 'set', 'type',
                   help='Set the type of user(s)')
def user_set_type(args: Namespace):
    logger = logging.getLogger(__name__)
    for user in args.user:
        try:
            set_user_type(user, args.type)
        except GlobalUser.DoesNotExist:
            logger.info(f'User {args.username} does not exist.')


@user_args.apply(required=True)
@site_args.apply(required=True)
@commands.register('database', 'user', 'groups',
                   help='Output the user(s) group memberships in YAML format')
def cmd_user_groups(args: Namespace):
    console = Console()

    output = {}
    for username in args.user:
        output[username] = list(query_user_groups(args.site, username))
    dumped = dumps_yaml(output)
    console.print(highlight_yaml(dumped))


@commands.register('database', 'user', 'index',
                   help='Update user search index.')
def user_index(args: Namespace):
    logger = logging.getLogger(__name__)
    console = Console()
    users = GlobalUser.objects()
    console.print(f'Updating index with {len(users)} users.')
    for user in users:
        UserSearch.update_index(user)


#########################################
#
# group commands: cheeto database group ...
#
#########################################

@commands.register('database', 'group',
                   aliases=['g', 'grp'],
                   help='Operations on groups')
def group_cmd(args: Namespace):
    pass


@commands.register('database', 'group', 'add',
                   help='Add elements to groups, ie: members, sponsors, sudoers, slurmers, or sites.')
def group_add_cmd(args: Namespace):
    pass


@commands.register('database', 'group', 'remove',
                   help='Remove elements from groups, ie: members, sponsors, sudoers, slurmers, or sites.')
def group_remove_cmd(args: Namespace):
    pass


@commands.register('database', 'group', 'new',
                   help='Create a new group')
def group_new_cmd(args: Namespace):
    pass


@arggroup('Group')
def group_args(parser: ArgParser,
               required: bool = False,
               single: bool = False):
    args = ('--groups', '-g')
    if single:
        parser.add_argument(*args, required=required)
    else:
        parser.add_argument(*args, nargs='+', required=required)


@group_args.apply(required=True, single=True)
@site_args.apply()
@commands.register('database', 'group', 'show',
                   help='Show group data.')
def group_show(args: Namespace):
    logger = logging.getLogger(__name__)
    console = Console()
    try:
        if args.site is not None:
            group = SiteGroup.objects.get(groupname=args.groups, sitename=args.site)
        else:
            group = GlobalGroup.objects.get(groupname=args.groups)
    except DoesNotExist:
        scope = 'Global' if args.site is None else args.site
        console.warn(f'Group {args.groups} with scope {scope} does not exist.')
    else:
        output = group.pretty(formatters={'_sponsors': '{data.username} <{data.email}>'}, 
                              lift=['parent'], 
                              order=['groupname', 'gid', 'type', 'members', 'sponsors', 'sudoers', 'slurmers', 'slurm'], 
                              skip=('id', 'sitename', 'iam_synced'))
        console.print(highlight_yaml(output))


@group_args.apply(required=True)
@user_args.apply(required=True)
@site_args.apply(required=True)
@commands.register('database', 'group', 'add', 'member',
                   help='Add user(s) to group(s)')
def cmd_group_add_member(args: Namespace):
    group_add_user_element(args.site, args.groups, args.user, '_members')


@group_args.apply(required=True)
@user_args.apply(required=True)
@site_args.apply(required=True)
@commands.register('database', 'group', 'remove', 'member',
                   help='Remove user(s) from group(s)')
def cmd_group_remove_member(args: Namespace):
    group_remove_user_element(args.site, args.groups, args.user, '_members')


@group_args.apply(required=True)
@site_args.apply(required=True)
@user_args.apply(required=True)
@commands.register('database', 'group', 'add', 'sponsor',
                   help='Add user(s) to group(s) as sponsors')
def cmd_group_add_sponsor(args: Namespace):
    group_add_user_element(args.site, args.groups, args.user, '_sponsors')


@group_args.apply(required=True)
@site_args.apply(required=True)
@user_args.apply(required=True)
@commands.register('database', 'group', 'remove', 'sponsor',
                   help='Remove user(s) from group(s) as sponsors')
def cmd_group_remove_sponsor(args: Namespace):
    group_remove_user_element(args.site, args.groups, args.user, '_sponsors')


@group_args.apply(required=True)
@site_args.apply(required=True)
@user_args.apply(required=True)
@commands.register('database', 'group', 'add', 'sudoer',
                   help='Add user(s) to group(s) as sudoers')
def cmd_group_add_sudoer(args: Namespace):
    group_add_user_element(args.site, args.groups, args.user, '_sudoers')


@group_args.apply(required=True)
@site_args.apply(required=True)
@user_args.apply(required=True)
@commands.register('database', 'group', 'remove', 'sudoer',
                   help='Remove user(s) from group(s) as sudoers')
def cmd_group_remove_sudoer(args: Namespace):
    group_remove_user_element(args.site, args.groups, args.user, '_sudoers')


@group_args.apply(required=True)
@site_args.apply(required=True)
@user_args.apply(required=True)
@commands.register('database', 'group', 'add', 'slurmer',
                   help='Add user(s) to group(s) as slurmers')
def cmd_group_add_slurmer(args: Namespace):
    group_add_user_element(args.site, args.groups, args.user, '_slurmers')


@group_args.apply(required=True)
@site_args.apply(required=True)
@user_args.apply(required=True)
@commands.register('database', 'group', 'remove', 'slurmer',
                   help='Remove user(s) from group(s) as slurmers')
def cmd_group_remove_slurmer(args: Namespace):
    group_remove_user_element(args.site, args.groups, args.user, '_slurmers')


@group_args.apply(required=True)
@site_args.apply(required=True)
@commands.register('database', 'group', 'add', 'site',
                   help='Add a Global group to a site')
def cmd_group_add_site(args: Namespace):
    for group in args.groups:
        add_site_group(group, args.site)


@group_args.apply(required=True)
@site_args.apply(required=True)
@commands.register('database', 'group', 'remove', 'site',
                   help='Remove a Global group from a site')
def cmd_group_remove_site(args: Namespace):
    for group in args.groups:
        remove_site_group(group, args.site)


@group_args.apply(required=True, single=True)
@site_args.apply(single=False)
@commands.register('database', 'group', 'new', 'system',
                   help='Create a new system group within the system ID range on the provided sites')
def cmd_group_new_system(args: Namespace):
    console = Console()

    if args.site == 'all':
        sites = [s.sitename for s in Site.objects()]
    else:
        sites = args.site

    group = create_system_group(args.groups, sitenames=sites)
    console.print(dumps_yaml(group._pretty()))


@arggroup()
def sponsor_args(parser: ArgParser, required: bool = False):
    parser.add_argument('--sponsors', nargs='+', required=required)


@site_args.apply(required=True)
@sponsor_args.apply(required=True)
@commands.register('database', 'group', 'new', 'class',
                   help='Create a new class group within the class ID range and add instructors as sponsors')
def cmd_group_new_class(args: Namespace):
    console = Console()

    sponsors = []
    for sponsor in args.sponsors:
        try:
            sponsors.append(query_user(username=sponsor, sitename=args.site))
        except DoesNotExist:
            console.warn(f'{sponsor} is not a valid user on {args.site}, skipping.')
            continue
    if not sponsors:
        console.error(f'No valid sponsors found, exiting.')
        return ExitCode.DOES_NOT_EXIST
    else:
        lead_sponsor = sponsors[0]
        console.info(f'Using {lead_sponsor.username} as the lead sponsor.')

    prefix = slugify(args.name)
    groupname = f'{prefix}-class'
    try:
        group : SiteGroup = create_class_group(groupname, args.site)
    except (DuplicateKeyError, NotUniqueError):
        console.warn(f'{groupname} already exists on {args.site}, we will add sponsors and students.')
        group : SiteGroup = SiteGroup.objects.get(groupname=groupname, sitename=args.site)

    for sponsor in sponsors:
        add_group_member(args.site, sponsor, group)
        add_group_sponsor(args.site, sponsor, group)

    passwords = []
    for student_num in range(1, args.n_students + 1):
        student = f'{prefix}-user-{student_num:03d}'
        if query_user_exists(student, sitename=args.site):
            console.warn(f'{student} already exists on {args.site}, skipping.')
            continue
        passwords.append((student, password := generate_password()))
        create_class_user(student,
                          lead_sponsor.email,
                          student,
                          password=password,
                          sitename=args.site)
        add_group_member(args.site, student, group)
    
    group.reload()
    console.info(f'Class Information')
    console.print(dumps_yaml(group._pretty(user_format='{user.username} <{user.parent.email}> (uid={user.parent.uid})')))
    if str(args.password_file) == '/dev/stdout':
        console.info(f'Class Passwords')
    else:
        console.info(f'Class Passwords written to {args.file}')
    with args.password_file.open('w') as fp:
        for student, password in passwords:
            fp.write(f'{student} {password}\n')


@cmd_group_new_class.args()
def _(parser: ArgParser):
    parser.add_argument('--name', required=True,
                        help='Class name that will be used as prefix for group name and class accounts')
    parser.add_argument('--n-students', type=int, default=0,
                        help='Number of class accounts to create')
    parser.add_argument('--password-file', type=Path, default='/dev/stdout')


@site_args.apply(required=True)
@group_args.apply(required=True, single=True)
@sponsor_args.apply(required=True)
@commands.register('database', 'group', 'new', 'lab',
                   help='Create a new lab group')
def cmd_group_new_lab(args: Namespace):
    console = Console()

    group : SiteGroup = create_lab_group(args.groups, sitename=args.site)
    for sponsor in args.sponsors:
        if not query_user_exists(sponsor, sitename=args.site):
            console.print(f':warning: [italic dark_orange]{sponsor} is not a valid user on {args.site}, skipping.')
            continue
        add_group_member(args.site, sponsor, group)
        add_group_sponsor(args.site, sponsor, group)

    console.print(group.pretty())


#########################################
#
# slurm commands: cheeto database slurm ...
#
#########################################

@commands.register('database', 'slurm',
                   help='Operations on Slurm')
def slurm_cmd(args: Namespace):
    pass


@commands.register('database', 'slurm', 'new',
                   help='Create new Slurm entities')
def slurm_new_cmd(args: Namespace):
    pass


@commands.register('database', 'slurm', 'remove',
                   help='Remove Slurm entities')
def slurm_remove_cmd(args: Namespace):
    pass


@commands.register('database', 'slurm', 'edit',
                   help='Edit Slurm entities')
def slurm_edit_cmd(args: Namespace):
    pass


@commands.register('database', 'slurm', 'show',
                   help='Show Slurm entities')
def slurm_show_cmd(args: Namespace):
    pass


@arggroup('Slurm QOS')
def slurm_qos_args(parser: ArgParser, required: bool = True):
    parser.add_argument('--group-limits', '-g', type=regex_argtype(QOS_TRES_REGEX))
    parser.add_argument('--user-limits', '-u', type=regex_argtype(QOS_TRES_REGEX))
    parser.add_argument('--job-limits', '-j', type=regex_argtype(QOS_TRES_REGEX))
    parser.add_argument('--priority', default=0, type=int)
    parser.add_argument('--flags', nargs='+')
    parser.add_argument('--qosname', '-n', required=required)


@site_args.apply(required=True)
@slurm_qos_args.apply()
@commands.register('database', 'slurm', 'new', 'qos',
                   help='Create a new QOS')
def cmd_slurm_new_qos(args: Namespace):
    group_limits = SlurmTRES(**parse_qos_tres(args.group_limits))
    user_limits = SlurmTRES(**parse_qos_tres(args.user_limits))
    job_limits = SlurmTRES(**parse_qos_tres(args.job_limits))
    
    qos = create_slurm_qos(args.qosname,
                           args.site,
                           group_limits=group_limits,
                           user_limits=user_limits,
                           job_limits=job_limits,
                           priority=args.priority,
                           flags=args.flags)

    console = Console()
    console.print(highlight_yaml(qos.pretty()))


@site_args.apply(required=True)
@slurm_qos_args.apply()
@commands.register('database', 'slurm', 'edit', 'qos',
                   help='Edit a QOS')
def cmd_slurm_edit_qos(args: Namespace):
    console = Console()
    group_limits = SlurmTRES(**parse_qos_tres(args.group_limits))
    user_limits = SlurmTRES(**parse_qos_tres(args.user_limits))
    job_limits = SlurmTRES(**parse_qos_tres(args.job_limits))

    try:
        qos = SiteSlurmQOS.objects.get(qosname=args.qosname, sitename=args.site)
    except SiteSlurmQOS.DoesNotExist:
        console.print(f'[red] QOS {args.qosname} does not exist.')
        return ExitCode.DOES_NOT_EXIST
    
    update_kwargs = {}
    if group_limits != qos.group_limits:
        update_kwargs['group_limits'] = group_limits
    if user_limits != qos.user_limits:
        update_kwargs['user_limits'] = user_limits
    if job_limits != qos.job_limits:
        update_kwargs['job_limits'] = job_limits
    if args.priority != qos.priority:
        update_kwargs['priority'] = args.priority
    if args.flags != qos.flags:
        update_kwargs['flags'] = args.flags
    
    if update_kwargs:
        qos.update(**update_kwargs)
        qos.reload()
    
    console.print(highlight_yaml(qos.pretty()))


@site_args.apply(required=True)
@commands.register('database', 'slurm', 'remove', 'qos',
                   help='Remove a QOS')
def cmd_slurm_remove_qos(args: Namespace):
    console = Console()
    try:
        qos = SiteSlurmQOS.objects.get(qosname=args.qosname, sitename=args.site)
    except SiteSlurmQOS.DoesNotExist:
        console.print(f'[red] QOS {args.qosname} does not exist.')
        return ExitCode.DOES_NOT_EXIST
    
    qos.delete()
    console.print(f'[green] QOS {args.qosname} removed.')


@site_args.apply(required=False)
@slurm_qos_args.apply(required=False)
@commands.register('database', 'slurm', 'show', 'qos',
                   help='Show QOSes')
def cmd_slurm_show_qos(args: Namespace):
    console = Console(stderr=False)
    query_kwargs = {}
    if args.qosname:
        query_kwargs['qosname'] = args.qosname
    if args.site:
        query_kwargs['sitename'] = args.site
    qos = SiteSlurmQOS.objects(**query_kwargs)
    raw = [qos._pretty() for qos in qos]
    console.print(highlight_yaml(dumps_yaml(raw)))


@cmd_slurm_remove_qos.args()
def _(parser: ArgParser):
    parser.add_argument('--qosname', '-n', required=True)


@arggroup('Slurm Partition')
def slurm_partition_args(parser: ArgParser):
    parser.add_argument('--name', '-n', required=True)


@site_args.apply(required=True)
@slurm_partition_args.apply()
@commands.register('database', 'slurm', 'new', 'partition',
                   help='Create a new Slurm partition')
def cmd_slurm_new_partition(args: Namespace):
    console = Console()
    partition = create_slurm_partition(args.name, args.site)
    console.print(highlight_yaml(partition.pretty()))


@site_args.apply(required=True)
@slurm_partition_args.apply()
@commands.register('database', 'slurm', 'remove', 'partition',
                   help='Remove a Slurm partition')
def cmd_slurm_remove_partition(args: Namespace):
    console = Console()
    partition = SiteSlurmPartition.objects.get(partitionname=args.name, sitename=args.site)
    partition.delete()


@arggroup('Slurm Association')
def slurm_assoc_args(parser: ArgParser, required: bool = True):
    parser.add_argument('--group', '-g', required=required)
    parser.add_argument('--partition', required=required)
    parser.add_argument('--qos', required=required)


@site_args.apply(required=True)
@slurm_assoc_args.apply()
@commands.register('database', 'slurm', 'new', 'assoc',
                   help='Create a new association')
def cmd_slurm_new_assoc(args: Namespace):
    query_site_exists(args.site, raise_exc=True)
    console = Console()

    try:
        assoc = create_slurm_association(args.site,
                                         args.partition,
                                         args.group,
                                         args.qos)
    except SiteSlurmPartition.DoesNotExist:
        console.print(f'[red] Partition {args.partition} does not exist.')
        return ExitCode.DOES_NOT_EXIST
    except SiteGroup.DoesNotExist:
        console.print(f'[red] Group {args.group} does not exist.')
        return ExitCode.DOES_NOT_EXIST
    except SiteSlurmQOS.DoesNotExist:
        console.print(f'[red] QOS {args.qos} does not exist.')
        return ExitCode.DOES_NOT_EXIST

    output = dumps_yaml(_show_slurm_assoc(assoc))
    console.print(highlight_yaml(output))


def _show_slurm_assoc(assoc: SiteSlurmAssociation) -> dict:
    return assoc._pretty(
        lift=['partition'],
        skip=('id',),
        formatters={
            'group': '{data.groupname}',
        }
    )


@site_args.apply(required=False)
@slurm_assoc_args.apply(required=False)
@commands.register('database', 'slurm', 'show', 'assoc',
                   help='Show associations')
def cmd_slurm_show_assoc(args: Namespace):
    console = Console(stderr=False)
    assocs = query_slurm_associations(sitename=args.site,
                                     groupname=args.group,
                                     partitionname=args.partition,
                                     qosname=args.qos)
    raw = [_show_slurm_assoc(assoc) for assoc in assocs]
    console.print(highlight_yaml(dumps_yaml(raw)))



@site_args.apply(required=False)
@slurm_assoc_args.apply(required=False)
@commands.register('database', 'slurm', 'remove', 'assoc',
                   help='Remove associations')
def cmd_slurm_remove_assoc(args: Namespace):
    from rich.prompt import Prompt
    console = Console()
    
    # Query matching associations
    assocs = query_slurm_associations(sitename=args.site,
                                     groupname=args.group,
                                     partitionname=args.partition,
                                     qosname=args.qos)
    
    if not assocs:
        console.warn("No matching associations found")
        return ExitCode.DOES_NOT_EXIST

    # Show associations that will be removed
    console.info("The following associations will be removed:")
    raw = [_show_slurm_assoc(assoc) for assoc in assocs]
    console.print(highlight_yaml(dumps_yaml(raw)))
    
    # Confirm deletion unless force flag is set
    if args.force or Prompt.ask(f"\n{Emotes.QUESTION.value} Do you want to proceed?",
                                choices=["y", "n"],
                                default="n") == "y":
        assocs.delete()
        console.print(f"{Emotes.DONE.value} Associations removed successfully")
    else:
        console.warn(f"{Emotes.STOP.value} Operation cancelled")
        return ExitCode.OPERATION_CANCELLED


@cmd_slurm_remove_assoc.args()
def _(parser: ArgParser):
    parser.add_argument('--force', '-f', action='store_true',
                       help='Skip confirmation prompt')


#########################################
#
# storage commands: cheeto database storage ...
#
#########################################


@commands.register('database', 'storage',
                   help='Operations on storage')
def storage_cmd(args: Namespace):
    pass


@arggroup('Storage')
def storage_query_args(parser: ArgParser):
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--user', '-u')
    group.add_argument('--group', '-g')
    group.add_argument('--name', '-n')
    group.add_argument('--collection', '-c')
    group.add_argument('--host')
    group.add_argument('--automount', '-a')
    


@site_args.apply(required=True)
@storage_query_args.apply()
@commands.register('database', 'storage', 'show',
                   help='Show storage information')
def cmd_storage_show(args: Namespace):
    console = Console(stderr=False)

    storages : list[Storage] = []
    if args.user:
        storages = query_user_storages(sitename=args.site, user=args.user)
    elif args.group:
        storages = query_group_storages(sitename=args.site, group=args.group)
    elif args.name:
        storages = Storage.objects(name=args.name)
    elif args.collection:
        collection = NFSSourceCollection.objects.get(sitename=args.site, name=args.collection)
        sources = StorageMountSource.objects(collection=collection)
        storages = Storage.objects(source__in=sources)
    elif args.host:
        sources = StorageMountSource.objects(sitename=args.site, _host=args.host)
        storages = Storage.objects(source__in=sources)
    elif args.automount:
        storages = query_automap_storages(sitename=args.site, tablename=args.automount)
    else:
        #sources = StorageMountSource.objects(sitename=args.site)
        mounts = StorageMount.objects(sitename=args.site)
        storages = Storage.objects(mount__in=mounts)
    
    for storage in storages:
        console.print(highlight_yaml(storage.pretty()))


@arggroup('Storage Source')
def storage_source_args(parser: ArgParser,
                        required: bool = False):
    parser.add_argument('--owner', required=required)
    parser.add_argument('--group', required=required)
    parser.add_argument('--host', required=required)
    parser.add_argument('--path', required=required)
    parser.add_argument('--collection')
    parser.add_argument('--quota', help='Override default quota from the collection')


@arggroup('Storage Mount')
def storage_mount_args(parser: ArgParser):
    parser.add_argument('--table', required=True)
    parser.add_argument('--options', help='Override mount options from automount map for this mount')
    parser.add_argument('--add-options', help='Add mount options to automount map options for this mount')
    parser.add_argument('--remove-options', help='Remove mount options from automount map options for this mount')
    parser.add_argument('--globus', type=bool, default=False)


@arggroup('Storage')
def storage_common_args(parser: ArgParser,
                        required: bool = False):
    parser.add_argument('--name', required=required)


@storage_common_args.apply(required=True)
@storage_source_args.apply(required=True)
@storage_mount_args.apply()
@site_args.apply(required=True)
@commands.register('database', 'storage', 'new', 'storage',
                   help='Create a new Storage (source and mount)')
def cmd_storage_new_storage(args: Namespace):
    logger = logging.getLogger(__name__)

    if args.collection is None:
        args.collection = args.table
    
    automap = AutomountMap.objects.get(sitename=args.site,  # type: ignore
                                       tablename=args.table)
    collection = ZFSSourceCollection.objects.get(sitename=args.site,  # type: ignore
                                                 name=args.collection)
    owner = GlobalUser.objects.get(username=args.owner)  # type: ignore
    group = GlobalGroup.objects.get(groupname=args.group)  # type: ignore

    source = ZFSMountSource(name=args.name,
                            sitename=args.site,
                            _host=args.host,
                            _host_path=args.path,
                            owner=owner,
                            group=group,
                            collection=collection,
                            _quota=args.quota)

    mount = Automount(name=args.name,
                      sitename=args.site,
                      map=automap,
                      _options=args.options.split(',') if args.options else None,
                      _add_options=args.add_options.split(',') if args.add_options else None,
                      _remove_options=args.remove_options.split(',') if args.remove_options else None)

    try:
        source.save()
    except NotUniqueError:
        logger.error(f'ZFSMountSource with name {args.name} on site {args.site} already exists')
        return ExitCode.NOT_UNIQUE

    try:
        mount.save()
    except NotUniqueError:
        logger.error(f'ZFSMountSource with name {args.name} on site {args.site} already exists')
        source.delete()
        return ExitCode.NOT_UNIQUE
    except:
        source.delete()
        raise

    storage = Storage(name=args.name,
                      source=source,
                      mount=mount,
                      globus=args.globus)
    storage.save()


@storage_common_args.apply(required=True)
@site_args.apply(required=True)
@storage_source_args.apply()
@commands.register('database', 'storage', 'edit', 'source',
                   help='Edit parameters of a storage source')
def cmd_edit_storage_source(args: Namespace):
    logger = logging.getLogger(__name__)
    console = Console()

    source = StorageMountSource.objects.get(name=args.name,
                                            sitename=args.site)
    console.info(f'Updating Storage Source:')
    console.print(highlight_yaml(source.pretty()))
    console.rule()
    update_kwargs = {}
    if args.owner:
        update_kwargs['owner'] = GlobalUser.objects.get(username=args.owner)
    if args.group:
        update_kwargs['group'] = GlobalGroup.objects.get(groupname=args.group)
    if args.host:
        update_kwargs['_host'] = args.host
    if args.path:
        update_kwargs['_host_path'] = args.path
    if args.collection:
        if type(source) is ZFSMountSource:
            new_collection = ZFSSourceCollection.objects.get(sitename=args.site,
                                                             name=args.collection)
        else:
            new_collection = NFSSourceCollection.objects.get(sitename=args.site,
                                                             name=args.collection)
        update_kwargs['collection'] = new_collection
    if args.quota and type(source) is ZFSMountSource:
        update_kwargs['_quota'] = args.quota

    source.update(**update_kwargs)
    console.info('Source Updated:')
    source.reload()
    console.print(highlight_yaml(source.pretty()))


@arggroup('Collection')
def collection_args(parser: ArgParser, required: bool = False):
    parser.add_argument('--host', required=required)
    parser.add_argument('--prefix', required=required)
    parser.add_argument('--quota', required=required, type=regex_argtype(DATA_QUOTA_REGEX))
    parser.add_argument('--options', help='Export options')
    parser.add_argument('--ranges', nargs='+', help='Export IP ranges')
    parser.add_argument('--clone', default=None, help='Clone the existing named collection')


@collection_args.apply()
@site_args.apply(required=True)
@commands.register('database', 'storage', 'new', 'collection',
                   help='Create a new storage Collection')
def cmd_new_collection(args: Namespace):
    logger = logging.getLogger(__name__)
    console = Console()
    query_site_exists(args.site, raise_exc=True)
    col_opts = ['prefix', 'host', 'quota', 'options', 'ranges']

    if args.clone is None and not all(vars(args)[arg] for arg in col_opts):
        console.print('[red] Must specify all attributes when not cloning!')
        return ExitCode.BAD_CMDLINE_ARGS

    col_kwargs = {}

    if args.clone:
        donor = ZFSSourceCollection.objects.get(sitename=args.site, name=args.clone)
        col_kwargs.update(donor.to_dict(raw=False))
        del col_kwargs['_cls']
        col_kwargs['sitename'] = args.site
        col_kwargs['name'] = args.name

    if args.host:
        col_kwargs['_host'] = args.host
    if args.prefix:
        col_kwargs['prefix'] = args.prefix
    if args.quota:
        col_kwargs['_quota'] = args.quota
    if args.options:
        col_kwargs['_export_options'] = args.options
    if args.ranges:
        col_kwargs['_export_ranges'] = args.ranges

    collection = ZFSSourceCollection(**col_kwargs)
    collection.save()

    console.print(highlight_yaml(collection.pretty()))


@cmd_new_collection.args()
def _(parser: ArgParser):
    parser.add_argument('--name', required=True)


@site_args.apply(required=True)
@commands.register('database', 'storage', 'to-puppet',
                   help='Output storage data to Puppet YAML')
def cmd_storage_to_puppet(args: Namespace):
    _storage_to_puppet(args.site, args.puppet_yaml)


@cmd_storage_to_puppet.args()
def _(parser: ArgParser):
    parser.add_argument('puppet_yaml', type=Path)


@commands.register('database', 'iam',
                   help='Data sync from UCD IAM')
def cmd_iam_sync(args: Namespace):
    pass


@commands.register('database', 'iam', 'sync',
                   help='Sync user data from UCD IAM')
def cmd_iam_sync_users(args: Namespace):
    logger = logging.getLogger(__name__)
    console = Console()
    api = IAMAPI(args.config.ucdiam)
    
    users_to_sync = GlobalUser.objects(iam_synced=False,
                                       iam_has_entry__in=[True, None],
                                       type__in=['user', 'admin']).limit(args.max_users)
    console.print(f'Syncing {len(users_to_sync)} users from IAM...')
    for n, user in enumerate(users_to_sync):
        try:
            sync_user_iam(user, api=api)
        except Exception as e:
            logger.error(f'Error syncing user {user.username}: {e}')
            continue


@cmd_iam_sync_users.args()
def _(parser: ArgParser):
    parser.add_argument('--max-users', type=int, default=100,
                        help='Maximum number of users to sync')


def _create_user_from_iam(config: IAMConfig, username: str, email: str, comment: str):
    logger = logging.getLogger(__name__ )

    api = IAMAPI(config.ucdiam)
    info_from_email = api.query_user_iamid_by_email(email)
    info_from_username = api.query_user_iamid(username)
    if info_from_email and info_from_username:
        if info_from_email['iamId'] != info_from_username['iamId']:
            logger.error(f'Username {username} and email {email} do not match.')
            return None
    
    if info_from_email:
        user_info = api.query_user_info(info_from_email['iamId'])
        username = user_info['userId']
        if query_user_exists(username):
            logger.error(f'User {username} already exists.')
            return None

        try:
            with run_in_transaction():
                user, _ = create_user(username,
                                    email,
                                    int(user_info['mothraId']),
                                    fullname=user_info['dFullName'],
                                    status='inactive',
                                    access=[])
                user.iam_id = user_info['iamId']
                user.iam_synced = True
                user.colleges = api.query_user_colleges(user_info['iamId'])
                user.save()
            add_user_comment(user.username, f'Created from IAM data: {comment}')
        except (DuplicateKeyError, NotUniqueError) as e:
            logger.error(f'Duplicate user {username}: {e}')
            logger.error(f'Existing user: {query_user(username)}')
            return None
        else:
            return user
    elif info_from_username:
        logger.warning(f'Found username {username} in IAM but not email {email}.')
    else:
        logger.error('No user found in IAM with the provided username or email.')
    return None


@commands.register('database', 'iam', 'new-user',
                   help='Create a user from IAM data')
def cmd_iam_new_user(args: Namespace):
    logger = logging.getLogger(__name__)
    console = Console()

    user = _create_user_from_iam(args.config.ucdiam,
                                 args.username,
                                 args.email,
                                 args.comment)
    if user:
        console.print('Created User:')
        console.print(highlight_yaml(dumps_yaml(_show_user(user))))


@cmd_iam_new_user.args()
def _(parser: ArgParser):
    parser.add_argument('username')
    parser.add_argument('email')
    parser.add_argument('comment')


@commands.register('database', 'iam', 'new-users',
                   help='Create users from IAM data')
def cmd_iam_new_users(args: Namespace):
    logger = logging.getLogger(__name__)
    console = Console()

    with open(args.users) as fp:
        for row in csv.DictReader(fp, delimiter='\t'):
            user = _create_user_from_iam(args.config,
                                         row['Username'],
                                         row['Email'],
                                         args.comment)
            if user:
                console.print('Created User:')
                console.print(highlight_yaml(dumps_yaml(_show_user(user))))

@cmd_iam_new_users.args()
def _(parser: ArgParser):
    parser.add_argument('users')
    parser.add_argument('comment')