#!/usr/bin/env python
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023-2025
# (c) The Regents of the University of California, Davis, 2023-2025
# File   : database/crud.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 29.01.2025

from collections import defaultdict
from functools import singledispatch
import logging
from operator import attrgetter
from pathlib import Path
from typing import Iterable, Optional
from venv import create

from httpx import get
from mongoengine import DoesNotExist, NotUniqueError
from mongoengine.context_managers import run_in_transaction
from mongoengine.queryset.visitor import Q as Qv

import pyescrypt

from cheeto import iam
from cheeto.hippoapi.models.queued_event_account_model import QueuedEventAccountModel

from ..encrypt import hash_yescrypt, get_mcf_hasher
from ..utils import (TIMESTAMP_NOW,
                    __pkg_dir__,
                    removed,
                    remove_nones)
from ..puppet import MIN_PIGROUP_GID, MIN_SYSTEM_UID
from ..puppet import (PuppetAccountMap, 
                      PuppetGroupRecord,
                      PuppetGroupStorage,
                      PuppetShareRecord,
                      PuppetUserRecord,
                      SlurmQOS as PuppetSlurmQOS,
                      SlurmQOSTRES as PuppetSlurmQOSTRES)
from ..types import (DEFAULT_SHELL,
                     DISABLED_SHELLS,
                     MIN_CLASS_ID,
                     MIN_LABGROUP_ID,
                     MAX_LABGROUP_ID,
                     GROUP_TYPES,
                     SlurmAccount as SlurmAccountTuple, hippo_to_cheeto_access)
from ..yaml import dumps as dumps_yaml

from .user import DuplicateGlobalUser, DuplicateSiteUser, DuplicateUser, NonExistentGlobalUser, NonExistentSiteUser
from .site import Site
from .hippo import HippoEvent
from .user import GlobalUser, SiteUser, User, UserSearch, global_user_t, site_user_t, handle_site_users
from .group import DuplicateGlobalGroup, DuplicateSiteGroup, GlobalGroup, NonExistentGlobalGroup, SiteGroup, global_group_t, site_group_t
from .slurm import SiteSlurmAssociation, SiteSlurmPartition, SiteSlurmQOS, SlurmTRES
from .storage import (Automount,
                      AutomountMap, NonExistentStorage,
                      Storage,
                      StorageMount,
                      StorageMountSource,
                      NFSMountSource,
                      ZFSMountSource,
                      NFSSourceCollection,
                      ZFSSourceCollection)


def create_site(sitename: str,
                fqdn: str):
    if query_site_exists(sitename=sitename):
        raise ValueError(f'Site {sitename} already exists.')
    site = Site(sitename=sitename, fqdn=fqdn)
    site.save()


def query_site_exists(sitename: str, raise_exc: bool = False) -> bool:
    logger = logging.getLogger(__name__)
    try:
        Site.objects.get(sitename=sitename) #type: ignore
    except DoesNotExist:
        if raise_exc:
            logger.error(f'Site {sitename} does not exist.')
            raise
        logger.info(f'Site {sitename} does not exist.')
        return False
    else:
        return True


def query_sitename(site: str):
    logger = logging.getLogger(__name__)
    try:
        Site.objects.get(sitename=site)
    except:
        logger.warning(f'Site {site} does not exist.')
        site = Site.objects.get(fqdn=site)
        return site.sitename
    else:
        return site


def query_user(username: str | list[str] | None = None,
               uid: int | list[int] | None = None,
               email: str | list[str] | None = None,
               sitename: str | None = None) -> User:
    kwargs = {}
    if username is not None:
        kwargs['username'] = username
    if uid is not None:
        kwargs['uid'] = uid
    if email is not None:
        kwargs['email'] = email
    if not kwargs:
        raise DoesNotExist()
    if sitename is not None:
        return SiteUser.objects.get(sitename=sitename, **kwargs)
    else:
        return GlobalUser.objects.get(**kwargs)


def query_user_exists(username: str,
                      sitename: Optional[str] = None,
                      raise_exc: bool = False) -> bool:
    logger = logging.getLogger(__name__)
    try:
        if sitename is None:
            _ = GlobalUser.objects.get(username=username)
        else:
            _ = SiteUser.objects.get(username=username, sitename=sitename)
    except DoesNotExist:
        if raise_exc:
            logger.warning(f'User {username} at site {sitename} does not exist.')
            raise
        return False
    else:
        return True
    

def query_group_exists(groupname: str,
                       sitename: Optional[str] = None):
    logger = logging.getLogger(__name__)
    try:
        if sitename is None:
            _ = GlobalGroup.objects.get(groupname=groupname)
        else:
            _ = SiteGroup.objects.get(groupname=groupname, sitename=sitename)
    except DoesNotExist:
        logger.info(f'Group {groupname} at site {sitename} does not exist.')
        return False
    else:    
        return True


def query_user_type(types: list[str],
                    sitename: str | None = None) -> list[User]:
    global_users = GlobalUser.objects(type__in=types)
    if sitename is not None:
        return SiteUser.objects(parent__in=global_users, sitename=sitename)
    else:
        return global_users


def query_user_access(access: list[str],
                      sitename: str | None = None) -> list[User]:
    global_users = GlobalUser.objects(access__in=access)
    if sitename is not None:
        return SiteUser.objects(parent__in=global_users, sitename=sitename)
    else:
        return global_users


def query_user_status(statuses: list[str],
                      sitename: str | None = None) -> list[User]:
    global_users = GlobalUser.objects(status__in=statuses)
    if sitename is not None:
        return SiteUser.objects(parent__in=global_users, sitename=sitename)
    else:
        return global_users


def tag_comment(comment: str):
    return f'[{TIMESTAMP_NOW}]: {comment}'


def add_user_comment(username: str, comment: str):
    comment = tag_comment(comment)
    GlobalUser.objects(username=username).update_one(push__comments=comment)


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
                         sitename=sitename).update_one(set___status=status)


def set_user_type(username: str,
                  usertype: str):
    GlobalUser.objects(username=username).update_one(set__type=usertype)


def set_user_shell(username: str,
                   shell: str):
    GlobalUser.objects(username=username).update_one(set__shell=shell)


def set_user_password(username: str,
                      password: str,
                      hasher: pyescrypt.Yescrypt):
    password = hash_yescrypt(hasher, password).decode('UTF-8')
    GlobalUser.objects(username=username).update_one(set__password=password)


@singledispatch
def add_user_access(user: SiteUser | GlobalUser | str, access: str | list[str]):
    pass


@add_user_access.register
def _(user: SiteUser, access: str | list[str]):
    user.update(add_to_set___access=access)


@add_user_access.register
def _(user: GlobalUser, access: str | list[str]):
    user.update(add_to_set__access=access)


@singledispatch
def remove_user_access(user: SiteUser | GlobalUser, access: str):
    pass


@remove_user_access.register
def _(user: SiteUser, access: str):
    user.update(pull___access=access)


@remove_user_access.register
def _(user: GlobalUser, access: str):
    user.update(pull__access=access)




def query_site_groups(sitename: str, groupnames: Iterable[str]):
    return SiteGroup.objects(sitename=sitename, groupname__in=set(groupnames))


def handle_site_group(sitename: str, group: site_group_t) -> site_group_t:
    if type(group) is str:
        return SiteGroup.objects.get(sitename=sitename, groupname=group)
    return group


def handle_site_groups(sitename: str, groups: Iterable[site_group_t]):
    to_query = []
    for group in groups:
        if type(group) is SiteGroup:
            yield group
        else:
            to_query.append(group)
    for group in SiteGroup.objects(sitename=sitename, groupname__in=to_query):
        yield group



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
                           _sponsors=[sponsor_user])
    site_group.save()
    logger.info(f'Created SiteGroup from sponsor {sponsor_user.username}: {site_group.to_dict()}')
    return site_group


def query_group_slurm_associations(sitename: str, group: site_group_t):
    if type(group) is str:
        group = SiteGroup.objects.get(sitename=sitename, groupname=group)

    users = (user for user in (set(group._members) | set(group._slurmers)) \
             if 'slurm' in user.access)
    associations = SiteSlurmAssociation.objects(sitename=sitename,
                                                group=group)
    for user in users:
        for assoc in associations:
            yield (user.username, assoc.group.groupname, assoc.partition.partitionname), assoc.qos.qosname


def query_user_groups(sitename: str,
                      user: site_user_t,
                      types: list[str] = GROUP_TYPES):
    if type(user) is str:
        user = SiteUser.objects.get(sitename=sitename, username=user)
    
    qs = SiteGroup.objects(_members=user,
                           sitename=sitename)
    for group in qs:
        if group.parent.type in types:
            yield group.groupname


def query_user_slurmership(sitename: str, user: site_user_t):
    if type(user) is str:
        user = SiteUser.objects.get(sitename=sitename, username=user)
    qs = SiteGroup.objects(_slurmers=user, sitename=sitename).only('groupname')
    for group in qs:
        yield group.groupname


def query_user_sudogroups(sitename: str, user: site_user_t):
    if type(user) is str:
        user = SiteUser.objects.get(sitename=sitename, username=user)
    qs = SiteGroup.objects(_sudoers=user, sitename=sitename).only('groupname')
    for group in qs:
        yield group.groupname


def query_user_slurm(sitename: str, user: site_user_t):
    if type(user) is str:
        user = SiteUser.objects.get(sitename=sitename, username=user)
    groups = SiteGroup.objects(Qv(_members=user, sitename=sitename)
                               | Qv(_slurmers=user, sitename=sitename))

    associations = SiteSlurmAssociation.objects(sitename=sitename,
                                                group__in=groups)
    for assoc in associations:
        yield assoc


def query_user_partitions(sitename: str, user: site_user_t):
    partitions = defaultdict(dict)
    for assoc in query_user_slurm(sitename, user):
        qos = removed(assoc.qos.to_dict(strip_empty=True), 'qosname')
        partitions[assoc.partition.partitionname][assoc.group.groupname] = qos
    return dict(partitions)


def query_user_sponsor_of(sitename: str, user: site_user_t):
    if type(user) is str:
        user = SiteUser.objects.get(sitename=sitename, username=user)
    qs = SiteGroup.objects(_sponsors=user, sitename=sitename).only('groupname')
    for group in qs:
        yield group.groupname


def query_admin_keys(sitename: Optional[str] = None):
    if sitename is not None:
        query = SiteUser.objects(sitename=sitename,
                                 parent__in=GlobalUser.objects(type='admin'))
    else:
        query = GlobalUser.objects(type='admin')
    
    keys = []
    for user in query:
        if 'root-ssh' in user.access:
            keys.extend(user.ssh_key)

    return keys


def query_user_home_storage(sitename: str, user: GlobalUser):
    logger = logging.getLogger(__name__)
    try:
        home_collection = NFSSourceCollection.objects.get(name='home',
                                                          sitename=sitename)
        home_sources = StorageMountSource.objects(collection=home_collection,
                                                  owner=user)
        home_mounts = Automount.objects(sitename=sitename)
        
        return Storage.objects.get(source__in=home_sources,
                                mount__in=home_mounts)
    except DoesNotExist:
        raise NonExistentStorage(f'home storage for {user.username} at {sitename}')
    

def query_user_storages(sitename: str, user: site_user_t):
    if type(user) is str:
        user = GlobalUser.objects.get(username=user)
    else:
        user = user.parent

    return Storage.objects(source__in=StorageMountSource.objects(sitename=sitename,
                                                                 owner=user))


def query_group_storages(sitename: str, group: global_group_t, tablename: Optional[str] = None):
    if type(group) is str:
        group = GlobalGroup.objects.get(groupname=group)
    if tablename:
        mounts = Automount.objects(map=AutomountMap.objects.get(sitename=sitename, tablename='group'))
        return Storage.objects(source__in=StorageMountSource.objects(sitename=sitename, group=group),
                               mount__in=mounts)
    else:
        return Storage.objects(source__in=StorageMountSource.objects(sitename=sitename, group=group))


def query_automap_storages(sitename: str, tablename: str):
    automap = AutomountMap.objects.get(sitename=sitename, tablename=tablename)
    mounts = Automount.objects(map=automap).only('id')

    return Storage.objects(mount__in=mounts)


def query_associated_storages(sitename: str, username: str):
    user = SiteUser.objects.get(sitename=sitename, username=username)
    sitegroups = SiteGroup.objects(sitename=sitename, _members=user).only('parent')
    globalgroups = [sg.parent for sg in sitegroups]

    sources = StorageMountSource.objects(Qv(sitename=sitename) &
                                         (Qv(group__in=globalgroups) | Qv(owner=user.parent)))
    return Storage.objects(source__in=sources)


def query_site_memberships(sitename: str):
    memberships = defaultdict(set)
    slurmerships = defaultdict(set)
    sudoships = defaultdict(set)
    for group in SiteGroup.objects(sitename=sitename):
        for username in group.members:
            memberships[username].add(group.groupname)
        for username in group.slurmers:
            slurmerships[username].add(group.groupname)
        for username in group.sudoers:
            sudoships[username].add(group.groupname)

    return memberships, slurmerships, sudoships


def group_add_user_element(sitename: str,
                           groups: Iterable[site_group_t], 
                           users: Iterable[site_user_t],
                           field: str):
    logger = logging.getLogger(__name__)
    groups = list(handle_site_groups(sitename, groups))
    users = list(handle_site_users(sitename, users))
    #logger.info(groups)
    #logger.info(users)
    for group in groups:
        logger.info(f'{group.groupname}: add {list(map(attrgetter("username"), users))} to {field}')
        group.update(**{f'add_to_set__{field}': users})


def group_remove_user_element(sitename: str,
                              groups: Iterable[site_group_t], 
                              users: Iterable[site_user_t],
                              field: str):
    logger = logging.getLogger(__name__)
    groups = handle_site_groups(sitename, groups)
    users = list(handle_site_users(sitename, users))
    for group in groups:
        logger.info(f'{group.groupname}: remove {list(map(attrgetter("username"), users))} from {field}')
        group.update(**{f'pull_all__{field}': users})


def add_group_member(sitename: str, 
                     user: site_user_t,
                     group: site_group_t):
    group_add_user_element(sitename, [group], [user], '_members')


def remove_group_member(sitename: str,
                        user: site_user_t,
                        group: site_group_t):
    group_remove_user_element(sitename, [group], [user], '_members')


def add_group_sponsor(sitename: str,
                      user: site_user_t,
                      group: site_group_t):
    group_add_user_element(sitename, [group], [user], '_sponsors')


def remove_group_sponsor(sitename: str, 
                         user: site_user_t, 
                         group: site_group_t):
    group_remove_user_element(sitename, [group], [user], '_sponsors')


def add_group_sudoer(sitename: str, 
                     user: site_user_t, 
                     group: site_group_t):
    group_add_user_element(sitename, [group], [user], '_sudoers')


def remove_group_sudoer(sitename: str,
                        user: site_user_t,
                        group: site_group_t):
    group_remove_user_element(sitename, [group], [user], '_sudoers')


def add_group_slurmer(sitename: str,
                      user: site_user_t,
                      group: site_group_t):
    group_add_user_element(sitename, [group], [user], '_slurmers')


def remove_group_slurmer(sitename: str,
                         user: site_user_t,
                         group: site_group_t):
    group_remove_user_element(sitename, [group], [user], '_slurmers')


def add_site_global_slurmer(sitename: str, group: site_group_t):
    if type(group) is str:
        group = SiteGroup.objects.get(sitename=sitename, groupname=group)
    Site.objects(sitename=sitename).update_one(add_to_set__global_slurmers=group)
    Site.objects.get(sitename=sitename).save()


def add_site_global_group(sitename: str, group: site_group_t):
    if type(group) is str:
        group = SiteGroup.objects.get(sitename=sitename, groupname=group)
    Site.objects.get(sitename=sitename).update_one(add_to_set__global_groups=group)
    Site.objects.get(sitename=sitename).save()


def get_next_system_id() -> int:
    ids = set((u.uid for u in GlobalUser.objects(uid__gte=MIN_SYSTEM_UID, 
                                                 uid__lt=MIN_SYSTEM_UID+100000000))) \
        | set((g.gid for g in GlobalGroup.objects(gid__gte=MIN_SYSTEM_UID, 
                                                  gid__lt=MIN_SYSTEM_UID+100000000)))
    if not ids:
        return MIN_SYSTEM_UID
    else:
        return max(ids) + 1


def get_next_class_id() -> int:
    ids = set((u.uid for u in GlobalUser.objects(uid__gte=MIN_CLASS_ID, 
                                                 uid__lt=MIN_CLASS_ID+100000000))) \
        | set((g.gid for g in GlobalGroup.objects(gid__gte=MIN_CLASS_ID, 
                                                  gid__lt=MIN_CLASS_ID+100000000)))
    if not ids:
        return MIN_CLASS_ID
    else:
        return max(ids) + 1


def get_next_lab_id() -> int:
    ids = set((u.uid for u in GlobalUser.objects(uid__gte=MIN_LABGROUP_ID, 
                                                 uid__lt=MAX_LABGROUP_ID))) \
        | set((g.gid for g in GlobalGroup.objects(gid__gte=MIN_LABGROUP_ID, 
                                                  gid__lt=MAX_LABGROUP_ID)))
    if not ids:
        return MIN_LABGROUP_ID
    else:
        return max(ids) + 1


def create_home_storage(sitename: str,
                        user: global_user_t,
                        source: NFSMountSource | None = None):
    logger = logging.getLogger(__name__)
    if type(user) is str:
        user = GlobalUser.objects.get(username=user)
    group = GlobalGroup.objects.get(groupname=user.username)
    collection = ZFSSourceCollection.objects.get(sitename=sitename,
                                                 name='home')
    automap = AutomountMap.objects.get(sitename=sitename,
                                       tablename='home')

    if source is None:
        source = ZFSMountSource(name=user.username,
                                sitename=sitename,
                                owner=user,
                                group=group,
                                collection=collection)
        try:
            source.save()
        except NotUniqueError as e:
            logger.warning(f'source already exists')
            source = ZFSMountSource.objects.get(sitename=sitename, name=user.username)
    else:
        source.update(collection=collection)

    mount = Automount(sitename=sitename,
                      name=user.username,
                      map=automap)
    try:
        mount.save()
    except NotUniqueError as e:
        logger.warning(f'mount {mount.to_dict()} already exists')
        mount = Automount.objects.get(sitename=sitename, name=user.username, map=automap)
    except Exception as e:
        logger.error(f'could not save mount: {mount.to_dict()}')
        source.delete()
        raise

    storage = Storage(name=user.username,
                      source=source,
                      mount=mount)
    storage.save()


def add_site_user(sitename: str, user: global_user_t):
    logger = logging.getLogger(__name__)
    if type(user) is str:
        try:
            user = GlobalUser.objects.get(username=user)
        except DoesNotExist:
            raise NonExistentGlobalUser(user)
    try:
        group = GlobalGroup.objects.get(groupname=user.username)
    except DoesNotExist:
        raise NonExistentGlobalGroup(user.username)

    logger.info(f'Adding user {user.username} to site {sitename}')

    if query_user_exists(user.username, sitename):
        raise DuplicateSiteUser(user.username, sitename)

    with run_in_transaction():
        site_user = SiteUser(username=user.username,
                             sitename=sitename,
                             parent=user)
        site_user.save(force_insert=True)

        site_group = SiteGroup(groupname=user.username,
                               sitename=sitename,
                               parent=group,
                               _members=[site_user])
        site_group.save(force_insert=True)

    logger.info(f'Created SiteUser {user.username} on site {sitename}')

    return site_user, site_group


def remove_site_user(sitename: str, user: site_user_t):
    logger = logging.getLogger(__name__)
    if type(user) is str:
        try:
            user = SiteUser.objects.get(sitename=sitename, username=user)
        except DoesNotExist:
            raise NonExistentSiteUser(user, sitename)
    with run_in_transaction():
        user.delete()
        try:
            group = SiteGroup.objects.get(sitename=sitename, groupname=user.username)
        except DoesNotExist:
            logger.warning(f'SiteGroup {user.username} does not exist on site {sitename}')
        else:
            group.delete()

    logger.info(f'Removed SiteUser {user.username} from site {sitename}')


def create_user(username: str,
                email: str,
                uid: int,
                fullname: str,
                type: str = 'user',
                shell: str = DEFAULT_SHELL,
                status: str = 'active',
                password: str | None = None,
                ssh_key: list[str] | None = None,
                access: list[str] | None = None,
                sitenames: list[str] | None = None,
                gid: int | None = None,
                iam_id: int | None = None):
    logger = logging.getLogger(__name__)

    if query_user_exists(username, raise_exc=False):
        raise DuplicateGlobalUser(username)
    
    if gid is None:
        gid = uid
    
    user_kwargs = dict(
        username=username,
        email=email,
        uid=uid,
        gid=gid,
        fullname=fullname,
        type=type,
        shell=shell,
        status=status,
        home_directory=f'/home/{username}'
    )
    if access is not None:
        user_kwargs['access'] = access
    if ssh_key is not None:
        user_kwargs['ssh_key'] = ssh_key
    if iam_id is not None:
        user_kwargs['iam_id'] = iam_id
    
    with run_in_transaction():

        global_user = GlobalUser(**user_kwargs)
        global_user.save(force_insert=True)

        UserSearch.update_index(global_user)

        if password is not None:
            hasher = get_mcf_hasher()
            set_user_password(username, password, hasher)

        global_group = GlobalGroup(groupname=username,
                                gid=global_user.gid,
                                type='user',
                                user=global_user)
        global_group.save(force_insert=True)

        if sitenames is not None:
            for sitename in sitenames:
                add_site_user(sitename, global_user)

    return global_user, global_group


def create_user_from_hippo(hippo_data: QueuedEventAccountModel,
                           use_access: bool = False):
    return create_user(hippo_data.kerberos,
                       hippo_data.email,
                       int(hippo_data.mothra),
                       hippo_data.name,
                       ssh_key=[hippo_data.key],
                       access=list(hippo_to_cheeto_access(hippo_data.access_types)) if use_access else None,
                       iam_id=int(hippo_data.iam))



def create_system_user(username: str,
                       email: str,
                       fullname: str,
                       password: str | None = None):
    uid = get_next_system_id()
    create_user(username,
                email,
                uid,
                fullname,
                type='system',
                password=password,
                access=['login-ssh', 'compute-ssh'])


def create_class_user(username: str,
                      email: str,
                      fullname: str,
                      password: str | None = None,
                      sitename: str | None = None):
    uid = get_next_class_id()
    create_user(username,
                email,
                uid,
                fullname,
                type='class',
                password=password,
                access=['login-ssh', 'compute-ssh'],
                sitenames=[sitename])


def create_group(groupname: str,
                 gid: int,
                 type: str = 'group',
                 sites: list[str] | None = None):
    logger = logging.getLogger(__name__)
    if query_group_exists(groupname):
        raise DuplicateGlobalGroup(groupname)
    
    global_group = GlobalGroup(groupname=groupname,
                               gid=gid,
                               type=type)
    global_group.save(force_insert=True)
    logger.info(f'Created GlobalGroup {groupname} gid={gid}')

    if sites is not None:
        for sitename in sites:
            if query_group_exists(groupname, sitename):
                # This state should never be reached, but just in case
                raise DuplicateSiteGroup(groupname, sitename)

            site_group = SiteGroup(groupname=groupname,
                                   sitename=sitename,
                                   parent=global_group)
            site_group.save(force_insert=True)
            logger.info(f'Created SiteGroup {groupname} for site {sitename}')

    return global_group


def create_system_group(groupname: str, sitenames: Optional[list[str]] = None):
    return create_group(groupname,
                        get_next_system_id(),
                        type='system',
                        sites=sitenames)


def create_class_group(groupname: str, sitename: str) -> SiteGroup:
    global_group = create_group(groupname,
                                get_next_class_id(),
                                type='class',
                                sites=[sitename])

    return SiteGroup.objects.get(groupname=groupname, sitename=sitename)


def create_lab_group(groupname: str, sitename: str | None = None):
    global_group = create_group(groupname,
                                get_next_lab_id(),
                                type='group',
                                sites=[sitename] if sitename is not None else None)
    if sitename is not None:
        return SiteGroup.objects.get(groupname=groupname, sitename=sitename)
    else:
        return global_group


def add_site_group(group: global_group_t, sitename: str):
    if type(group) is str:
        group = GlobalGroup.objects.get(groupname=group)

    if query_group_exists(group.groupname, sitename):
        raise DuplicateSiteGroup(group.groupname, sitename)

    SiteGroup(groupname=group.groupname,
              sitename=sitename,
              parent=group).save(force_insert=True)


def remove_site_group(group: global_group_t, sitename: str):
    if type(group) is str:
        group = GlobalGroup.objects.get(groupname=group)
    SiteGroup.objects.get(parent=group, sitename=sitename).delete()


def create_slurm_partition(partitionname: str, sitename: str):
    logger = logging.getLogger(__name__)
    logger.info(f'Create partition {partitionname} on site {sitename}.')
    query_site_exists(sitename, raise_exc=True)

    try:
        partition = SiteSlurmPartition(sitename=sitename, partitionname=partitionname)
        partition.save()
    except Exception as e:
        logger.error(f'Error creating partition {partitionname} on site {sitename}: {e.__class__} {e}')
        partition = SiteSlurmPartition.objects.get(sitename=sitename, partitionname=partitionname)

    return partition


def create_slurm_qos(qosname: str,
                     sitename: str,
                     group_limits: Optional[SlurmTRES] = None,
                     user_limits: Optional[SlurmTRES] = None,
                     job_limits: Optional[SlurmTRES] = None,
                     priority: int = 0,
                     flags: Optional[set[str]] = None):

    logger = logging.getLogger(__name__)
    logger.info(f'Create QOS {qosname} on site {sitename}.')
    try:
        qos = SiteSlurmQOS(qosname=qosname,
                           sitename=sitename,
                           group_limits=group_limits,
                           user_limits=user_limits,
                           job_limits=job_limits,
                           priority=priority,
                           flags=list(flags) if flags is not None else None)
        qos.save()
    except Exception as e:
        logger.error(f'Error creating QOS {qosname}: {e.__class__} {e}')
        qos = SiteSlurmQOS.objects.get(qosname=qosname, sitename=sitename)

    return qos


def create_slurm_association(sitename: str, partitionname: str, groupname: str, qosname: str):
    logger = logging.getLogger(__name__)
    logger.info(f'Create Association ({groupname}, {partitionname}, {qosname}) on site {sitename}.')
    query_site_exists(sitename, raise_exc=True)

    partition = SiteSlurmPartition.objects.get(partitionname=partitionname, sitename=sitename)
    group = SiteGroup.objects.get(sitename=sitename, groupname=groupname)
    qos = SiteSlurmQOS.objects.get(qosname=qosname, sitename=sitename)

    try:
        assoc = SiteSlurmAssociation.objects.get(sitename=sitename,
                                                 qos=qos,
                                                 group=group,
                                                 partition=partition)
    except DoesNotExist:
        assoc = SiteSlurmAssociation(sitename=sitename,
                                     qos=qos,
                                     group=group,
                                     partition=partition)
        assoc.save()

    return assoc


def query_slurm_associations(sitename: str | None = None, 
                            qosname: str | None = None, 
                            partitionname: str | None = None, 
                            groupname: str | None = None):
    query_kwargs = {}
    if sitename is not None:
        query_kwargs['sitename'] = sitename
    if qosname is not None:
        query_kwargs['qos__in'] = SiteSlurmQOS.objects(qosname=qosname)
    if partitionname is not None:
        query_kwargs['partition__in'] = SiteSlurmPartition.objects(partitionname=partitionname)
    if groupname is not None:
        query_kwargs['group__in'] = SiteGroup.objects(groupname=groupname)
        
    return SiteSlurmAssociation.objects(**query_kwargs)


def slurm_qos_state(sitename: str) -> dict[str, SiteSlurmQOS]:
    return {s.qosname: s.to_puppet() for s in SiteSlurmQOS.objects(sitename=sitename)}


def slurm_association_state(sitename: str, ignore: set[str] = {'root'}):
    logger = logging.getLogger(__name__)

    state = dict(users={}, accounts={})

    for group in SiteGroup.objects(sitename=sitename,
                                   parent__in=GlobalGroup.objects(type__in=['group', 'class'])):

        assocs = False
        for assoc_tuple, qos_name in query_group_slurm_associations(sitename, group):
            state['users'][assoc_tuple] = qos_name
            assocs = True
        
        if assocs:
            account = SlurmAccountTuple(max_user_jobs=group.slurm.max_user_jobs,
                                   max_submit_jobs=group.slurm.max_submit_jobs,
                                   max_group_jobs=group.slurm.max_group_jobs,
                                   max_job_length=group.slurm.max_job_length)
            state['accounts'][group.groupname] = account

    return state



def user_to_puppet(user: SiteUser,
                   groups: set[str], 
                   slurm: set[str], 
                   group_sudo: set[str]):
    logger = logging.getLogger(__name__)
    logger.info(f'convert user {user.username} to legacy puppet format.')
    groups.remove(user.username)
    slurm = {'account': sorted(slurm)} if slurm else None
    
    tags = set()
    if 'compute-ssh' in user.access:
        tags.add('ssh-tag')
    if 'root-ssh' in user.access:
        tags.add('root-ssh-tag')
    if 'sudo' in user.access:
        tags.add('sudo-tag')
    if user.type == 'system':
        tags.add('system-tag')

    shell = user.shell
    if shell in DISABLED_SHELLS:
        shell = '/usr/bin/bash'
    if user.status in ('inactive', 'disabled'):
        shell = '/usr/sbin/nologin-account-disabled'

    try:
        home_storage = query_user_home_storage(user.sitename, user.parent)
    except (DoesNotExist, NonExistentStorage):
        storage = None
        logger.warning(f'No home storage found for {user.username} at {user.sitename}')
    else:
        storage = dict(autofs={'nas': home_storage.host,
                               'path': str(home_storage.host_path.parent)},
                       zfs=get_puppet_zfs(home_storage))
    user_data = dict(fullname=user.fullname,
                     email=user.email,
                     uid=user.uid,
                     gid=user.gid,
                     groups=sorted(groups),
                     group_sudo=sorted(group_sudo),
                     shell=user.shell,
                     tag=sorted(tags),
                     home=user.home_directory,
                     password=user.password,
                     expiry=str(user.expiry) if user.expiry else None,
                     slurm=slurm,
                     storage=storage)
    remove_nones(user_data)
    try:
        return PuppetUserRecord.load(user_data)
    except:
        logger.error(f'PuppetUserRecord validation error loading {user_data}')
        return None


def get_puppet_zfs(storage: Storage):
    if type(storage.source) is ZFSMountSource and storage.source.quota:
        return {'quota': storage.source.quota}
    else:
        return False


def group_to_puppet(group: SiteGroup):
    storages = query_group_storages(group.sitename,
                                    group.parent,
                                    tablename='group')
    storages_data = [
        {'name': s.name,
         'owner': s.owner,
         'group': s.group,
         'autofs': {
            'nas': s.host,
            'path': str(s.host_path),
            'options': ','.join(sorted(set(s.mount_options) - {'fstype=nfs'}, reverse=True))
         },
         'zfs': get_puppet_zfs(s),
         'globus': s.globus
        } for s in storages
    ]

    return PuppetGroupRecord.load(dict(gid=group.gid,
                                       sponsors=sorted(group.sponsors),
                                       storage=storages_data))


def share_to_puppet(share: Storage):
    storage_data = {
        'owner': share.owner,
        'group': share.group,
        'autofs': {
            'nas': share.host,
            'path': str(share.host_path)},
        'zfs': get_puppet_zfs(share)
    }

    return PuppetShareRecord.load(dict(storage=storage_data))


def site_to_puppet(sitename: str) -> PuppetAccountMap:
    logger = logging.getLogger(__name__)
    logger.info(f'convert site {sitename} to legacy puppet format.')
    
    logger.info(f'creating membership map')
    memberships, slurmerships, sudoships = query_site_memberships(sitename)

    logger.info(f'converting users')
    users = {}
    for user in SiteUser.objects(sitename=sitename).order_by('username'):
        record = user_to_puppet(user,
                                memberships.get(user.username, set()),
                                slurmerships.get(user.username, set()),
                                sudoships.get(user.username, set()))
        if record:
            users[user.username] = record

    logger.info(f'converting groups')
    groups = {}
    for group in SiteGroup.objects(sitename=sitename).order_by('groupname'):
        if group.parent.type == 'user':
            user = SiteUser.objects.get(username=group.groupname, sitename=sitename)
            if user.uid == group.gid:
                continue
        groups[group.groupname] = group_to_puppet(group)

    logger.info(f'converting shares')
    shares = {}
    storages = query_automap_storages(sitename, 'share')
    for share in storages.order_by('name'):
        shares[share.name] = share_to_puppet(share)

    return PuppetAccountMap(user=users,
                            group=groups,
                            share=shares)


def _storage_to_puppet(sitename: str, output: Path):
    zfs = dict(group=defaultdict(list),
               user=defaultdict(list),
               share=defaultdict(list))
    nfs = dict(group=defaultdict(list),
               user=defaultdict(list),
               share=defaultdict(list))

    def add_storage(s, key, perms='2770'):
        data = dict(name=s.name,
                    owner=s.owner, 
                    group=s.group, 
                    path=str(s.host_path), 
                    export_options=s.source.export_options, 
                    export_ranges=s.source.export_ranges)
        if s.source._cls == 'StorageMountSource.NFSMountSource.ZFSMountSource':
            data['quota'] = s.quota
            data['permissions'] = perms
            zfs[key][s.host].append(data)
        else:
            nfs[key][s.host].append(data)

    for table, key, perms in (('home', 'user', '0770'),
                              ('group', 'group', '2770'),
                              ('share', 'share', '2775')):
        for s in query_automap_storages(sitename, table):
            add_storage(s, key, perms)

    puppet = {'zfs': zfs}
    if nfs:
        puppet['nfs'] = nfs

    with output.open('w') as fp:
        print(dumps_yaml(puppet), file=fp)



#####################################################################################################
# LEGACY
#####################################################################################################


def load_share_from_puppet(shares: dict[str, PuppetShareRecord],
                           sitename: str,
                           mount_source_site: str | None = None):
    logger = logging.getLogger(__name__)
    logger.info(f'Load share storages on site {sitename}')

    collection = ZFSSourceCollection.objects.get(sitename=sitename if mount_source_site is None \
                                                          else mount_source_site,
                                                 name='share')
    automap = AutomountMap.objects.get(sitename=sitename,
                                       tablename='share')

    for share_name, share in shares.items():
        owner = GlobalUser.objects.get(username=share.storage.owner)
        group = GlobalGroup.objects.get(groupname=share.storage.group)
        if share.storage.zfs:
            source_type = ZFSMountSource
        else:
            source_type = NFSMountSource

        with run_in_transaction():

            if mount_source_site is None:

                source_args = dict(name=share_name,
                                   sitename=sitename,
                                   _host_path=share.storage.autofs.path,
                                   _host=share.storage.autofs.nas,
                                   owner=owner,
                                   group=group,
                                   collection=collection)

                if source_type is ZFSMountSource:
                    source_args['_quota'] = share.storage.zfs.quota

                source = source_type(**source_args)

                try:
                    source.save()
                except NotUniqueError:
                    source = source_type.objects.get(sitename=sitename,
                                                     name=share_name)
            else:
                try:
                    source = source_type.objects.get(sitename=mount_source_site,
                                                     name=share_name)
                except DoesNotExist:
                    logger.error(f'Does not exist: sitename={mount_source_site}, name={share.storage.autofs.path}')
                    raise

            mount = Automount(sitename=sitename,
                              name=share_name,
                              map=automap,
                              _options=share.storage.autofs.split_options())
            try:
                mount.save()
            except NotUniqueError:
                mount = Automount.objects.get(sitename=sitename,
                                              name=share_name,
                                              map=automap)

            storage = Storage(name=share_name,
                              source=source,
                              mount=mount)

            try:
                storage.save()
            except NotUniqueError:
                pass


def load_group_storages_from_puppet(storages: list[PuppetGroupStorage],
                                    groupname: str,
                                    sitename: str,
                                    mount_source_site: str | None = None):

    logger = logging.getLogger(__name__)
    logger.info(f'Load storages for group {groupname} on site {sitename}')

    collection = ZFSSourceCollection.objects.get(sitename=sitename if mount_source_site is None \
                                                          else mount_source_site,
                                                 name='group')
    automap = AutomountMap.objects.get(sitename=sitename,
                                       tablename='group')

    for storage in storages:

        owner = GlobalUser.objects.get(username=storage.owner)
        if storage.group:
            group = GlobalGroup.objects.get(groupname=storage.group)
        else:
            group = GlobalGroup.objects.get(groupname=groupname)

        if not storage.zfs:
            source_type = NFSMountSource
        else:
            source_type = ZFSMountSource

        with run_in_transaction():
            if mount_source_site is None:
                if source_type is NFSMountSource:
                    source = NFSMountSource(name=storage.name,
                                            sitename=sitename,
                                            _host_path=storage.autofs.path,
                                            _host=storage.autofs.nas,
                                            owner=owner,
                                            group=group,
                                            collection=collection)
                    try:
                        source.save()
                    except NotUniqueError:
                        source = NFSMountSource.objects.get(sitename=sitename,
                                                            _host=storage.autofs.nas,
                                                            _host_path=storage.autofs.path)
                                                            
                else:
                    source = ZFSMountSource(name=storage.autofs.path,
                                            sitename=sitename,
                                            _host=storage.autofs.nas,
                                            _host_path=storage.autofs.path,
                                            owner=owner,
                                            group=group,
                                            _quota=storage.zfs.quota,
                                            collection=collection)
                    try:
                        source.save()
                    except NotUniqueError:
                        source = ZFSMountSource.objects.get(sitename=sitename,
                                                            _host=storage.autofs.nas,
                                                            _host_path=storage.autofs.path)
            else:
                source = source_type.objects.get(sitename=mount_source_site,
                                                 _host=storage.autofs.nas,
                                                 _host_path=storage.autofs.path)

            mount = Automount(sitename=sitename,
                              map=automap,
                              name=storage.name,
                              _options=storage.autofs.split_options())
            try:
                mount.save()
            except NotUniqueError:
                mount = Automount.objects.get(sitename=sitename,
                                              map=automap,
                                              name=storage.name)
            
            record = Storage(name=storage.name,
                             source=source,
                             mount=mount,
                             globus=storage.globus)
            try:
                record.save()
            except NotUniqueError:
                pass


def load_slurm_from_puppet(sitename: str, data: PuppetAccountMap):
    logger = logging.getLogger(__name__)
    from .slurm import get_qos_name

    partitions = set()
    qos_map = {}
    qos_references = []
    for group_name, group in data.group.items():
        if group.slurm is None or group.slurm.partitions is None:
            continue
        for partition_name, partition in group.slurm.partitions.items():
            partitions.add(partition_name)
            # If it is a reference to another QOS, it will be put in the map
            # where it is defined
            if type(partition.qos) is str:
                qos_references.append((group_name, partition_name, partition.qos))
                continue
            qos_name = get_qos_name(group_name, partition_name)
            qos_map[qos_name] = (group_name, partition_name, partition.qos)

    # Validate that all QOS references actually exist
    for group_name, partition_name, qos_name in qos_references:
        if qos_name not in qos_map:
            raise ValueError(f'{group_name} has invalid QoS for {partition_name}: {qos_name}')

    for partitionname in partitions:
        create_slurm_partition(partitionname=partitionname, sitename=sitename)

    for qosname, (groupname, partitionname, puppet_qos) in qos_map.items():
        qos = SiteSlurmQOS.from_puppet(qosname, sitename, puppet_qos)
        try:
            qos.save()
        except:
            logger.info(f'QOS {qosname} already exists')

        create_slurm_association(sitename, partitionname, groupname, qosname)

    for groupname, partitionname, qosname in qos_references:
        create_slurm_association(sitename, partitionname, groupname, qosname)

    for username, user in data.user.items():
        if user.slurm is not None and user.slurm.account is not None:
            for groupname in user.slurm.account:
                add_group_slurmer(sitename, username, groupname)

