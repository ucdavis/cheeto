#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023-2025
# (c) The Regents of the University of California, Davis, 2023-2025
# File   : database/ldap.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 30.01.2025

import logging

from ..config import Config
from ..ldap import LDAPCommitFailed, LDAPManager, LDAPUser, LDAPGroup

from .site import Site
from .user import GlobalUser, SiteUser
from .group import GlobalGroup, SiteGroup
from .crud import query_automap_storages, query_admin_keys, DoesNotExist


def ldap_sync(sitename: str, config: Config, force: bool = False):
    logger = logging.getLogger(__name__)
    ldap_mgr = LDAPManager(config.ldap, pool_keepalive=15, pool_lifetime=30)
    site = Site.objects.get(sitename=sitename)

    for user in SiteUser.objects(sitename=sitename):
        if ldap_sync_globaluser(user.parent, ldap_mgr, force=force):
            user.ldap_synced = False
            user.save()
            user.reload()

    for group in SiteGroup.objects(sitename=sitename):
        ldap_sync_group(group, ldap_mgr, force=force)

    for user in SiteUser.objects(sitename=sitename):
        ldap_sync_siteuser(user, ldap_mgr, force=force)

    try:
        for storage in query_automap_storages(sitename, 'home'):
            logger.info(f'sync home storage {storage.name}') 
            host = f'{storage.host}${{HOST_SUFFIX}}'
            ldap_mgr.connection.delete(ldap_mgr.get_automount_dn(storage.owner,
                                                                'home',
                                                                sitename))
            ldap_mgr.add_home_automount(storage.owner,
                                        sitename,
                                        host,
                                        storage.host_path,
                                        '-' + ','.join(storage.mount_options))
    except DoesNotExist:
        logger.warning(f'No home storages found for site {sitename}')

    try:
        for storage in query_automap_storages(sitename, 'group'):
            host = f'{storage.host}${{HOST_SUFFIX}}'
            ldap_mgr.connection.delete(ldap_mgr.get_automount_dn(storage.name,
                                                                'group',
                                                                sitename))
            ldap_mgr.add_group_automount(storage.name,
                                        sitename,
                                        host,
                                        storage.host_path,
                                        '-' + ','.join(storage.mount_options))
    except DoesNotExist:
        logger.warning(f'No group storages found for site {sitename}')


def ldap_sync_group(group: SiteGroup, mgr: LDAPManager, force: bool = False):
    logger = logging.getLogger(__name__)
    if not force and (group.ldap_synced and group.parent.ldap_synced):
        logger.info(f'Group {group.groupname} does not need to be synced')
        return

    if force:
        logger.info(f'force sync {group.groupname}, deleting existing dn')
        mgr.delete_dn(mgr.get_group_dn(group.groupname, group.sitename))
    
    logger.info(f'sync {group.groupname}') 

    if not mgr.group_exists(group.groupname, group.sitename):
        mgr.add_group(LDAPGroup.load(dict(groupname=group.groupname,
                                          gid=group.parent.gid,
                                          members=group.members)),
                      group.sitename)
        return

    special_groups = set(mgr.config.user_access_groups.values()) | set(mgr.config.user_status_groups.values())
    if group.groupname in special_groups:
        logger.info(f'Skip sync for special group {group.groupname}')
        return

    ldap_group = mgr.query_group(group.groupname, group.sitename)
    to_remove = ldap_group.members - set(group.members)
    to_add = set(group.members) - ldap_group.members

    mgr.remove_users_from_group(to_remove, group.groupname, group.sitename)
    mgr.add_user_to_group(to_add, group.groupname, group.sitename)

    group.ldap_synced = True
    group.parent.ldap_synced = True

    group.save()
    group.parent.save()
    group.reload()


def ldap_sync_globaluser(user: GlobalUser, mgr: LDAPManager, force: bool = False):
    logger = logging.getLogger(__name__)
    if not force and user.ldap_synced:
        logger.info(f'GlobalUser {user.username} does not need to be synced.')
        return False
    logger.info(f'sync {user.username}')

    if force:
        logger.info(f'force sync {user.username}, deleting existing dn')
        mgr.delete_user(user.username)

    data = dict(email=user.email,
                uid=user.uid,
                gid=user.gid,
                shell=user.shell,
                home_directory=user.home_directory,
                fullname=user.fullname,
                password=f'{{CRYPT}}{user.password}' if user.password else '',
                surname=user.fullname.split()[-1]) #type: ignore

    if user.ssh_key:
        data['ssh_keys'] = user.ssh_key
    
    try:
        if mgr.user_exists(user.username):
            mgr.update_user(user.username, **data)
        else:
            data['username'] = user.username
            mgr.add_user(LDAPUser.load(data))
    except LDAPCommitFailed as e:
        logger.error(f'Failed to sync GlobalUser {user.username}: {e}')
    else:
        user.ldap_synced = True
        user.save()
        user.reload()
    
    return True


def ldap_sync_siteuser(user: SiteUser, mgr: LDAPManager, force: bool = False):
    logger = logging.getLogger(__name__)
    if not force and (user.ldap_synced and user.parent.ldap_synced):
        logger.info(f'SiteUser {user.username} does not need to be synced.')
        return

    if not mgr.user_exists(user.username):
        ldap_sync_globaluser(user.parent, mgr, force=force)

    ldap_groups = mgr.query_user_memberships(user.username, user.sitename)

    for status, groupname in mgr.config.user_status_groups.items():
        if status == user.status and groupname not in ldap_groups:
            logger.info(f'add status {status} for {user.username}')
            mgr.add_user_to_group(user.username, groupname, user.sitename)
        if status != user.status and groupname in ldap_groups:
            logger.info(f'remove status {status} for {user.username}')
            mgr.remove_users_from_group([user.username], groupname, user.sitename)

    for access, groupname in mgr.config.user_access_groups.items():
        if access in user.access and groupname not in ldap_groups:
            logger.info(f'add access {access} for {user.username}')
            mgr.add_user_to_group(user.username, groupname, user.sitename)
        if access not in user.access and groupname in ldap_groups:
            logger.info(f'remove access {access} for {user.username}')
            mgr.remove_users_from_group([user.username], groupname, user.sitename)

    if user.type == 'system':
        keys = list(set(query_admin_keys(sitename=user.sitename) + user.ssh_key)) #type: ignore
        mgr.update_user(user.username, ssh_keys=keys)

    user.ldap_synced = True
    user.save()
    user.reload()