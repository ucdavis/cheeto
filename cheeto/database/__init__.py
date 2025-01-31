#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023-2025
# (c) The Regents of the University of California, Davis, 2023-2025
# File   : database/__init__.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 30.01.2025

from .base import (
    connect_to_database,
    InvalidUser,
    DuplicateUser,
    SyncQuerySet,
    BaseDocument,
    handler
)
from .site import Site
from .hippo import HippoEvent
from .user import GlobalUser, SiteUser, UserSearch, User
from .group import GlobalGroup, SiteGroup, SiteSlurmAccount
from .slurm import (
    SiteSlurmAssociation, 
    SiteSlurmPartition, 
    SiteSlurmQOS,
    SlurmTRES
)
from .storage import (Automount,
                      AutomountMap,
                      Storage,
                      StorageMount,
                      StorageMountSource,
                      NFSMountSource,
                      ZFSMountSource,
                      NFSSourceCollection,
                      ZFSSourceCollection)
from .crud import (
    create_site,
    query_site_exists,
    query_sitename,
    query_user,
    query_user_exists,
    query_user_type,
    query_user_access,
    query_user_status,
    tag_comment,
    add_user_comment,
    set_user_shell,
    set_user_status,
    set_user_type,
    set_user_password,
    add_user_access,
    remove_user_access,
    query_site_groups,
    handle_site_group,
    handle_site_groups,
    create_group_from_sponsor,
    query_group_slurm_associations,
    query_user_groups,
    query_user_slurmership,
    query_user_sudogroups,
    query_user_slurm,
    query_user_partitions,
    query_user_sponsor_of,
    query_admin_keys,
    query_user_home_storage,
    query_user_storages,
    query_group_storages,
    query_automap_storages,
    query_associated_storages,
    query_site_memberships,
    group_add_user_element,
    group_remove_user_element,
    add_group_member,
    remove_group_member,
    add_group_sponsor,
    remove_group_sponsor,
    add_group_sudoer,
    remove_group_sudoer,
    add_group_slurmer,
    remove_group_slurmer,
    add_site_global_slurmer,
    add_site_global_group,
    get_next_system_id,
    get_next_class_id,
    get_next_lab_id,
    create_home_storage,
    add_site_user,
    create_user,
    create_system_user,
    create_class_user,
    create_system_group,
    create_class_group,
    create_lab_group,
    add_site_group,
    create_slurm_partition,
    create_slurm_qos,
    create_slurm_association,
    slurm_qos_state,
    slurm_association_state,
    user_to_puppet,
    get_puppet_zfs,
    group_to_puppet,
    share_to_puppet,
    site_to_puppet,
    _storage_to_puppet,
    load_share_from_puppet,
    load_group_storages_from_puppet,
    load_slurm_from_puppet
) 
                   

from .ldap import ldap_sync, ldap_sync_globaluser, ldap_sync_group, ldap_sync_siteuser

COLLECTIONS = [Site,
               HippoEvent,
               GlobalUser,
               SiteUser,
               UserSearch,
               GlobalGroup,
               SiteGroup,
               SiteSlurmAssociation,
               SiteSlurmPartition,
               SiteSlurmQOS,
               Automount,
               AutomountMap,
               Storage,
               StorageMount,
               StorageMountSource,
               NFSMountSource,
               ZFSMountSource,
               NFSSourceCollection,
               ZFSSourceCollection]


def purge_database():
    prompt = input('WARNING: YOU ARE ABOUT TO PURGE THE DATABASE. TYPE "PURGE" TO CONTINUE: ')
    if prompt != 'PURGE':
        print('Aborting!')
        return

    for collection in COLLECTIONS:
        collection.drop_collection()