#!/usr/bin/env python
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023-2025
# (c) The Regents of the University of California, Davis, 2023-2025
# File   : database/site.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 29.01.2025

import logging

from mongoengine import (ListField, 
                         GenericReferenceField, 
                         ReferenceField, 
                         StringField, 
                         signals,
                         PULL)

from .base import BaseDocument, handler
from .group import SiteGroup


@handler(signals.post_save)
def site_apply_globals(sender, document, **kwargs):
    from .user import SiteUser
    logger = logging.getLogger(__name__)
    logger.info(f'Site {document.sitename} modified, syncing globals')
    if document.global_groups or document.global_slurmers:
        site_users = SiteUser.objects(sitename=document.sitename)
        logger.info(f'Update globals with {len(site_users)} users')
        for site_group in document.global_groups:
            for site_user in site_users:
                site_group.update(add_to_set___members=site_user)
        for site_group in document.global_slurmers:
            for site_user in site_users:
                site_group.update(add_to_set___slurmers=site_user)


@site_apply_globals.apply #type: ignore
class Site(BaseDocument):
    sitename = StringField(required=True, primary_key=True)
    fqdn = StringField(required=True)
    global_groups = ListField(ReferenceField('SiteGroup', reverse_delete_rule=PULL))
    global_slurmers = ListField(ReferenceField('SiteGroup', reverse_delete_rule=PULL))
    default_home = GenericReferenceField()


site_t = Site | str

