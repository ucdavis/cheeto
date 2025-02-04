#!/usr/bin/env python
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023-2025
# (c) The Regents of the University of California, Davis, 2023-2025
# File   : iam.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 03.02.2025

from http import HTTPStatus
import json
import logging

from .config import IAMConfig
from .database import GlobalUser, SiteUser, run_in_transaction
from .iamapi.client import AuthenticatedClient, Client
from .iamapi.api.people_ctlr import (
    search_pri_kerb_acct,
    search_contact_info,
    get_person_using_iam_id
)
from .iamapi.api.people_associations_controller import get_pps_assocs_using_iam_id
from .iamapi.api.organization_info_controller import search_ppsbo_us


class IAMAPI:

    def __init__(self, config: IAMConfig):
        self.client = Client(follow_redirects=True,
                                          base_url=config.base_url)
        self.key = config.api_key

    def query_sync(self, query_func, **kwargs):
        result = query_func(client=self.client, key=self.key, **kwargs)
        if result.status_code != HTTPStatus.OK:
            raise Exception(f'Query failed with status {result.status_code}')
        parsed = json.loads(result.content)
        return parsed['responseData']['results']

    def query_user_iamid(self, user_id: str):
        result = self.query_sync(search_pri_kerb_acct.sync_detailed, user_id=user_id)
        return result[0] if result else None

    def query_user_iamid_by_email(self, email: str):
        result = self.query_sync(search_contact_info.sync_detailed, email=email)
        return result[0] if result else None
    
    def query_user_info(self, iam_id: int):
        result = self.query_sync(get_person_using_iam_id.sync_detailed, iam_id=str(iam_id))
        return result[0] if result else None
    
    def query_user_associations(self, iam_id: int):
        return self.query_sync(get_pps_assocs_using_iam_id.sync_detailed, iam_id=str(iam_id))
    
    def query_org_division(self, org_oid: str):
        return self.query_sync(search_ppsbo_us.sync_detailed, org_o_id=org_oid)
    
    def query_user_colleges(self, iam_id: int):
        associations = self.query_user_associations(iam_id)
        oids = {assoc['bouOrgOId'] for assoc in associations}
        divisions_info = []
        for oid in oids:
            div_info = self.query_org_division(org_oid=oid)
            divisions_info.extend(div_info)
        return [div['deptOfficialName'] for div in divisions_info]


def sync_user_iam(user: GlobalUser, api: IAMAPI):
    logger = logging.getLogger(__name__)
    with run_in_transaction():
        if user.iam_id is None:
            user_data = api.query_user_iamid(user.username)
            if not user_data:
                logger.warning(f'No IAM ID found for {user.username}')
                user.iam_has_entry = False
                user.save()
                return
            user.iam_id = int(user_data['iamId'])
            logger.info(f'Query IAM ID for {user.username}: got {user.iam_id}')
        user.iam_has_entry = True

        user_data = api.query_user_info(user.iam_id)
        colleges = api.query_user_colleges(user.iam_id)
        if user.fullname != user_data['dFullName']:
            logger.info(f'Updating user {user.username} fullname from {user.fullname} to {user_data["dFullName"]}')
            user.fullname = user_data['dFullName']
        if user.colleges != colleges:
            logger.info(f'Updating user {user.username} colleges from {user.colleges} to {colleges}')
            user.colleges = colleges
        user.iam_synced = True
        user.save()