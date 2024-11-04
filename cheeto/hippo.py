#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023-2024
# (c) The Regents of the University of California, Davis, 2023
# File   : hippo.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 17.02.2023

import argparse
import logging
import shlex
import socket
import sys
import traceback
from typing import Optional, List

from rich.console import Console

from .config import HippoConfig
from .database import (add_group_member,
                       add_user_access,
                       GlobalUser,
                       SiteUser,
                       GlobalGroup,
                       SiteGroup,
                       HippoEvent,
                       create_group_from_sponsor,
                       create_home_storage,
                       query_user_exists, 
                       set_user_status)
from .hippoapi.api.event_queue import (event_queue_pending_events,
                                       event_queue_update_status)
from .hippoapi.client import AuthenticatedClient
from .hippoapi.models import (QueuedEventModel,
                              QueuedEventDataModel,
                              QueuedEventUpdateModel)
from .templating import PKG_TEMPLATES
from .types import *
from .utils import (_ctx_name,
                    human_timestamp,
                    TIMESTAMP_NOW)


def hippoapi_client(config: HippoConfig, quiet: bool = False):
    if not quiet:
        console = Console(stderr=True)
        console.print('hippo config:')
        console.print(f'  uri: {config.base_url}')
    return AuthenticatedClient(follow_redirects=True,
                               base_url=config.base_url,
                               token=config.api_key,
                               #httpx_args={"event_hooks": {"request": [log_request]}},
                               auth_header_name='X-API-Key',
                               prefix='')


class HippoEventHandler:

    event_name : str = 'Null'

    
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
                                                                    set_on_insert__data=event.to_dict(),
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
    sitename = config.site_aliases.get(event.cluster, event.cluster).lower()
    username = hippo_account.kerberos
    ssh_key = hippo_account.key

    logger.info(f'Process UpdateSshKey for user {username}')
    user = SiteUser.objects.get(username=username, sitename=sitename)
    global_user = user.parent
    global_user.ssh_key = [ssh_key]
    global_user.save()

    logger.info(f'Add login-ssh access to user {username}, site {sitename}')
    add_user_access(user, 'login-ssh')


def process_createaccount_event(event: QueuedEventDataModel,
                                config: HippoConfig):

    logger = logging.getLogger(__name__)

    hippo_account = event.accounts[0]
    sitename = config.site_aliases.get(event.cluster, event.cluster).lower()
    username = hippo_account.kerberos

    logger.info(f'Process CreateAccount for site {sitename}, event: {event}')

    if not query_user_exists(username):
        logger.info(f'GlobalUser for {username} does not exist, creating.')
        global_user = GlobalUser.from_hippo(hippo_account)
        global_user.save()
        global_group = GlobalGroup(groupname=username,
                                   gid=global_user.gid,
                                   type='user')
        global_group.save()
    else:
        logger.info(f'GlobalUser for {username} exists, checking status.')
        global_user = GlobalUser.objects.get(username=username) #type: ignore
        global_group = GlobalGroup.objects.get(groupname=username)  # type: ignore
        if global_user.status != 'active':
            logger.info(f'GlobalUser for {username} has status {global_user.status}, setting to "active"')
            set_user_status(username, 'active', 'Activated from HiPPO')

    if query_user_exists(username, sitename=sitename):
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
                             _access=hippo_to_cheeto_access(hippo_account.access_types) | {'slurm'}) #type: ignore
        site_user.save()
        site_group = SiteGroup(groupname=username,
                               sitename=sitename,
                               parent=global_group,
                               _members=[site_user])
        site_group.save()

    try:
        create_home_storage(sitename, global_user)
    except Exception:
        logger.error(f'{_ctx_name()}: error creating home storage for {username} on site {sitename}')

    for group in event.groups:
        add_group_member(sitename, username, group.name)

    if any((group.name == 'sponsors' for group in event.groups)):
        create_group_from_sponsor(site_user)


def process_addaccounttogroup_event(event: QueuedEventDataModel,
                                    config: HippoConfig):
    logger = logging.getLogger(__name__)
    hippo_account = event.accounts[0]
    sitename = config.site_aliases.get(event.cluster, event.cluster).lower()
    logger.info(f'Process AddAccountToGroup for site {sitename}, event: {event}')

    for group in event.groups:
        add_group_member(sitename, hippo_account.kerberos, group.name)

    if any((group.name == 'sponsors' for group in event.groups)):
        site_user = SiteUser.objects.get(sitename=sitename, username=hippo_account.kerberos)
        create_group_from_sponsor(site_user)






def postprocess(self):
    if self.state != ConversionState.PROC or self.user_record is None:
        return

    mail = Mailx()

    if self.op == ConversionOp.USER:
        group = list(self.hippo_record.groups)[0]
        slurm_account, slurm_partitions = self.site.get_group_slurm_partitions(group)
        storages = self.site.get_group_storage_paths(group)
        template = self.jinja_env.get_template('account-ready.txt.j2')
        email_contents = template.render(
                            cluster=self.hippo_record.meta.cluster,
                            username=self.user_name,
                            domain=self.site.root.name,
                            slurm_account=slurm_account,
                            slurm_partitions=slurm_partitions,
                            storages=storages
                         )
        email_subject = f'Account Information: {self.hippo_record.meta.cluster}'
        email_cmd = mail.send(self.user_record.email,
                              email_contents,
                              reply_to='hpc-help@ucdavis.edu',
                              subject=email_subject)
    elif self.op == ConversionOp.KEY:
        template = self.jinja_env.get_template('key-updated.txt.j2')
        email_contents = template.render(
                            cluster=self.hippo_record.meta.cluster,
                            username=self.user_name,
                            domain=self.site.root.name
                         )
        email_subject = f'Key Updated: {self.hippo_record.meta.cluster}'
        email_cmd = mail.send(self.user_record.email,
                              email_contents,
                              reply_to='hpc-help@ucdavis.edu',
                              subject=email_subject)
    else:
        # okay this is ugly but if its a sponsor group, its new, so the only
        # two elements in the groups will be "sponsors" and the new group
        group = list(self.user_record.groups.copy() - {self.sponsors_group})[0]
        template = self.jinja_env.get_template('new-sponsor.txt.j2')
        email_contents = template.render(
                            cluster=self.hippo_record.meta.cluster,
                            username=self.user_name,
                            domain=self.site.root.name,
                            group=group
                         )
        email_subject = f'New Sponsor Account: {self.hippo_record.meta.cluster}'
        email_cmd = mail.send(self.user_record.email,
                              email_contents,
                              reply_to='hpc-help@ucdavis.edu',
                              subject=email_subject)

    self.logger.info(f'Sending email: \nSubject: {email_subject}\nBody: {email_contents}')
    email_cmd()
    self.set_complete()



def sync(args: argparse.Namespace):
    templates_dir = PKG_TEMPLATES / 'emails'
    jinja_env = Environment(loader=FileSystemLoader(searchpath=templates_dir))

    try:
        _sync(args, jinja_env)
    except:
        logger = logging.getLogger(__name__)
        logger.critical(f'Exception in sync, sending ticket to hpc-help.')
        logger.critical(traceback.format_exc())
        template = jinja_env.get_template('sync-error.txt.j2')
        subject = f'cheeto sync exception on {socket.getfqdn()}'
        contents = template.render(hostname=socket.getfqdn(),
                                   stacktrace=traceback.format_exc(),
                                   logfile=args.log,
                                   timestamp=human_timestamp(TIMESTAMP_NOW),
                                   pyexe=sys.executable,
                                   exeargs=shlex.join(sys.argv))
        Mailx().send('cswel@ucdavis.edu', contents, subject=subject)()
        

