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
from typing import Callable, Optional, List

from jinja2 import Environment, FileSystemLoader
from mongoengine.context_managers import run_in_transaction
from rich.console import Console

from .config import HippoConfig
from .database import (Site,
                       add_group_member,
                       add_user_access,
                       GlobalUser,
                       SiteUser,
                       GlobalGroup,
                       SiteGroup,
                       HippoEvent,
                       create_group_from_sponsor,
                       create_home_storage, query_associated_storages,
                       query_user_exists, query_user_groups, query_user_slurm, 
                       set_user_status)
from .hippoapi.api.event_queue import (event_queue_pending_events,
                                       event_queue_update_status)
from .hippoapi.api.notify import (notify_raw,
                                  notify_styled)
from .hippoapi.client import AuthenticatedClient
from .hippoapi.models import (QueuedEventModel,
                              QueuedEventDataModel,
                              QueuedEventUpdateModel,
                              SimpleNotificationModel)

from .mail import Email, NewAccountEmail, NewMembershipEmail, NewSponsorEmail, UpdateSSHKeyEmail
from .templating import PKG_TEMPLATES
from .types import *
from .utils import (_ctx_name,
                    human_timestamp,
                    TIMESTAMP_NOW)


EVENT_HANDLERS = {}

def event_handler(event_name: str):
    def wrapper(func: Callable[[QueuedEventDataModel, AuthenticatedClient, HippoConfig], None]):
        EVENT_HANDLERS[event_name] = func
        return func
    return wrapper


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


def hippoapi_send_email(data: Email,
                        client: AuthenticatedClient):
    logger = logging.getLogger(__name__)
    body = SimpleNotificationModel(subject=data.subject,
                                   header=data.header,
                                   emails=data.emails,
                                   cc_emails=data.ccEmails,
                                   paragraphs=list(data.paragraphs()))
    result = notify_styled.sync_detailed(client=client, body=body)
    if result.status_code != 200:
        logger.error(f'Failed to send notification: {result.content}')
    else:
        logger.info(f'Successfully sent notification: {body}')
    return result


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
            handler = EVENT_HANDLERS.get(event.action)
            handler(event.data, client, config)
        except Exception as e:
            event_record.modify(inc__n_tries=True)
            logger.critical(f'Error processing event id={event.id}, n_tries={event_record.n_tries}: {e}')
            logger.critical(traceback.format_exc())
            if event_record.n_tries >= config.max_tries:
                logger.warning(f'Event id={event.id} failed {event_record.n_tries}, POST back that it Failed.')
                event_record.modify(set__status='Failed')
                if post_back:
                    logger.info(f'Event id={event.id}: attempt postback')
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


@event_handler('UpdateSshKey')
def handle_updatesshkey_event(event: QueuedEventDataModel,
                              client: AuthenticatedClient,
                              config: HippoConfig):
    logger = logging.getLogger(__name__)
    hippo_account = event.accounts[0]
    sitename = config.site_aliases.get(event.cluster, event.cluster).lower()
    username = hippo_account.kerberos
    ssh_key = hippo_account.key

    logger.info(f'Process UpdateSshKey for user {username}')
    try:
        with run_in_transaction():
            user = SiteUser.objects.get(username=username, sitename=sitename)
            global_user = user.parent
            global_user.ssh_key = [ssh_key]
            global_user.ldap_synced = False
            global_user.save()

            logger.info(f'Add login-ssh access to user {username}, site {sitename}')
            add_user_access(user, 'login-ssh')
    except Exception:
        raise
    else:
        hippoapi_send_email(get_updatesshkey_email(user), client)


def get_updatesshkey_email(user: SiteUser):
    sitefqdn = Site.objects.get(sitename=user.sitename).fqdn
    email = UpdateSSHKeyEmail(to=[user.parent.email],
                              username=user.username,
                              sitename=user.sitename.capitalize(),
                              sitefqdn=sitefqdn)
    return email


@event_handler('CreateAccount')
def handle_createaccount_event(event: QueuedEventDataModel,
                               client: AuthenticatedClient,
                               config: HippoConfig):

    logger = logging.getLogger(__name__)

    hippo_account = event.accounts[0]
    sitename = config.site_aliases.get(event.cluster, event.cluster).lower()
    username = hippo_account.kerberos
    is_sponsor_account = any((group.name == 'sponsors' for group in event.groups))

    logger.info(f'Process CreateAccount for site {sitename}, event: {event}')

    try:
        with run_in_transaction():
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

            if is_sponsor_account:
                sponsor_group = create_group_from_sponsor(site_user)
    except Exception:
        raise
    else:
        hippoapi_send_email(get_createaccount_email(site_user), client)
        if is_sponsor_account:
            hippoapi_send_email(get_newsponsor_email(site_user, sponsor_group), client)



def get_user_email_info(user: SiteUser):
    groups = query_user_groups(user.sitename, user, types=['group', 'class', 'admin'])
    sitefqdn = Site.objects.get(sitename=user.sitename).fqdn
    slurm_accounts = {}
    for assoc in query_user_slurm(user.sitename, user.username):
        if assoc.group.groupname in slurm_accounts:
            slurm_accounts[assoc.group.groupname].append(assoc.partition.partitionname)
        else:
            slurm_accounts[assoc.group.groupname] = [assoc.partition.partitionname]
    group_storages = []
    for storage in query_associated_storages(user.sitename, user.username):
        if str(storage.mount_path) != f'/home/{user.username}':
            group_storages.append(storage.mount_path)
    return groups, sitefqdn, slurm_accounts, group_storages


def get_createaccount_email(user: SiteUser):
    groups, sitefqdn, slurm_accounts, group_storages = get_user_email_info(user)

    email = NewAccountEmail(to=[user.parent.email],
                            username=user.username,
                            groups=groups,
                            sitename=user.sitename.capitalize(),
                            sitefqdn=sitefqdn,
                            slurm_accounts=slurm_accounts,
                            group_storages=group_storages)
    return email


@event_handler('AddAccountToGroup')
def handle_addaccounttogroup_event(event: QueuedEventDataModel,
                                   client: AuthenticatedClient,
                                   config: HippoConfig):
    logger = logging.getLogger(__name__)
    hippo_account = event.accounts[0]
    sitename = config.site_aliases.get(event.cluster, event.cluster).lower()
    is_sponsor_account = any((group.name == 'sponsors' for group in event.groups))
    logger.info(f'Process AddAccountToGroup for site {sitename}, event: {event}')

    try:
        with run_in_transaction():
            site_user = SiteUser.objects.get(sitename=sitename, username=hippo_account.kerberos)
            for group in event.groups:
                add_group_member(sitename, site_user, group.name)

            if is_sponsor_account:
                sponsor_group = create_group_from_sponsor(site_user)
    except Exception:
        raise
    else:
        if is_sponsor_account:
            hippoapi_send_email(get_newsponsor_email(site_user, sponsor_group), client)
        hippoapi_send_email(get_newmembership_email(site_user), client)


def get_newsponsor_email(user: SiteUser, group: SiteGroup):
    sitefqdn = Site.objects.get(sitename=user.sitename).fqdn
    email = NewSponsorEmail(to=[user.parent.email],
                            username=user.username,
                            group=group.groupname,
                            sitename=user.sitename.capitalize(),
                            sitefqdn=sitefqdn)
    return email


def get_newmembership_email(user: SiteUser):
    groups, sitefqdn, slurm_accounts, group_storages = get_user_email_info(user)
    email = NewMembershipEmail(to=[user.parent.email],
                               username=user.username,
                               groups=groups,
                               sitename=user.sitename.capitalize(),
                               sitefqdn=sitefqdn,
                               slurm_accounts=slurm_accounts,
                               group_storages=group_storages)
    return email


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
        

