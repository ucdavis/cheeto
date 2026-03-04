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
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable, Iterable, List, Optional

import httpx
from jinja2 import Environment, FileSystemLoader
from mongoengine import DoesNotExist
from mongoengine.context_managers import run_in_transaction
from rich.console import Console

from cheeto.database.crud import query_user_home_storage, remove_group_member
from cheeto.database.storage import NonExistentStorage

from .config import HippoConfig
from .database import (Site,
                       add_group_member,
                       add_user_access,
                       add_site_user,
                       DuplicateGlobalUser,
                       DuplicateSiteUser,
                       GlobalUser,
                       SiteUser,
                       SiteGroup,
                       HippoEvent,
                       create_group_from_sponsor,
                       create_user_from_hippo,
                       create_home_storage,
                       query_associated_storages,
                       query_user_groups,
                       query_user_slurm, 
                       set_user_status)
from .hippoapi.api.event_queue import (event_queue_pending_events,
                                       event_queue_update_status)
from .hippoapi.api.notify import (notify_raw,
                                  notify_styled)
from .hippoapi.client import AuthenticatedClient
from .hippoapi.models import (QueuedEventAccountModel,
                              QueuedEventModel,
                              QueuedEventDataModel,
                              QueuedEventUpdateModel,
                              SimpleNotificationModel)

from .mail import Email, NewAccountEmail, NewMembershipEmail, NewSponsorEmail, RemovedFromGroupEmail, UpdateSSHKeyEmail
from .templating import PKG_TEMPLATES
from .types import *
from .utils import (_ctx_name,
                    human_timestamp,
                    TIMESTAMP_NOW)

def log_request(request: httpx.Request):
    logger = logging.getLogger(__name__)
    logger.info(f'{request.method} {request.url}')


def log_response(response: httpx.Response):
    logger = logging.getLogger(__name__)
    logger.info(f'{response.status_code} {response.url}')


@dataclass
class EventContext:
    """Encapsulates shared handler dependencies. Extensible for future async migration."""

    client: AuthenticatedClient
    config: HippoConfig
    event_record: HippoEvent


@dataclass
class ParsedEventContext:
    """Parsed event data shared across handlers."""

    sitename: str
    username: str
    hippo_account: QueuedEventAccountModel


def parse_event_context(event: QueuedEventDataModel, config: HippoConfig) -> ParsedEventContext:
    hippo_account = event.accounts[0]
    sitename = config.site_aliases.get(event.cluster, event.cluster).lower()
    return ParsedEventContext(
        sitename=sitename,
        username=hippo_account.kerberos,
        hippo_account=hippo_account,
    )


def send_notification_if(
    condition: bool,
    email_fn: Callable[[], Email],
    client: AuthenticatedClient,
) -> None:
    if condition:
        hippoapi_send_email(email_fn(), client)


def get_site_user_or_raise(sitename: str, username: str) -> SiteUser:
    logger = logging.getLogger(__name__)
    try:
        return SiteUser.objects.get(sitename=sitename, username=username)
    except DoesNotExist:
        logger.error(f"SiteUser {username} does not exist on site {sitename}")
        raise


def get_site_group_or_raise(sitename: str, groupname: str) -> SiteGroup:
    logger = logging.getLogger(__name__)
    try:
        return SiteGroup.objects.get(sitename=sitename, groupname=groupname)
    except DoesNotExist:
        logger.error(f"SiteGroup {groupname} does not exist on site {sitename}")
        raise


class BaseEventHandler(ABC):
    """Base class for HiPPO event handlers. Designed for future async migration."""

    event_name: str

    @abstractmethod
    def handle(
        self,
        event: QueuedEventDataModel,
        context: EventContext,
        notify: bool = True,
    ) -> None:
        """Process the event. Future: async def handle(...) -> None"""
        ...


class AccountEventHandler(BaseEventHandler):
    """Base for handlers that operate on event.accounts[0]."""

    def handle(
        self,
        event: QueuedEventDataModel,
        context: EventContext,
        notify: bool = True,
    ) -> None:
        parsed = parse_event_context(event, context.config)
        self._log_start(parsed)
        result = self._execute(event, parsed, context)
        self._maybe_notify(event, parsed, context, notify, result)

    def _log_start(self, parsed: ParsedEventContext) -> None:
        logging.getLogger(__name__).info(
            f"Process {self.event_name} for site {parsed.sitename}"
        )

    @abstractmethod
    def _execute(
        self,
        event: QueuedEventDataModel,
        parsed: ParsedEventContext,
        context: EventContext,
    ):
        """Perform the handler's core logic. Return data for _maybe_notify (or None)."""
        ...

    def _maybe_notify(
        self,
        event: QueuedEventDataModel,
        parsed: ParsedEventContext,
        context: EventContext,
        notify: bool,
        result,
    ) -> None:
        """Override to send email. Receives result from _execute. Default no-op."""
        pass


class HandlerRegistry:
    """Registry of event handlers. Extensible for custom handlers."""

    def __init__(self) -> None:
        self._handlers: dict[str, BaseEventHandler] = {}

    def register(self, handler: BaseEventHandler) -> None:
        self._handlers[handler.event_name] = handler

    def get(self, action: str) -> BaseEventHandler | None:
        return self._handlers.get(action)

    @classmethod
    def default(cls) -> "HandlerRegistry":
        """Build registry with all built-in handlers."""
        registry = cls()
        for handler in [
            UpdateSshKeyHandler(),
            CreateAccountHandler(),
            AddAccountToGroupHandler(),
            RemoveAccountFromGroupHandler(),
            CreateGroupHandler(),
        ]:
            registry.register(handler)
        return registry


def hippoapi_client(config: HippoConfig, quiet: bool = False):
    if not quiet:
        console = Console(stderr=True)
        console.print('hippo config:')
        console.print(f'  uri: {config.base_url}')
    return AuthenticatedClient(follow_redirects=True,
                               base_url=config.base_url,
                               token=config.api_key,
                               httpx_args={"event_hooks": {"request": [log_request], "response": [log_response]}},
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


def filter_events(
    events: List[QueuedEventModel],
    event_type: Optional[str] = None,
    event_id: Optional[int] = None,
) -> Iterable[QueuedEventModel]:
    if event_type is None and event_id is None:
        yield from events
    else:
        for event in events:
            if event.id == event_id:
                yield event
            elif event.action == event_type:
                yield event


class EventProcessor:
    """Orchestrates HiPPO event processing. Designed for future async migration."""

    def __init__(self, config: HippoConfig, registry: HandlerRegistry | None = None) -> None:
        self.config = config
        self.registry = registry or HandlerRegistry.default()

    def process(
        self,
        post_back: bool = False,
        event_type: Optional[str] = None,
        event_id: Optional[int] = None,
    ) -> None:
        """Main entry point. Future: async def process(...)"""
        logger = logging.getLogger(__name__)
        with hippoapi_client(self.config) as client:
            events = event_queue_pending_events.sync(client=client)
            if events:
                self._process_events(
                    filter_events(events, event_type=event_type, event_id=event_id),
                    client,
                    post_back,
                )
            else:
                logger.warning("Got no events to process.")

    def _process_events(
        self,
        events: Iterable[QueuedEventModel],
        client: AuthenticatedClient,
        post_back: bool,
    ) -> None:
        logger = logging.getLogger(__name__)
        for event in events:
            logger.info(f"Process hippoapi {event.action} id={event.id}")
            if event.status != "Pending":
                logger.info(f"Skipping hippoapi id={event.id} because status is {event.status}")
                continue

            event_record = HippoEvent.objects(
                hippo_id=event.id,
                hippo_endpoint=self.config.base_url,
            ).modify(
                upsert=True,
                new=True,
                set__action=event.action,
                set__data=event.to_dict(),
                set__status="Pending",
            )

            handler = self.registry.get(event.action)
            if handler is None:
                logger.error(f"No handler registered for event action: {event.action}")
                event_record.modify(inc__n_tries=True, set__status="Failed")
                if post_back:
                    postback_event_failed(event.id, client)
                continue

            context = EventContext(client=client, config=self.config, event_record=event_record)
            try:
                handler.handle(event.data, context)
            except Exception as e:
                event_record.modify(inc__n_tries=True)
                logger.critical(
                    f"Error processing event id={event.id}, n_tries={event_record.n_tries}: {e}"
                )
                logger.critical(traceback.format_exc())
                if event_record.n_tries >= self.config.max_tries:
                    logger.warning(
                        f"Event id={event.id} failed {event_record.n_tries}, POST back that it Failed."
                    )
                    event_record.modify(set__status="Failed")
                    if post_back:
                        logger.info(f"Event id={event.id}: attempt postback")
                        postback_event_failed(event.id, client)
            else:
                event_record.modify(inc__n_tries=True, set__status="Complete")
                logger.info(f"Event id={event.id} completed.")
                if post_back:
                    logger.info(f"Event id={event.id}: attempt postback")
                    postback_event_complete(event.id, client)


def process_hippoapi_events(
    config: HippoConfig,
    post_back: bool = False,
    event_type: Optional[str] = None,
    event_id: Optional[int] = None,
) -> None:
    """Thin wrapper for backward compatibility."""
    EventProcessor(config).process(
        post_back=post_back,
        event_type=event_type,
        event_id=event_id,
    )


def postback_event_complete(event_id: int,
                            client: AuthenticatedClient):
    update = QueuedEventUpdateModel(status='Complete', id=event_id)
    return event_queue_update_status.sync_detailed(client=client, body=update)


def postback_event_failed(event_id: int,
                         client: AuthenticatedClient):
    update = QueuedEventUpdateModel(status='Failed', id=event_id)
    return event_queue_update_status.sync_detailed(client=client, body=update)


def _get_updatesshkey_email(user: SiteUser) -> UpdateSSHKeyEmail:
    sitefqdn = Site.objects.get(sitename=user.sitename).fqdn
    email = UpdateSSHKeyEmail(to=[user.parent.email],
                              username=user.username,
                              sitename=user.sitename.capitalize(),
                              sitefqdn=sitefqdn)
    return email


class UpdateSshKeyHandler(AccountEventHandler):
    event_name = "UpdateSshKey"

    def _execute(
        self,
        event: QueuedEventDataModel,
        parsed: ParsedEventContext,
        context: EventContext,
    ):
        with run_in_transaction():
            user = SiteUser.objects.get(
                username=parsed.username, sitename=parsed.sitename
            )
            global_user = user.parent
            global_user.ssh_key = [parsed.hippo_account.key]
            global_user.ldap_synced = False
            global_user.save()
            logger = logging.getLogger(__name__)
            logger.info(
                f"Add login-ssh access to user {parsed.username}, site {parsed.sitename}"
            )
            add_user_access(user, "login-ssh")
        return user

    def _maybe_notify(
        self,
        event: QueuedEventDataModel,
        parsed: ParsedEventContext,
        context: EventContext,
        notify: bool,
        result,
    ) -> None:
        send_notification_if(
            notify, lambda: _get_updatesshkey_email(result), context.client
        )


class CreateAccountHandler(BaseEventHandler):
    event_name = "CreateAccount"

    def handle(
        self,
        event: QueuedEventDataModel,
        context: EventContext,
        notify: bool = True,
    ) -> None:
        parsed = parse_event_context(event, context.config)
        logger = logging.getLogger(__name__)
        logger.info(f"Process CreateAccount for site {parsed.sitename}, event: {event}")

        guser = self._ensure_global_user(parsed)
        suser = self._ensure_site_user(parsed.sitename, guser, parsed)
        add_user_access(
            suser,
            list(
                hippo_to_cheeto_access(parsed.hippo_account.access_types or [])
                | {"slurm"}
            ),
        )
        self._ensure_home_storage(parsed.sitename, guser, parsed.username)
        self._add_to_groups(parsed.sitename, suser, event.groups, parsed.username)

        send_notification_if(
            notify,
            lambda: _get_createaccount_email(suser),
            context.client,
        )

    def _ensure_global_user(self, parsed: ParsedEventContext) -> GlobalUser:
        logger = logging.getLogger(__name__)
        try:
            guser, _ = create_user_from_hippo(parsed.hippo_account)
            logger.info(
                f"Created GlobalUser and GlobalGroup for {parsed.username}"
            )
        except DuplicateGlobalUser:
            logger.info(f"GlobalUser for {parsed.username} already exists.")
            guser = GlobalUser.objects.get(username=parsed.username)
            if (
                parsed.hippo_account.key
                and parsed.hippo_account.key not in guser.ssh_key
            ):
                logger.info(f"Updating SSH key for {parsed.username}")
                guser.update(
                    ssh_key=[parsed.hippo_account.key], ldap_synced=False
                )

        if guser.status == "inactive":
            logger.info(
                f"GlobalUser for {parsed.username} is inactive, setting to 'active'"
            )
            set_user_status(
                parsed.username, "active", "Activated from HiPPO"
            )
        return guser

    def _ensure_site_user(
        self, sitename: str, guser: GlobalUser, parsed: ParsedEventContext
    ) -> SiteUser:
        logger = logging.getLogger(__name__)
        try:
            suser, _ = add_site_user(sitename, guser)
        except DuplicateSiteUser:
            logger.warning(
                f"SiteUser for {parsed.username} already exists. This should not happen!"
            )
            suser = SiteUser.objects.get(
                username=parsed.username, sitename=sitename
            )

        if suser.status == "inactive":
            logger.info(
                f"SiteUser for {parsed.username} is inactive, setting to 'active'"
            )
            set_user_status(
                parsed.username,
                "active",
                "Activated from HiPPO",
                sitename=sitename,
            )
        return suser

    def _ensure_home_storage(
        self, sitename: str, guser: GlobalUser, username: str
    ) -> None:
        logger = logging.getLogger(__name__)
        try:
            query_user_home_storage(sitename, guser)
        except NonExistentStorage:
            logger.info(f"Creating home storage for {username}")
            create_home_storage(sitename, guser)
        else:
            logger.warning(
                f"Home storage for {username} already exists. This should not happen!"
            )

    def _add_to_groups(
        self,
        sitename: str,
        suser: SiteUser,
        groups: list,
        username: str,
    ) -> None:
        logger = logging.getLogger(__name__)
        for group in groups:
            try:
                add_group_member(sitename, suser, group.name)
            except Exception as e:
                logger.error(
                    f"{_ctx_name()}: error adding user {username} to group {group.name} on site {sitename}: {e}"
                )
                raise


def _get_user_email_info(user: SiteUser):
    groups = query_user_groups(user.sitename, user, types=["group", "class", "admin"])
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


def _get_createaccount_email(user: SiteUser):
    groups, sitefqdn, slurm_accounts, group_storages = _get_user_email_info(user)

    email = NewAccountEmail(to=[user.parent.email],
                            username=user.username,
                            groups=groups,
                            sitename=user.sitename.capitalize(),
                            sitefqdn=sitefqdn,
                            slurm_accounts=slurm_accounts,
                            group_storages=group_storages)
    return email


class AddAccountToGroupHandler(AccountEventHandler):
    event_name = "AddAccountToGroup"

    def _execute(
        self,
        event: QueuedEventDataModel,
        parsed: ParsedEventContext,
        context: EventContext,
    ):
        with run_in_transaction():
            site_user = SiteUser.objects.get(
                sitename=parsed.sitename, username=parsed.username
            )
            for group in event.groups:
                add_group_member(parsed.sitename, site_user, group.name)
        return site_user

    def _maybe_notify(
        self,
        event: QueuedEventDataModel,
        parsed: ParsedEventContext,
        context: EventContext,
        notify: bool,
        result,
    ) -> None:
        send_notification_if(
            notify,
            lambda: _get_newmembership_email(result),
            context.client,
        )


def _get_newsponsor_email(user: SiteUser, group: SiteGroup):
    sitefqdn = Site.objects.get(sitename=user.sitename).fqdn
    email = NewSponsorEmail(to=[user.parent.email],
                            username=user.username,
                            group=group.groupname,
                            sitename=user.sitename.capitalize(),
                            sitefqdn=sitefqdn)
    return email


def _get_newmembership_email(user: SiteUser):
    groups, sitefqdn, slurm_accounts, group_storages = _get_user_email_info(user)
    email = NewMembershipEmail(to=[user.parent.email],
                               username=user.username,
                               groups=groups,
                               sitename=user.sitename.capitalize(),
                               sitefqdn=sitefqdn,
                               slurm_accounts=slurm_accounts,
                               group_storages=group_storages)
    return email


class RemoveAccountFromGroupHandler(AccountEventHandler):
    event_name = "RemoveAccountFromGroup"

    def _execute(
        self,
        event: QueuedEventDataModel,
        parsed: ParsedEventContext,
        context: EventContext,
    ):
        user = get_site_user_or_raise(parsed.sitename, parsed.username)
        group = get_site_group_or_raise(parsed.sitename, event.groups[0].name)
        with run_in_transaction():
            remove_group_member(parsed.sitename, user, group)
        logger = logging.getLogger(__name__)
        logger.info(
            f"Removed user {user.username} from group {group.groupname} on site {parsed.sitename}"
        )
        return (user, group)

    def _maybe_notify(
        self,
        event: QueuedEventDataModel,
        parsed: ParsedEventContext,
        context: EventContext,
        notify: bool,
        result,
    ) -> None:
        user, group = result
        send_notification_if(
            notify,
            lambda: _get_removemembership_email(user, group),
            context.client,
        )


def _get_removemembership_email(user: SiteUser, group: SiteGroup):
    sponsors = []
    for sponsor in group._sponsors:
        sponsors.append((sponsor.fullname, sponsor.email))
    email = RemovedFromGroupEmail(to=[user.parent.email],
                                  group=group.groupname,
                                  sitename=user.sitename.capitalize(),
                                  sponsors=sponsors)
    return email


class CreateGroupHandler(AccountEventHandler):
    event_name = "CreateGroup"

    def _execute(
        self,
        event: QueuedEventDataModel,
        parsed: ParsedEventContext,
        context: EventContext,
    ):
        with run_in_transaction():
            sponsor = SiteUser.objects.get(
                sitename=parsed.sitename, username=parsed.username
            )
            group = create_group_from_sponsor(sponsor)
        logger = logging.getLogger(__name__)
        logger.info(
            f"Processed CreateGroup for group {group.groupname} for sponsor {parsed.username} on site {parsed.sitename}"
        )
        return (sponsor, group)

    def _maybe_notify(
        self,
        event: QueuedEventDataModel,
        parsed: ParsedEventContext,
        context: EventContext,
        notify: bool,
        result,
    ) -> None:
        sponsor, group = result
        send_notification_if(
            notify,
            lambda: _get_newsponsor_email(sponsor, group),
            context.client,
        )


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
        

