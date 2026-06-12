"""Async port of the HiPPO event processor.

Mirrors the handler-dispatch design in cheeto/hippo.py but targets the
beanie stack. Each handler orchestrates existing ng Operations so every
hippo-driven DB mutation shows up in the History audit log.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, ClassVar

from beanie.operators import In
from pymongo import AsyncMongoClient

from ..config import HippoConfig
from ..constants import HIPPO_EVENT_ACTIONS
from ..hippoapi import AuthenticatedClient
from ..hippoapi.api.event_queue import (
    event_queue_pending_events,
    event_queue_update_status,
)
from ..hippoapi.api.notify import notify_styled
from ..hippoapi.models.queued_event_account_model import QueuedEventAccountModel
from ..hippoapi.models.queued_event_data_model import QueuedEventDataModel
from ..hippoapi.models.queued_event_model import QueuedEventModel
from ..hippoapi.models.queued_event_update_model import QueuedEventUpdateModel
from ..hippoapi.models.simple_notification_model import SimpleNotificationModel
from ..log import Console
from ..mail import (
    Email,
    NewAccountEmail,
    NewMembershipEmail,
    NewSponsorEmail,
    RemovedFromGroupEmail,
    UpdateSSHKeyEmail,
)
from ..models.group import Group
from ..models.group_membership import GroupMembership
from ..models.hippo import HippoEvent
from ..models.site import Site
from ..models.user import User
from .group_membership import AddGroupMember, RemoveGroupMember
from .group import CreateGroupFromSponsor
from .storage import CreateHomeStorage
from .user import AddUserAccess, CreateUser
from .user_site import AddSiteUser


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Client factory + helpers
# ---------------------------------------------------------------------------


def hippoapi_client(config: HippoConfig, quiet: bool = False) -> AuthenticatedClient:
    """Build an authenticated HTTP client for the HiPPO REST API."""
    if not quiet:
        console = Console(stderr=True)
        console.print(f'hippo config:')
        console.print(f'  base_url: [green]{config.base_url}')
        console.print(f'  max_tries: [green]{config.max_tries}')
    return AuthenticatedClient(
        follow_redirects=True,
        base_url=config.base_url,
        token=config.api_key,
        auth_header_name='X-API-Key',
        prefix='',
    )


_HIPPO_ACCESS_MAP = {
    'OpenOnDemand': 'ondemand',
    'SshKey': 'login-ssh',
}


def _hippo_to_cheeto_access(access_types: list[str] | None) -> set[str]:
    if not access_types:
        return set()
    return {
        _HIPPO_ACCESS_MAP[t] for t in access_types if t in _HIPPO_ACCESS_MAP
    }


def filter_events(events, event_type: str | None = None, event_id: int | None = None):
    """Yield events whose action and/or id match the filters (None = match all)."""
    for event in events:
        if event_type is not None and event.action != event_type:
            continue
        if event_id is not None and event.id != event_id:
            continue
        yield event


# ---------------------------------------------------------------------------
# Handler context + base classes
# ---------------------------------------------------------------------------


@dataclass
class ParsedEventContext:
    sitename: str
    username: str
    hippo_account: QueuedEventAccountModel


def _parse_event(event: QueuedEventDataModel,
                 config: HippoConfig) -> ParsedEventContext:
    account = event.accounts[0]
    raw_cluster = event.cluster or ''
    sitename = config.site_aliases.get(raw_cluster, raw_cluster).lower()
    return ParsedEventContext(
        sitename=sitename,
        username=account.kerberos,
        hippo_account=account,
    )


@dataclass
class HippoContext:
    client: AsyncMongoClient
    hippo_client: AuthenticatedClient
    config: HippoConfig
    event_record: HippoEvent
    author: User | None = None


class BaseHippoHandler(ABC):
    """Abstract base for async hippo event handlers."""

    action: ClassVar[str]

    @abstractmethod
    async def handle(self,
                     event: QueuedEventDataModel,
                     context: HippoContext,
                     notify: bool = True) -> Any:
        ...


class AccountHippoHandler(BaseHippoHandler):
    """Template for handlers that operate on `event.accounts[0]`."""

    async def handle(self,
                     event: QueuedEventDataModel,
                     context: HippoContext,
                     notify: bool = True) -> Any:
        parsed = _parse_event(event, context.config)
        logger.info(
            '[%s] handling for user=%s site=%s',
            self.action, parsed.username, parsed.sitename,
        )
        result = await self._execute(event, parsed, context)
        await self._maybe_notify(event, parsed, context, notify, result)
        return result

    @abstractmethod
    async def _execute(self,
                       event: QueuedEventDataModel,
                       parsed: ParsedEventContext,
                       context: HippoContext) -> Any:
        ...

    async def _maybe_notify(self,
                            event: QueuedEventDataModel,
                            parsed: ParsedEventContext,
                            context: HippoContext,
                            notify: bool,
                            result: Any) -> None:
        # Default: no-op. Override to send an email.
        return None


# ---------------------------------------------------------------------------
# Email sending via hippoapi notify endpoint
# ---------------------------------------------------------------------------


async def _send_email(mail: Email, hippo_client: AuthenticatedClient) -> None:
    body = SimpleNotificationModel(
        subject=mail.subject,
        header=mail.header,
        emails=mail.emails,
        cc_emails=mail.ccEmails,
        paragraphs=list(mail.paragraphs()),
    )
    response = await notify_styled.asyncio_detailed(
        client=hippo_client, body=body,
    )
    if response.status_code == 200:
        logger.info('sent email subject=%r to=%r', mail.subject, mail.emails)
    else:
        logger.error(
            'email send failed (status=%d): %s',
            response.status_code, response.content,
        )


# ---------------------------------------------------------------------------
# Concrete handlers
# ---------------------------------------------------------------------------


class UpdateSshKeyHandler(AccountHippoHandler):
    action = 'UpdateSshKey'

    async def _execute(self, event, parsed, context):
        from ..models.user import SshKey as SshKeyModel
        user = await User.find_one(User.name == parsed.username)
        if user is None:
            raise ValueError(f'User {parsed.username} does not exist')
        key = parsed.hippo_account.key
        if key:
            # The bulk delete bypasses SshKey's LDAP-dirty propagation hook,
            # but the paired insert below fires it — the user is re-dirtied.
            await SshKeyModel.find(SshKeyModel.user.id == user.id).delete()
            await SshKeyModel(key=key, user=user).insert()
        await AddUserAccess.run(
            context.client, context.author,
            name=parsed.username, access='login-ssh',
        )
        context.event_record.target_user = user
        return user

    async def _maybe_notify(self, event, parsed, context, notify, result):
        if not notify:
            return
        site = await Site.find_one(Site.name == parsed.sitename)
        sitefqdn = site.fqdn if site else parsed.sitename
        mail = UpdateSSHKeyEmail(
            to=[parsed.hippo_account.email],
            username=parsed.username,
            sitename=parsed.sitename,
            sitefqdn=sitefqdn,
        )
        await _send_email(mail, context.hippo_client)


class CreateAccountHandler(BaseHippoHandler):
    """Create user (if new), add to site, apply access, create home storage, add to groups."""

    action = 'CreateAccount'

    async def handle(self, event, context, notify=True):
        parsed = _parse_event(event, context.config)
        logger.info(
            '[CreateAccount] user=%s site=%s', parsed.username, parsed.sitename,
        )

        site = await Site.find_one(Site.name == parsed.sitename)
        if site is None:
            raise ValueError(f'Site {parsed.sitename} does not exist')

        user = await self._ensure_user(parsed, context)

        existing_on_site = await self._on_site(user.id, site.id)
        if not existing_on_site:
            await AddSiteUser.run(
                context.client, context.author,
                user_name=parsed.username, site_name=parsed.sitename,
            )

        access = _hippo_to_cheeto_access(parsed.hippo_account.access_types or [])
        access.add('slurm')
        if access:
            await AddUserAccess.run(
                context.client, context.author,
                name=parsed.username, access=sorted(access),
            )

        await self._ensure_home_storage(parsed, user, site, context)

        added_groups: list[Group] = []
        for group_ref in (event.groups or []):
            grp = await self._add_to_group(
                parsed.username, group_ref.name, parsed.sitename, context,
            )
            if grp is not None:
                added_groups.append(grp)

        context.event_record.target_user = user
        context.event_record.target_groups = added_groups
        context.event_record.target_groupnames = [g.name for g in (event.groups or [])]

        if notify:
            mail = await self._build_email(parsed, user, site)
            await _send_email(mail, context.hippo_client)
        return user

    async def _ensure_user(self, parsed: ParsedEventContext,
                           context: HippoContext) -> User:
        existing = await User.find_one(User.name == parsed.username)
        if existing is not None:
            return existing
        acct = parsed.hippo_account
        # HiPPO identities carry mothra/iam ids we could fold into User.iam, but
        # UCDIAMInfo requires both — skip until we cross-walk properly.
        user, _ = await CreateUser.run(
            context.client, context.author,
            name=parsed.username,
            email=acct.email,
            uid=int(acct.mothra),
            fullname=acct.name,
            gid=int(acct.mothra),
            home_directory=f'/home/{parsed.username}',
        )
        return user

    async def _on_site(self, user_id, site_id) -> bool:
        from ..models.user_site_info import UserSiteInfo
        found = await UserSiteInfo.find_one(
            UserSiteInfo.user.id == user_id,
            UserSiteInfo.site.id == site_id,
        )
        return found is not None

    async def _ensure_home_storage(self, parsed, user, site, context):
        # home storage creation requires host info that doesn't come from the
        # event payload; the old implementation derived it from site defaults.
        # For now, skip automatic creation — operators can run
        # `ng storage new home` or we add site-default host later.
        return None

    async def _add_to_group(self, username: str, groupname: str,
                            sitename: str,
                            context: HippoContext) -> Group | None:
        grp = await Group.find_one(Group.name == groupname)
        if grp is None:
            logger.warning('group %s does not exist, skipping', groupname)
            return None
        try:
            await AddGroupMember.run(
                context.client, context.author,
                group_name=groupname, user_name=username,
                site_name=sitename,
            )
        except Exception as e:
            logger.warning('could not add %s to %s: %s', username, groupname, e)
            return None
        return grp

    async def _build_email(self, parsed, user, site) -> Email:
        # Gather the user's groups + slurm associations at this site for the
        # email body — reuse the existing queries package.
        from ..queries.slurm import user_slurm_at_site
        slurm = await user_slurm_at_site(user, site)
        slurm_accounts = {
            entry['group'].name: {
                'partitions': sorted({a.partition.name for a in entry['slurm'].associations}),
                'qoses': sorted({a.qos.name for a in entry['slurm'].associations}),
            }
            for entry in [
                {'group': e.group, 'slurm': e.slurm} for e in slurm
            ]
        }
        group_storages: list = []  # no storage query ported yet
        return NewAccountEmail(
            to=[parsed.hippo_account.email],
            username=parsed.username,
            groups=sorted({e.group.name for e in slurm}),
            sitename=parsed.sitename,
            sitefqdn=site.fqdn,
            slurm_accounts=slurm_accounts,
            group_storages=group_storages,
        )


class AddAccountToGroupHandler(AccountHippoHandler):
    action = 'AddAccountToGroup'

    async def _execute(self, event, parsed, context):
        user = await User.find_one(User.name == parsed.username)
        if user is None:
            raise ValueError(f'User {parsed.username} does not exist')
        added: list[Group] = []
        for group_ref in (event.groups or []):
            grp = await Group.find_one(Group.name == group_ref.name)
            if grp is None:
                logger.warning('group %s not in new db, skipping', group_ref.name)
                continue
            await AddGroupMember.run(
                context.client, context.author,
                group_name=group_ref.name, user_name=parsed.username,
                site_name=parsed.sitename,
            )
            added.append(grp)
        context.event_record.target_user = user
        context.event_record.target_groups = added
        context.event_record.target_groupnames = [g.name for g in (event.groups or [])]
        return user, added

    async def _maybe_notify(self, event, parsed, context, notify, result):
        if not notify:
            return
        user, _added = result
        site = await Site.find_one(Site.name == parsed.sitename)
        if site is None:
            return
        from ..queries.slurm import user_slurm_at_site
        slurm_entries = await user_slurm_at_site(user, site)
        slurm_accounts = {
            e.group.name: {
                'partitions': sorted({a.partition.name for a in e.slurm.associations}),
                'qoses': sorted({a.qos.name for a in e.slurm.associations}),
            }
            for e in slurm_entries
        }
        mail = NewMembershipEmail(
            to=[parsed.hippo_account.email],
            username=parsed.username,
            groups=sorted({e.group.name for e in slurm_entries}),
            sitename=parsed.sitename,
            sitefqdn=site.fqdn,
            slurm_accounts=slurm_accounts,
            group_storages=[],
        )
        await _send_email(mail, context.hippo_client)


class RemoveAccountFromGroupHandler(AccountHippoHandler):
    action = 'RemoveAccountFromGroup'

    async def _execute(self, event, parsed, context):
        user = await User.find_one(User.name == parsed.username)
        if user is None:
            raise ValueError(f'User {parsed.username} does not exist')
        group_ref = event.groups[0]
        grp = await Group.find_one(Group.name == group_ref.name, fetch_links=True)
        if grp is None:
            raise ValueError(f'Group {group_ref.name} does not exist')
        await RemoveGroupMember.run(
            context.client, context.author,
            group_name=group_ref.name, user_name=parsed.username,
            site_name=parsed.sitename,
        )
        logger.info('Removed %s from %s', parsed.username, group_ref.name)
        context.event_record.target_user = user
        context.event_record.target_groups = [grp]
        context.event_record.target_groupnames = [group_ref.name]
        return user, grp

    async def _maybe_notify(self, event, parsed, context, notify, result):
        if not notify:
            return
        _user, grp = result
        # Sponsors are per-site now: pull the sponsor-role membership edges
        # for this group at this site and resolve the sponsor users.
        sponsors: list[tuple[str, str]] = []
        site = await Site.find_one(Site.name == parsed.sitename)
        if site is not None:
            edges = await GroupMembership.find(
                GroupMembership.group.id == grp.id,
                GroupMembership.site.id == site.id,
                fetch_links=True,
            ).to_list()
            sponsors = [
                (s.user.fullname or s.user.name, s.user.email)
                for s in edges if 'sponsor' in s.roles
            ]
        mail = RemovedFromGroupEmail(
            to=[parsed.hippo_account.email],
            group=grp.name,
            sitename=parsed.sitename,
            sponsors=sponsors,
        )
        await _send_email(mail, context.hippo_client)


class CreateGroupHandler(BaseHippoHandler):
    action = 'CreateGroup'

    async def handle(self, event, context, notify=True):
        parsed = _parse_event(event, context.config)
        logger.info(
            '[CreateGroup] sponsor=%s site=%s',
            parsed.username, parsed.sitename,
        )
        sponsor = await User.find_one(User.name == parsed.username)
        if sponsor is None:
            raise ValueError(f'Sponsor {parsed.username} does not exist')
        group = await CreateGroupFromSponsor.run(
            context.client, context.author,
            sponsor_name=parsed.username,
            site_name=parsed.sitename,
        )
        context.event_record.target_user = sponsor
        context.event_record.target_groups = [group]
        context.event_record.target_groupnames = [group.name]
        context.event_record.sponsor_username = parsed.username

        if notify:
            site = await Site.find_one(Site.name == parsed.sitename)
            sitefqdn = site.fqdn if site else parsed.sitename
            mail = NewSponsorEmail(
                to=[parsed.hippo_account.email],
                username=parsed.username,
                group=group.name,
                sitename=parsed.sitename,
                sitefqdn=sitefqdn,
            )
            await _send_email(mail, context.hippo_client)
        return sponsor, group


# ---------------------------------------------------------------------------
# Handler registry
# ---------------------------------------------------------------------------


class HippoHandlerRegistry:
    def __init__(self) -> None:
        self._handlers: dict[str, BaseHippoHandler] = {}

    def register(self, handler: BaseHippoHandler) -> None:
        self._handlers[handler.action] = handler

    def get(self, action: str) -> BaseHippoHandler | None:
        return self._handlers.get(action)

    @classmethod
    def default(cls) -> HippoHandlerRegistry:
        reg = cls()
        reg.register(UpdateSshKeyHandler())
        reg.register(CreateAccountHandler())
        reg.register(AddAccountToGroupHandler())
        reg.register(RemoveAccountFromGroupHandler())
        reg.register(CreateGroupHandler())
        return reg


# ---------------------------------------------------------------------------
# Processor
# ---------------------------------------------------------------------------


async def _upsert_hippo_event(
    upstream: QueuedEventModel,
    config: HippoConfig,
) -> HippoEvent:
    """Find or create the HippoEvent record for an upstream queued event."""
    existing = await HippoEvent.find_one(
        HippoEvent.hippo_id == upstream.id,
        HippoEvent.hippo_endpoint == config.base_url,
    )
    if existing is not None:
        return existing

    data: QueuedEventDataModel = upstream.data
    raw_cluster = data.cluster or ''
    sitename = config.site_aliases.get(raw_cluster, raw_cluster).lower()
    site = await Site.find_one(Site.name == sitename) if sitename else None

    # Pull the primary account (if any) for easy querying
    target_username = None
    if data.accounts:
        target_username = data.accounts[0].kerberos
    target_user = None
    if target_username:
        target_user = await User.find_one(User.name == target_username)

    group_names = [g.name for g in (data.groups or [])]
    target_groups = []
    if group_names:
        target_groups = await Group.find(In(Group.name, group_names)).to_list()

    event = HippoEvent(
        hippo_id=upstream.id,
        hippo_endpoint=config.base_url,
        action=upstream.action,
        status=upstream.status,
        cluster=raw_cluster,
        site=site,
        target_username=target_username,
        target_user=target_user,
        target_groupnames=group_names,
        target_groups=target_groups,
        raw=upstream.to_dict(),
    )
    await event.insert()
    return event


class HippoEventProcessor:

    def __init__(self,
                 client: AsyncMongoClient,
                 config: HippoConfig,
                 registry: HippoHandlerRegistry | None = None,
                 author: User | None = None) -> None:
        self.client = client
        self.config = config
        self.registry = registry or HippoHandlerRegistry.default()
        self.author = author

    async def process(self,
                      post_back: bool = False,
                      event_type: str | None = None,
                      event_id: int | None = None) -> None:
        with hippoapi_client(self.config) as hippo_client:
            events = await event_queue_pending_events.asyncio(client=hippo_client) or []
            await self._process_events(
                list(filter_events(events, event_type, event_id)),
                hippo_client, post_back,
            )

    async def _process_events(self, events, hippo_client, post_back):
        for upstream in events:
            if upstream.status != 'Pending':
                continue
            record = await _upsert_hippo_event(upstream, self.config)

            if record.status in ('Complete', 'Failed', 'Canceled'):
                # Already processed locally (e.g. a previous run without
                # post_back); never re-handle or re-notify. If HiPPO still
                # serves the event as Pending we owe it a postback so it
                # stops.
                if post_back and record.posted_back_at is None:
                    if await _postback(hippo_client, upstream.id,
                                       record.status):
                        record.posted_back_at = datetime.now(timezone.utc)
                        await record.save()
                continue

            handler = self.registry.get(upstream.action)
            if handler is None:
                msg = f'no handler for action {upstream.action}'
                logger.error(msg)
                record.status = 'Failed'
                record.last_error = msg
                if post_back and await _postback(hippo_client, upstream.id,
                                                 'Failed'):
                    record.posted_back_at = datetime.now(timezone.utc)
                await record.save()
                continue

            context = HippoContext(
                client=self.client,
                hippo_client=hippo_client,
                config=self.config,
                event_record=record,
                author=self.author,
            )

            record.n_tries = (record.n_tries or 0) + 1
            try:
                await handler.handle(upstream.data, context, notify=True)
            except Exception as exc:
                import traceback
                tb = traceback.format_exc()
                logger.critical(
                    'handler for %s failed (try %d/%d): %s',
                    upstream.action, record.n_tries, self.config.max_tries, exc,
                )
                record.last_error = tb
                if record.n_tries >= self.config.max_tries:
                    record.status = 'Failed'
                    record.completed_at = datetime.now(timezone.utc)
                    if post_back and await _postback(hippo_client,
                                                     upstream.id, 'Failed'):
                        record.posted_back_at = datetime.now(timezone.utc)
                await record.save()
                continue

            record.status = 'Complete'
            record.completed_at = datetime.now(timezone.utc)
            record.last_error = None
            if post_back and await _postback(hippo_client, upstream.id,
                                             'Complete'):
                record.posted_back_at = datetime.now(timezone.utc)
            await record.save()


async def _postback(client: AuthenticatedClient,
                    event_id: int,
                    status: str) -> bool:
    """Report a terminal status to the HiPPO API. Returns success: callers
    record `posted_back_at` only on a 200 so a failed postback is retried
    on a later run (without re-processing the event)."""
    body = QueuedEventUpdateModel(status=status, id=event_id)
    resp = await event_queue_update_status.asyncio_detailed(
        client=client, body=body,
    )
    if resp.status_code != 200:
        logger.error(
            'postback %s -> %s failed: status=%d',
            event_id, status, resp.status_code,
        )
        return False
    return True
