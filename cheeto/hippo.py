#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023-2024
# (c) The Regents of the University of California, Davis, 2023
# File   : hippo.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 17.02.2023

"""Shared HiPPO REST API layer: the authenticated client factory and the
styled-notification (email) transport. The event-processing handlers that
used to live here have moved to the async beanie stack
(`cheeto/operations/hippo.py`); this module holds only the non-handler
plumbing that both that stack and the CLI/daemon build on."""

import logging
from contextlib import contextmanager
from typing import Awaitable, Callable, Iterable, Iterator, Optional

from .config import HippoConfig
from .hippoapi.api.notify import notify_styled
from .hippoapi.client import AuthenticatedClient
from .hippoapi.models import SimpleNotificationModel
from .log import Console
from .mail import Email


logger = logging.getLogger(__name__)


def hippoapi_client(config: HippoConfig, quiet: bool = False) -> AuthenticatedClient:
    """Build an authenticated client for the HiPPO REST API.

    No httpx event hooks: AuthenticatedClient shares one ``httpx_args`` across
    its sync *and* async clients, and the async client *awaits* its hooks — a
    plain (sync) hook would make every async request fail with
    ``TypeError: object NoneType can't be used in 'await' expression``.
    """
    if not quiet:
        console = Console(stderr=True)
        console.print('hippo config:')
        console.print(f'  base_url: [green]{config.base_url}')
        console.print(f'  max_tries: [green]{config.max_tries}')
    return AuthenticatedClient(
        follow_redirects=True,
        base_url=config.base_url,
        token=config.api_key,
        auth_header_name='X-API-Key',
        prefix='',
    )


async def send_email(mail: Email, client: AuthenticatedClient) -> bool:
    """POST a rendered Email through the HiPPO styled-notification endpoint.
    Logs on failure; never raises on a non-200 (callers treat notification as
    best-effort). Returns True iff the endpoint accepted it (HTTP 200)."""
    body = SimpleNotificationModel(
        subject=mail.subject,
        header=mail.header,
        emails=mail.emails,
        cc_emails=mail.ccEmails,
        paragraphs=list(mail.paragraphs()),
    )
    response = await notify_styled.asyncio_detailed(client=client, body=body)
    if response.status_code == 200:
        logger.info('sent email subject=%r to=%r', mail.subject, mail.emails)
        return True
    logger.error(
        'email send failed (status=%d): %s',
        response.status_code, response.content,
    )
    return False


@contextmanager
def email_notifier(
    config: HippoConfig,
    *,
    db: 'AsyncMongoClient',
    author: 'User | None' = None,
    enabled: bool = True,
    quiet: bool = True,
) -> Iterator[Optional[Callable[[Email], Awaitable[None]]]]:
    """Yield an async `notify(mail)` closure backed by a HiPPO client, or
    `None` when `enabled` is False. Centralizes the on/off gate and the client
    lifetime so callers can write::

        with email_notifier(config.hippo, db=args.db, author=args.author,
                            enabled=cfg.notify) as notify:
            await SomeOp.run(..., notifier=notify)

    Every send goes through the `SendUserEmail` operation so the fully-rendered
    email is recorded in History — hence `db`/`author` (the audit author)."""
    if not enabled:
        yield None
        return
    with hippoapi_client(config, quiet=quiet) as client:
        async def notify(mail: Email) -> None:
            # Lazy import: operations.email imports send_email from this module.
            from .operations.email import SendUserEmail
            await SendUserEmail.run(db, author, mail=mail, hippo_client=client)
        yield notify


def filter_events(
    events: Iterable,
    event_type: Optional[str] = None,
    event_id: Optional[int] = None,
) -> Iterable:
    """Yield events whose action and/or id match the filters (None = match all)."""
    for event in events:
        if event_type is not None and event.action != event_type:
            continue
        if event_id is not None and event.id != event_id:
            continue
        yield event
