#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023-2024
# (c) The Regents of the University of California, Davis, 2023-2024
# File   : cmds/config.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 04.11.2024

from argparse import Namespace
import logging
from pathlib import Path

from ponderosa import ArgParser, arggroup

from . import commands
from ..database import connect_to_database
from ..hippoapi.api.action import action_sync_puppet_accounts
from ..hippoapi.api.event_queue import event_queue_pending_events
from ..hippo import (EventProcessor,
                     hippoapi_client,
                     filter_events,
                     HIPPO_EVENT_ACTIONS)
from ..log import Console


@commands.register('hippo',
                   help='Interaction with the HiPPO API')
def _(*args):
    pass


@arggroup('HiPPO API', desc='HiPPO API event arguments')
def event_args(parser: ArgParser):
    parser.add_argument('--post', default=False, action='store_true')
    parser.add_argument('--id', default=None, dest='event_id', type=int)
    parser.add_argument('--type', choices=list(HIPPO_EVENT_ACTIONS))


@event_args.apply()
@commands.register('hippo', 'process',
                   help='Process Events from HiPPO API')
def cmd_hippoapi_process(args: Namespace):
    connect_to_database(args.config.mongo)
    console = Console()
    EventProcessor(args.config.hippo).process(
        post_back=args.post,
        event_type=args.type,
        event_id=args.event_id,
    )


@event_args.apply()
@commands.register('hippo', 'events',
                   help='List HiPPO event queue')
def cmd_hippoapi_events(args: Namespace):
    logger = logging.getLogger(__name__)
    console = Console()
    connect_to_database(args.config.mongo)
    with hippoapi_client(args.config.hippo) as client:
        events = event_queue_pending_events.sync(client=client)
   
    if not events:
        return

    for event in filter_events(events, event_type=args.type, event_id=args.event_id):
        console.print(event)

@commands.register('hippo', 'sync-puppet',
                   help='Force a HiPPO sync from puppet YAML files')
def cmd_hippoapi_sync_puppet(args: Namespace):
    console = Console()
    with hippoapi_client(args.config.hippo) as client:
        response = action_sync_puppet_accounts.sync_detailed(client=client)
        console.info(response)