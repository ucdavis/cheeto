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
import sys

from ponderosa import ArgParser, arggroup

from . import commands
from ..database import connect_to_database
from ..hippo import (hippoapi_client, process_hippoapi_events,
                     event_queue_pending_events,
                     filter_events,
                     HIPPO_EVENT_ACTIONS)
from ..log import Console


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
    process_hippoapi_events(args.config.hippo,
                            event_type=args.type,
                            event_id=args.event_id,
                            post_back=args.post)


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