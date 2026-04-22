#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023-2026
# (c) The Regents of the University of California, Davis, 2023-2026
# File   : cmds/ipython.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 24.02.2026

from argparse import Namespace

import nest_asyncio
from IPython import start_ipython
from ponderosa import ArgParser

from . import commands
from ..database import connect_mongoengine, connect_to_database
from ..ldap import LDAPManager
from ..hippo import hippoapi_client
from ..iam import IAMAPI
from ..log import Console

@commands.register('ipython',
                   help='Start an IPython session')
def cmd_ipython(args: Namespace):
    # Ponderosa runs async postprocessors via asyncio.run(), leaving a running
    # event loop when this command body executes. IPython's prompt_toolkit also
    # calls asyncio.run() internally, which forbids nesting. nest_asyncio patches
    # asyncio to allow reentrant event loops so IPython can start cleanly.
    nest_asyncio.apply()

    config = args.config
    db = args.db
    lm = LDAPManager(config.ldap, pool_keepalive=15, pool_lifetime=30)
    hippo = hippoapi_client(config.hippo)
    iam = IAMAPI(config.ucdiam)

    console = Console()
    console.print(f'Connected to database: {db}')
    console.print(f'Connected to LDAP: {lm}')
    console.print(f'Connected to HiPPO: {hippo}')
    console.print(f'Connected to IAM: {iam}')

    if args.odm == 'beanie':
        from ..models import ALL_MODELS
        for model in ALL_MODELS:
            globals()[model.__name__] = model
    else:
        from ..database import COLLECTIONS
        for collection in COLLECTIONS:
            globals()[collection.__name__] = collection

    # --autoawait lets users write bare `await User.find_one(...)` in the shell
    # without wrapping in asyncio.run() — essential for poking at beanie docs.
    start_ipython(
        argv=['--autoawait', 'asyncio'],
        user_ns=locals() | globals(),
    )

@cmd_ipython.args()
def ipython_args(parser: ArgParser):
    parser.add_argument('--odm', choices=['mongoengine', 'beanie'], default='mongoengine',
                        help='ODM backend to use for database connection')

@ipython_args.postprocessor(priority=50)
async def _(args: Namespace):
    args.db = await connect_to_database(args.config.mongo, quiet=args.quiet, odm=args.odm)