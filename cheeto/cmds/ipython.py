#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) The Regents of the University of California, Davis
# File   : cmds/ipython.py
# License: Modified BSD

from argparse import Namespace

import nest_asyncio
from IPython import start_ipython
from ponderosa import ArgParser

from . import commands
from ..db import connect_beanie
from ..hippo import hippoapi_client
from ..log import Console


@commands.register('ipython',
                   help='Start an IPython session with the beanie models loaded')
def cmd_ipython(args: Namespace):
    # Ponderosa runs async postprocessors via asyncio.run(), leaving a running
    # event loop when this command body executes. IPython's prompt_toolkit also
    # calls asyncio.run() internally, which forbids nesting. nest_asyncio patches
    # asyncio to allow reentrant event loops so IPython can start cleanly.
    nest_asyncio.apply()

    config = args.config
    db = args.db
    hippo = hippoapi_client(config.hippo)

    console = Console()
    console.print(f'Connected to database: {db}')
    console.print(f'Connected to HiPPO: {hippo}')

    from ..models import ALL_MODELS
    for model in ALL_MODELS:
        globals()[model.__name__] = model

    # --autoawait lets users write bare `await User.find_one(...)` in the shell
    # without wrapping in asyncio.run() — essential for poking at beanie docs.
    start_ipython(
        argv=['--autoawait', 'asyncio'],
        user_ns=locals() | globals(),
    )


@cmd_ipython.args()
def ipython_args(parser: ArgParser):
    pass


@ipython_args.postprocessor(priority=50)
async def _(args: Namespace):
    args.db = await connect_beanie(args.config.mongo, quiet=args.quiet)
