#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) The Regents of the University of California, Davis
# File   : cmds/ipython.py
# License: Modified BSD

import asyncio
import threading
from argparse import Namespace

from IPython import start_ipython
from traitlets.config import Config as IPythonConfig

from . import commands
from ..db import connect_beanie
from ..hippo import hippoapi_client
from ..log import Console


@commands.register('ipython',
                   help='Start an IPython session with the beanie models loaded')
def cmd_ipython(args: Namespace):
    # Ponderosa runs this body inside asyncio.run() (the command tree has
    # async postprocessors), so the main thread's loop is busy for as long as
    # the prompt runs and IPython cannot drive `await` cells on it. Nesting
    # loops with nest_asyncio stopped working on Python 3.14+ (C-implemented
    # tasks refuse re-entry). Instead, run a dedicated event loop on a
    # background thread, connect beanie there (AsyncMongoClient is
    # loop-bound), and give IPython a loop runner that submits each awaiting
    # cell to that loop. Plain cells still run on the main thread.
    config = args.config
    db_loop = asyncio.new_event_loop()
    threading.Thread(target=db_loop.run_forever, name='cheeto-db-loop',
                     daemon=True).start()

    def on_db_loop(coro):
        fut = asyncio.run_coroutine_threadsafe(coro, db_loop)
        try:
            return fut.result()
        except KeyboardInterrupt:
            fut.cancel()
            raise

    db = on_db_loop(connect_beanie(config.mongo, quiet=args.quiet))
    hippo = hippoapi_client(config.hippo)

    console = Console()
    console.print(f'Connected to database: {db}')
    console.print(f'Connected to HiPPO: {hippo}')

    from ..models import ALL_MODELS
    # Build the shell namespace explicitly rather than dumping locals() and
    # globals(): under PEP 810 lazy imports a module global that was never
    # touched is still a lazy proxy, and copying the dict copies the proxy.
    user_ns = {model.__name__: model for model in ALL_MODELS}
    user_ns.update(args=args, config=config, db=db, hippo=hippo, console=console)

    # autoawait lets users write bare `await User.find_one(...)` in the shell
    # without wrapping in asyncio.run() — essential for poking at beanie docs.
    ipy_config = IPythonConfig()
    ipy_config.InteractiveShell.autoawait = True
    ipy_config.InteractiveShell.loop_runner = on_db_loop
    try:
        start_ipython(argv=[], config=ipy_config, user_ns=user_ns)
    finally:
        on_db_loop(db.close())
        db_loop.call_soon_threadsafe(db_loop.stop)
