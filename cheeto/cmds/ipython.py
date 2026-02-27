#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023-2026
# (c) The Regents of the University of California, Davis, 2023-2026
# File   : cmds/ipython.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 24.02.2026

from argparse import Namespace

from IPython import start_ipython

from . import commands
from ..database import connect_to_database
from ..ldap import LDAPManager
from ..hippo import hippoapi_client
from ..iam import IAMAPI
from ..log import Console


from ..database import *
from ..ldap import *
from ..hippo import *
from ..iam import *

@commands.register('ipython',
                   help='Start an IPython session')
def cmd_ipython(args: Namespace):

    config = args.config
    db = connect_to_database(config.mongo)
    lm = LDAPManager(config.ldap, pool_keepalive=15, pool_lifetime=30)
    hippo = hippoapi_client(config.hippo)
    iam = IAMAPI(config.ucdiam)

    console = Console()
    console.print(f'Connected to database: {db}')
    console.print(f'Connected to LDAP: {lm}')
    console.print(f'Connected to HiPPO: {hippo}')
    console.print(f'Connected to IAM: {iam}')

    start_ipython(argv=[], user_ns=locals() | globals())