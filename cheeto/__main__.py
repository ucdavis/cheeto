#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023
# (c) The Regents of the University of California, Davis, 2023
# File   : __main__.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 17.02.2023

import sys

from .args import commands, banner
from . import __version__
from . import config
from . import database
from . import hippo
from . import monitor
from . import log
from . import nocloud
from . import puppet
from . import slurm
from . import templating


@commands.register('help',
                   help='Show the full subcomamnd tree')
def help(args):
    console = log.Console()
    print(banner, file=sys.stderr)
    console.print(commands.format_help())
    return 0


def main():
    commands._root.add_argument('--version', action='version', version=f'cheeto {__version__}')
    return commands.run()


if __name__ == '__main__':
    sys.exit(main())
