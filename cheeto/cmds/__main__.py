#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023
# (c) The Regents of the University of California, Davis, 2023
# File   : __main__.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 17.02.2023

import importlib
import sys

from . import commands

# Each command module registers its subcommands on `commands` via
# @commands.register when it is imported; nothing here references the modules
# by name. Import them with importlib (always eager) rather than
# `from . import ...`: under PEP 810 lazy imports (Python 3.15+) an unused
# from-import is deferred forever, and the whole tree must exist before
# commands.run() applies the common args. See cheeto/__init__.py.
for _name in ('config', 'daemon', 'monitor', 'ng', 'nocloud', 'puppet', 'ipython'):
    importlib.import_module(f'.{_name}', __package__)


def main():
    return commands.run()


if __name__ == '__main__':
    sys.exit(main())
