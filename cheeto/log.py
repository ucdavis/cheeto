#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023-2024
# (c) The Regents of the University of California, Davis, 2023
# File   : log.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 23.05.2023

import logging
from typing import TextIO

from rich.console import Console as _Console
from rich.logging import RichHandler


def Console(*args, **kwargs):
    return _Console(*args, soft_wrap=True, **kwargs)


def setup(log_file: TextIO,
          quiet: bool = False,
          level=logging.INFO):

    handlers = [RichHandler(console=Console(file=log_file))]

    if not quiet:
        handlers.append(RichHandler(console=Console(stderr=True),
                                    markup=True))

    logging.basicConfig(
        level=level,
        format='%(funcName)20s: %(message)s',
        #format='%(asctime)s %(levelname)10s [%(filename)s:%(lineno)s - %(funcName)20s()] %(message)s',
        datefmt="[%x %X]",
        handlers=handlers
    )

