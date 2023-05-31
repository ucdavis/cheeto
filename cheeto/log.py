#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023
# (c) The Regents of the University of California, Davis, 2023
# File   : log.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 23.05.2023

import logging
from typing import TextIO

from rich.console import Console
from rich.logging import RichHandler


def setup(log_file: TextIO,
          quiet: bool = False):

    handlers = [RichHandler(console=Console(file=log_file))]

    if not quiet:
        handlers.append(RichHandler(console=Console(stderr=True),
                                    markup=True))

    logging.basicConfig(
        level=logging.NOTSET,
        format='%(message)s',
        #format='%(asctime)s %(levelname)10s [%(filename)s:%(lineno)s - %(funcName)20s()] %(message)s',
        datefmt="[%x %X]",
        handlers=handlers
    )

