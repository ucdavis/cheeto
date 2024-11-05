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
          level=logging.INFO):
    logging.basicConfig(
        level=level,
        format='%(funcName)s: %(message)s',
        datefmt="[%x %X]",
        handlers=[RichHandler(console=Console(file=log_file))]
    )

