#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023-2024
# (c) The Regents of the University of California, Davis, 2023-2024
# File   : cmds/config.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 04.11.2024

from argparse import Namespace
from pathlib import Path
import sys

from ponderosa import ArgParser, arggroup
import sh

from . import commands
from ..monitor import parse_dcmi_power, poll_dcmi_power


@commands.register('monitor', 'power',
                   help='Monitor power usage from DCMI')
def power(args: Namespace):
    poll_dcmi_power(args.output, args.interval)


@power.args()
def add_power_args(parser: ArgParser):
    parser.add_argument('-o', '--output', default='/dev/stdout')
    parser.add_argument('--interval', default=5, type=int)