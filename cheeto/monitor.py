#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023-2024
# (c) The Regents of the University of California, Davis, 2023
# File   : monitor.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 01.05.2023

from argparse import Namespace
from datetime import datetime
import time

import sh

from .args import commands, ArgParser


def parse_dcmi_power(dcmi_str: str):
    lines = [l for l in (l.strip() for l in dcmi_str.split('\n')) if l]
    _, _, reading = lines[0].partition(':')
    return int(reading.removesuffix('Watts').strip())


@commands.register('monitor', 'power',
                   help='Monitor power usage from DCMI')
def power(args: Namespace):
    cmd = sh.sudo.bake('ipmitool', 'dcmi', 'power', 'reading')
    
    with open(args.output, 'w') as fp:
        try:
            while(True):
                cur_time = datetime.now().isoformat()
                cur_power = parse_dcmi_power(cmd())
                print(f'{cur_time}, {cur_power}', file=fp, flush=True)
                time.sleep(args.interval)
        except KeyboardInterrupt:
            pass


@power.args()
def add_power_args(parser: ArgParser):
    parser.add_argument('-o', '--output', default='/dev/stdout')
    parser.add_argument('--interval', default=5, type=int)
