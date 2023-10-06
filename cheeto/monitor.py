#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023
# (c) The Regents of the University of California, Davis, 2023
# File   : monitor.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 01.05.2023

from datetime import datetime
from io import StringIO
import time

import sh


def add_power_args(parser):
    parser.add_argument('-o', '--output', default='/dev/stdout')
    parser.add_argument('--interval', default=5, type=int)


def parse_dcmi_power(dcmi_str: str):
    lines = [l for l in (l.strip() for l in dcmi_str.split('\n')) if l]
    _, _, reading = lines[0].partition(':')
    return int(reading.removesuffix('Watts').strip())


def power(args):
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

