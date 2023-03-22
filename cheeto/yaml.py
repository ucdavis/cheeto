#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023
# (c) The Regents of the University of California, Davis, 2023
# File   : yaml.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 22.03.2023

from mergedeep import merge, Strategy
from ruamel import yaml


def parse_yaml(filename):
    with open(filename) as fp:
        return yaml.safe_load(fp)


def puppet_merge(*dicts):
    return merge(*dicts, strategy=Strategy.ADDITIVE)

