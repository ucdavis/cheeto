#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023
# (c) The Regents of the University of California, Davis, 2023
# File   : slurm.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 12.04.2023

from yaml import *

from collections import OrderedDict


def represent_dict(dumper, instance):
    return dumper.represent_mapping('tag:yaml.org,2002:map', instance.items())

add_representer(OrderedDict, represent_dict)

dumps = dump
