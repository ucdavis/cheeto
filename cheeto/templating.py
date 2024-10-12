#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023-2024
# (c) The Regents of the University of California, Davis, 2023
# File   : templating.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 27.07.2023

from jinja2 import Environment, FileSystemLoader

from .utils import __pkg_dir__


PKG_TEMPLATES = __pkg_dir__ / 'templates'
