#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023-2024
# (c) The Regents of the University of California, Davis, 2023
# File   : errors.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 15.05.2023

from enum import IntEnum


class ExitCode(IntEnum):
    VALIDATION_ERROR = 1
    BAD_MERGE = 2
    INVALID_SPONSOR = 3
    FILE_EXISTS = 4
    BAD_LDAP_QUERY = 5
    BAD_CMDLINE_ARGS = 6
    NOT_UNIQUE = 7
    DOES_NOT_EXIST = 8
    INVALID_METADATA = 9
    OPERATION_CANCELLED = 10