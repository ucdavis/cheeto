#!/usr/bin/env python
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023-2025
# (c) The Regents of the University of California, Davis, 2023-2024
# File   : database/fields.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 29.01.2025

from mongoengine import IntField, StringField

from ..types import (DATA_QUOTA_REGEX,
                     DEFAULT_SHELL,
                     DISABLED_SHELLS, 
                     ENABLED_SHELLS, 
                     GROUP_TYPES,
                     UINT_MAX,
                     USER_TYPES,
                     USER_STATUSES, 
                     ACCESS_TYPES,
                     SlurmQOSValidFlags)


def POSIXNameField(**kwargs):
    return StringField(regex=r'[a-z_]([a-z0-9_-]{0,31}|[a-z0-9_-]{0,30}\$)',
                       min_length=1,
                       max_length=32,
                       **kwargs)

def POSIXIDField(**kwargs) -> IntField:
    return IntField(min_value=0, 
                    max_value=UINT_MAX,
                    **kwargs)

def UInt32Field(**kwargs) -> IntField:
    return IntField(min_value=0,
                    max_value=UINT_MAX,
                    **kwargs)

def UserTypeField(**kwargs): 
    return StringField(choices=USER_TYPES,
                       **kwargs)

def UserStatusField(**kwargs):
    return StringField(choices=USER_STATUSES,
                       **kwargs)

def UserAccessField(**kwargs) -> StringField:
    return StringField(choices=ACCESS_TYPES,
                       **kwargs)

def GroupTypeField(**kwargs) -> StringField:
    return StringField(choices=GROUP_TYPES,
                       **kwargs)

def ShellField(**kwargs) -> StringField:
    return StringField(choices=ENABLED_SHELLS | DISABLED_SHELLS,
                       default=DEFAULT_SHELL,
                       **kwargs)

def DataQuotaField(**kwargs) -> StringField:
    return StringField(regex=DATA_QUOTA_REGEX,
                       **kwargs)

def SlurmQOSFlagField(**kwargs) -> StringField:
    return StringField(choices=SlurmQOSValidFlags,
                       **kwargs)