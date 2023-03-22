#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023
# (c) The Regents of the University of California, Davis, 2023
# File   : types.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 22.03.2023

from marshmallow import validate as mv
from marshmallow import fields as mf
from marshmallow_dataclass import NewType

from mashumaro.config import (BaseConfig,
                              TO_DICT_ADD_OMIT_NONE_FLAG)
from mashumaro.mixins.yaml import DataClassYAMLMixin


UINT_MAX = 4_294_967_296


class BaseModel(DataClassYAMLMixin):
    class Config(BaseConfig):
        code_generation_options = [TO_DICT_ADD_OMIT_NONE_FLAG]


KerberosID = NewType(
    "KerberosID", str, validate=mv.Regexp(r'[a-z_]([a-z0-9_-]{0,31}|[a-z0-9_-]{0,30}\$)')
)

MothraID = NewType(
    "MothraID", int, validate=mv.Range(min=0, max=UINT_MAX)
)

IAMID = NewType(
    "IAMID", int, validate=mv.Range(min=0, max=UINT_MAX)
)

Email = NewType(
    "Email", str, field=mf.Email
)

IPv4 = NewType(
    "IPv4", str, mf.IPv4
)

PuppetEnsure = NewType(
    "PuppetEnsure", str, validate=mv.OneOf(("present", "absent"))
)

PuppetMembership = NewType(
    "PuppetMembership", str, validate=mv.OneOf(("inclusive", "minimum"))
)

LinuxUID = NewType(
    "LinuxUID", int, validate=mv.Range(min=0, max=UINT_MAX)
)

LinuxGID = NewType(
    "LinuxGID", int, validate=mv.Range(min=0, max=UINT_MAX)
)

# TODO: Needs to actually be "x" or "min length"
LinuxPassword = NewType(
    "LinuxPassword", str, validate=mv.Length(min=1)
)

Shell = NewType(
    "Shell", str, validate=mv.OneOf(("/bin/sh", "/bin/bash", "/bin/zsh"))
)

ZFSQuota = NewType(
    "ZFSQuota", str, validate=mv.Regexp(r'[0-9]+[MGTP]')
)
