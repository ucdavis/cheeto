#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023
# (c) The Regents of the University of California, Davis, 2023
# File   : types.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 22.03.2023

from collections import OrderedDict
import dataclasses
from pathlib import Path
from typing import Union

from marshmallow import validate as mv
from marshmallow import fields as mf
from marshmallow import post_dump
from marshmallow_dataclass import NewType

from . import _yaml
from .utils import parse_yaml


UINT_MAX = 4_294_967_296


class BaseModel:

    SKIP_VALUES = [None]

    def items(self):
        return dataclasses.asdict(self).items() #type: ignore

    @post_dump
    def remove_skip_values(self, data, **kwargs):
        return OrderedDict([
            (key, value) for key, value in data.items()
            if value not in BaseModel.SKIP_VALUES
        ])

    class Meta:
        ordered = True
        render_module = _yaml

    @classmethod
    def load_yaml(cls, filename: Union[Path, str]):
        return cls.Schema().load(parse_yaml(str(filename))) #type: ignore

    def save_yaml(self, filename: Path):
        with filename.open('w') as fp:
            print(type(self).Schema().dumps(self), file=fp) #type: ignore


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

UInt32 = NewType(
    "UInt32", int, validate=mv.Range(min=0, max=UINT_MAX)
)

LinuxUID = UInt32

LinuxGID = UInt32

# TODO: Needs to actually be "x" or "min length"
LinuxPassword = NewType(
    "LinuxPassword", str, validate=mv.Length(min=0)
)

Shell = NewType(
    "Shell", str, validate=mv.OneOf(
        ("/bin/sh",
         "/bin/bash",
         "/bin/zsh",
         "/usr/bin/zsh",
         "/usr/bin/bash",
         "/usr/sbin/nologin-account-disabled",
         "/bin/false",
         "/usr/sbin/nologin")
    )
)

DataQuota = NewType(
    "DataQuota", str, validate=mv.Regexp(r'[+-]?([0-9]*[.])?[0-9]+[MmGgTtPp]')
)
