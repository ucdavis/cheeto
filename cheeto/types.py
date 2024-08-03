#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023
# (c) The Regents of the University of California, Davis, 2023
# File   : types.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 22.03.2023

from collections import OrderedDict
from collections.abc import Hashable, Iterable, Sequence
import dataclasses
import datetime
from pathlib import Path
from typing import Annotated, ClassVar, Type, Union

from marshmallow import validate as mv
from marshmallow import fields as mf
from marshmallow import post_dump, Schema as _Schema
from marshmallow_dataclass import dataclass
import marshmallow_dataclass
import marshmallow_dataclass.collection_field

from . import yaml
from .yaml import parse_yaml


UINT_MAX = 4_294_967_296

DEFAULT_SHELL = '/usr/bin/bash'

ENABLED_SHELLS = {"/bin/sh",
                  "/bin/bash",
                  "/bin/zsh",
                  "/usr/bin/sh",
                  "/usr/bin/zsh",
                  "/usr/bin/bash"}

DISABLED_SHELLS = {"/usr/sbin/nologin-account-disabled",
                   "/bin/false",
                   "/usr/sbin/nologin"}


def is_listlike(obj):
    return isinstance(obj, Sequence) and not isinstance(obj, (str, bytes, bytearray))


class _BaseModel:

    SKIP_VALUES = [None, {}, []]

    def items(self):
        return dataclasses.asdict(self).items() #type: ignore

    @post_dump
    def remove_skip_values(self, data, **kwargs):
        return OrderedDict([
            (key, value) for key, value in data.items()
            if value not in BaseModel.SKIP_VALUES
        ])

    @staticmethod
    def _sortable(data):
        if not isinstance(data, Iterable):
            return False
        # Get the type of the first element
        try:
            first = next(iter(data))
        except StopIteration:
            # empty iterable, still sortable
            return True
        keytype = type(first)
        # Check that all elements are of that type
        return isinstance(first, Hashable) and all(map(lambda item: isinstance(item, keytype), data))

    @staticmethod
    def _sort(data):
        if BaseModel._sortable(data):
            if isinstance(data, Sequence) and not isinstance(data, (str, bytes, bytearray)):
                return sorted(data)
            elif isinstance(data, (dict, OrderedDict)):

                return OrderedDict(sorted(data.items(), key=lambda t: t[0]))
            else:
                return data
        else:
            return data

    @post_dump
    def sort_listlikes(self, data, **kwargs):
        return BaseModel._sort(data)


    class Meta:
        ordered = True
        render_module = yaml

    @classmethod
    def load_yaml(cls, filename: Union[Path, str]):
        return cls.Schema().load(parse_yaml(str(filename))) #type: ignore

    def save_yaml(self, filename: Path):
        with filename.open('w') as fp:
            print(type(self).Schema().dumps(self), file=fp) #type: ignore

    def to_raw_yaml(self):
        return type(self).Schema().dump(self) #type: ignore



@dataclass(frozen=True)
class BaseModel(_BaseModel):

    Schema: ClassVar[Type[_Schema]] = _Schema # For the type checker


KerberosID = Annotated[str, mf.String(validate=mv.Regexp(r'[a-z_]([a-z0-9_-]{0,31}|[a-z0-9_-]{0,30}\$)'))]


class SimpleDate(mf.Date):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, format='%Y-%m-%d', **kwargs)


Date = Annotated[datetime.date, SimpleDate]

Email = Annotated[str, mf.Email]

IPv4 = Annotated[str, mf.IPv4]

PuppetEnsure = Annotated[str, mf.String(validate=mv.OneOf(("present", "absent")))]

PuppetMembership = Annotated[str, mf.String(validate=mv.OneOf(("inclusive", "minimum")))]

PuppetAbsent = Annotated[str, mf.String(validate=mv.Equal("absent"))]

UInt32 = Annotated[int, mf.Integer(validate=mv.Range(min=0, max=UINT_MAX))]

IAMID = UInt32

MothraID = UInt32

LinuxUID = UInt32

LinuxGID = UInt32

# TODO: Needs to actually be "x" or "min length"
LinuxPassword = Annotated[str, mf.String(validate=mv.Length(min=0))]

Shell = Annotated[str, mf.String(validate=mv.OneOf(ENABLED_SHELLS | DISABLED_SHELLS))]

DataQuota = Annotated[str, mf.String(validate=mv.Regexp(r'[+-]?([0-9]*[.])?[0-9]+[MmGgTtPp]'))]

SlurmQOSValidFlags = ("DenyOnLimit",
                      "EnforceUsageThreshold",
                      "NoDecay",
                      "NoReserve",
                      "OverPartQOS",
                      "PartitionMaxNodes",
                      "PartitionMinNodes",
                      "PartitionTimeLimit",
                      "RequiresReservation",
                      "UsageFactorSafe")

SlurmQOSFlag = Annotated[str, mf.String(validate=mv.OneOf(SlurmQOSValidFlags))]


SEQUENCE_FIELDS = {
    marshmallow_dataclass.collection_field.Sequence,
    marshmallow_dataclass.collection_field.Set,
    mf.List,
    mf.Tuple
}
