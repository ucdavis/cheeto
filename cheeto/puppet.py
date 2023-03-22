#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023
# (c) The Regents of the University of California, Davis, 2023
# File   : puppet.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 21.02.2023

from dataclasses import dataclass
import os
from typing import Optional, List, Mapping, Union

import marshmallow_dataclass

from .types import *
from .utils import require_kwargs
from .yaml import parse_yaml, puppet_merge


@require_kwargs
@dataclass(frozen=True)
class PuppetAutofs(BaseModel):
    nas: str
    path: str # TODO: path-like


@require_kwargs
@dataclass(frozen=True)
class PuppetZFS(BaseModel):
    quota: ZFSQuota


@require_kwargs
@dataclass(frozen=True)
class PuppetUserStorage(BaseModel):
    zfs: Union[PuppetZFS, bool]
    autofs: Optional[PuppetAutofs]


@require_kwargs
@dataclass(frozen=True)
class PuppetUserRecord(BaseModel):
    fullname: str
    email: Email
    uid: LinuxUID
    gid: LinuxGID
    groups: Optional[List[str]] = None
    password: Optional[LinuxPassword] = None
    shell: Optional[Shell] = None
    tag: Optional[List[str]] = None

    ensure: Optional[PuppetEnsure] = None
    membership: Optional[PuppetMembership] = None

    storage: Optional[PuppetUserStorage] = None

    class Meta:
        ordered = True


@require_kwargs
@dataclass(frozen=True)
class PuppetUserMap(BaseModel):
    user: Mapping[KerberosID, PuppetUserRecord]


PuppetUserRecordSchema = marshmallow_dataclass.class_schema(PuppetUserRecord)
PuppetUserMapSchema    = marshmallow_dataclass.class_schema(PuppetUserMap)


def add_validate_args(parser):
    parser.add_argument('--merge', action='store_true', default=False,
                        help='Merge the given YAML files before validation.')
    parser.add_argument('--dump', default='/dev/stdout',
                        help='Dump the validated YAML to the given file')
    parser.add_argument('files', nargs='+',
                        help='YAML files to validate.')


def validate_yamls(args):
    parsed_yamls = []
    for filename in args.files:
        parsed_yamls.append(parse_yaml(filename))

    if args.merge:
        parsed_yamls = [puppet_merge(*parsed_yamls)]
    
    for yaml_obj in parsed_yamls:
        puppet_data = PuppetUserMapSchema().load(yaml_obj)
        if args.dump:
            with open(args.dump, 'w') as fp:
                print(puppet_data.to_yaml(omit_none=True), file=fp)

