#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023
# (c) The Regents of the University of California, Davis, 2023
# File   : hippo.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 17.02.2023

from dataclasses import dataclass
import os
import sys

import marshmallow_dataclass

from .puppet import PuppetUserRecord, PuppetUserMap
from .utils import require_kwargs
from .types import *
from .utils import parse_yaml


@require_kwargs
@dataclass(frozen=True)
class HippoSponsor(BaseModel):
    accountname: str
    name: str
    email: Email
    kerb: KerberosID
    iam: IAMID
    mothra: MothraID


@require_kwargs
@dataclass(frozen=True)
class HippoAccount(BaseModel):
    name: str
    email: Email
    kerb: KerberosID
    iam: IAMID
    mothra: MothraID
    key: str


@require_kwargs
@dataclass(frozen=True)
class HippoRecord(BaseModel):
    sponsor: HippoSponsor
    account: HippoAccount


HippoAccountSchema = marshmallow_dataclass.class_schema(HippoSponsor)
HippoSponsorSchema = marshmallow_dataclass.class_schema(HippoSponsor)
HippoRecordSchema  = marshmallow_dataclass.class_schema(HippoRecord)


def add_convert_args(parser):
    parser.add_argument('-i', '--hippo-file', required=True)
    parser.add_argument('-o', '--puppet-file', default='/dev/stdout')
    parser.add_argument('--key-dir')


def convert_to_puppet(args):
    hippo_yaml = parse_yaml(args.hippo_file)
    hippo_record = HippoRecordSchema().load(hippo_yaml)

    user_record = PuppetUserRecord(
        fullname = hippo_record.account.name,
        email = hippo_record.account.email,
        uid = hippo_record.account.mothra,
        gid = hippo_record.account.mothra,
        groups = [f'{hippo_record.sponsor.kerb}grp']
    )

    user_map = PuppetUserMap(
        user = {hippo_record.account.kerb: user_record}
    )

    with open(args.puppet_file, 'w') as fp:
        print(PuppetUserMap.Schema().dumps(user_map), file=fp)

    if args.key_dir:
        key_dest = os.path.join(args.key_dir,
                                f'{hippo_record.account.kerb}.pub')
        with open(key_dest, 'w') as fp:
            print(hippo_record.account.key, file=fp)

