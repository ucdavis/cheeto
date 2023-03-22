#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023
# (c) The Regents of the University of California, Davis, 2023
# File   : hippo.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 17.02.2023

from dataclasses import dataclass, field
import os

from .utils import require_kwargs, __pkg_dir__

from mashumaro.mixins.yaml import DataClassYAMLMixin
import yamale


@require_kwargs
@dataclass(frozen=True)
class HippoSponsor(DataClassYAMLMixin):
    accountname: str
    name: str
    email: str
    kerb: str
    iam: int
    mothra: int


@require_kwargs
@dataclass(frozen=True)
class HippoAccount(DataClassYAMLMixin):
    name: str
    email: str
    kerb: str
    iam: int
    mothra: int
    key: str


@require_kwargs
@dataclass(frozen=True)
class HippoRecord(DataClassYAMLMixin):
    sponsor: HippoSponsor
    account: HippoAccount

    @staticmethod
    def parse_and_validate(filename):
        schema = yamale.make_schema(os.path.join(__pkg_dir__, 
                                                 'schemas', 
                                                 'hippo.schema.yaml'))
        data = yamale.make_data(filename, parser='ruamel')
        try:
            yamale.validate(schema, data)
            return data
        except yamale.YamaleError as e:
            print(f'Validation error: {filename}', file=sys.stderr)
            for result in e.results:
                print("Error validating data '%s' with '%s'\n\t" % (result.data, result.schema))
                for error in result.errors:
                    print('\t%s' % error)
            return None

    @classmethod
    def from_yaml_validated(cls, filename):
        data = cls.parse_and_validate(filename)
        if data is None:
            raise ValueError()
        return cls.from_dict(data[0][0])
