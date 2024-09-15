#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023
# (c) The Regents of the University of California, Davis, 2023
# File   : _yaml.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 12.04.2023

from collections import OrderedDict, defaultdict
from typing import Any, Type
from enum import Enum
import os
from pathlib import Path
from typing import Generator, Optional, Union
import sys

from bson import ObjectId, objectid
from bson.int64 import Int64
from mergedeep import merge, Strategy
from mongoengine.dereference import DBRef
from rich.syntax import Syntax

from ruamel import yaml as ryaml
from ruamel.yaml.compat import StringIO
from ruamel.yaml.representer import RoundTripRepresenter

from .errors import ExitCode


class OrderedDictRepresenter(RoundTripRepresenter):
    pass


def str_representer(dumper, data):
    if len(data) > 80:
        return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='"')
    else:
        return dumper.represent_scalar('tag:yaml.org,2002:str', data)


def objectid_representer(dumper, data):
    return dumper.represent_scalar('tag:yaml.org,2002:str', repr(data))


def dbref_representer(dumper, data):
    return dumper.represent_scalar('tag:yaml.org,2002:str', repr(data))


def dumps(obj: Any, *args, many: bool | None = None, **kwargs) -> str:
    dumper = ryaml.YAML()
    dumper.width = sys.maxsize
    dumper.Representer.add_representer(OrderedDict, RoundTripRepresenter.represent_dict)
    dumper.Representer.add_representer(defaultdict, RoundTripRepresenter.represent_dict)
    dumper.Representer.add_representer(str, str_representer)
    dumper.Representer.add_representer(Int64, RoundTripRepresenter.represent_int)
    dumper.Representer.add_representer(ObjectId, objectid_representer)
    dumper.Representer.add_representer(DBRef, dbref_representer)
    dumper.Representer.add_representer(set, RoundTripRepresenter.represent_list)
    stream = StringIO()
    dumper.dump(obj, stream, **kwargs)
    return stream.getvalue()


class MergeStrategy(Enum):
    ALL = 'all'
    PREFIX = 'prefix'
    NONE = 'none'


def parse_yaml(filename: Union[str, Path]) -> dict:
    try:
        with open(str(filename)) as fp:
            parsed = ryaml.YAML(typ='safe').load(fp)
            if parsed is None:
                return {}
            return parsed
    except FileNotFoundError:
        return {}


def puppet_merge(*dicts: dict) -> dict:
    '''Merge dictionaries together using puppet's deep merge strategy.


    Args:
        dicts: Arbitrary dicts to be deep merged.

    Returns:
        dict: The resulting merged dict.
    '''
    return merge(*dicts, strategy=Strategy.ADDITIVE) #type: ignore


def parse_yaml_forest(yaml_files: list,
                      merge_on: Optional[MergeStrategy] = MergeStrategy.NONE) -> dict:
    yaml_forest = {}
    if merge_on is MergeStrategy.ALL:
        parsed_yamls = [parse_yaml(f) for f in yaml_files]
        yaml_forest = {'merged-all': puppet_merge(*parsed_yamls)}

    elif merge_on is MergeStrategy.NONE:
        yaml_forest = {f: parse_yaml(f) for f in yaml_files}

    elif merge_on is MergeStrategy.PREFIX:
        file_groups = {}
        for filename in yaml_files:
            prefix, _, _ = os.path.basename(filename).partition('.')
            if prefix in file_groups:
                file_groups[prefix].append(parse_yaml(filename))
            else:
                file_groups[prefix] = [parse_yaml(filename)]
        yaml_forest = {prefix: puppet_merge(*yamls) for prefix, yamls in file_groups.items()}

    return yaml_forest


def highlight_yaml(dumped: str):
    return Syntax(dumped,
                  'yaml',
                  theme='github-dark',
                  background_color='default')
