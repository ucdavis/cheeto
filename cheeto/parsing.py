#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023
# (c) The Regents of the University of California, Davis, 2023
# File   : parsing.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 11.12.2023

from enum import Enum
import logging
import os
from typing import Optional, Type
import sys

import marshmallow
from mergedeep import merge, Strategy
from ruamel import yaml as ryaml

from .errors import ExitCode
from .types import BaseModel


class MergeStrategy(Enum):
    ALL = 'all'
    PREFIX = 'prefix'
    NONE = 'none'


def parse_yaml(filename: str) -> dict:
    try:
        with open(filename) as fp:
            parsed = ryaml.safe_load(fp)
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


def validate_yaml_forest(yaml_forest: dict,
                         MapSchema: Type[BaseModel],
                         strict: Optional[bool] = False,
                         partial: Optional[bool] = False): 

    logger = logging.getLogger(__name__)

    for source_root, yaml_obj in yaml_forest.items():

        try:
            puppet_data = MapSchema.Schema().load(yaml_obj, #type: ignore
                                                  partial=partial)
        except marshmallow.exceptions.ValidationError as e: #type: ignore
            logger.error(f'[red]ValidationError: {source_root}[/]')
            logger.error(e.messages)
            if strict:
                sys.exit(ExitCode.VALIDATION_ERROR)
            continue
        else:
            yield source_root, puppet_data
