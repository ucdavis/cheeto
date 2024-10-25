#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023-2024
# (c) The Regents of the University of California, Davis, 2023
# File   : __main__.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 31.10.2023

# pyright: reportMissingTypeArgument=true


from argparse import Action, ArgumentParser, Namespace, _SubParsersAction, ArgumentTypeError
import os
from functools import wraps
from pathlib import Path
import re
from typing import Callable, Optional
from typing_extensions import Concatenate, ParamSpec, Union

from .errors import ExitCode


P = ParamSpec('P')
Subparsers = _SubParsersAction
NS = Namespace
NamespaceFunc = Callable[Concatenate[NS, P], Union[int, ExitCode, None]]
SubCommandFunc = Callable[Concatenate[Subparsers, P], None]


def add_common_args(parser):
    from .config import DEFAULT_CONFIG_PATH
    group = parser.add_argument_group('Config')
    group.add_argument('--log', type=Path, default=Path(os.devnull),
                       help='Log to file.')
    group.add_argument('--quiet', default=False, action='store_true')
    group.add_argument('--config', type=Path, default=DEFAULT_CONFIG_PATH,
                       help='Path to alternate config file')
    group.add_argument('--profile', default='default',
                       help='Config profile to use')


def regex_argtype(pattern: re.Pattern[str] | str):
    _pattern = pattern
    def inner(value: str | None):
        if value is None:
            return value
        if not isinstance(_pattern, re.Pattern):
            pattern = re.compile(_pattern)
        else:
            pattern = _pattern
        if not pattern.match(value):
            raise ArgumentTypeError(f'Invalid value, should match: {pattern.pattern}')
        return value
    return inner


def subcommand(subcommand_name: str,
               *arg_adders: Callable[[ArgumentParser], Optional[Action]],
               help: Optional[str] = None) \
-> Callable[[NamespaceFunc[P]], SubCommandFunc[P]]:

    def wrapper(func: NamespaceFunc[P]) -> SubCommandFunc[P]:
   
        @wraps(func)
        def wrapped(parent_parser: Subparsers, *args: P.args, **kwargs: P.kwargs) -> None:
            parser = parent_parser.add_parser(subcommand_name, help=help)
            add_common_args(parser)
            parser.set_defaults(func=func)
            for adder in arg_adders:
                adder(parser)

        return wrapped

    return wrapper
