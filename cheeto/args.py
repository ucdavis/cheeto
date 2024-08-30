#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023
# (c) The Regents of the University of California, Davis, 2023
# File   : __main__.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 31.10.2023

# pyright: reportMissingTypeArgument=true


from argparse import ArgumentParser, Namespace, _SubParsersAction
import os
from functools import wraps
from pathlib import Path
from typing import Callable
from typing_extensions import Concatenate, ParamSpec, Union

from .errors import ExitCode


P = ParamSpec('P')
Subparsers = _SubParsersAction
NS = Namespace
NamespaceFunc = Callable[Concatenate[NS, P], Union[int, ExitCode, None]]
SubCommandFunc = Callable[Concatenate[Subparsers, P], None]


def add_common_args(parser):
    from .config import get_config
    
    parser.add_argument('--log', type=Path, default=Path(os.devnull),
                        help='Log to file.')
    parser.add_argument('--quiet', default=False, action='store_true')
    parser.add_argument('--config', type=get_config, default=get_config())


def subcommand(subcommand_name: str, *arg_adders: Callable[[ArgumentParser], None]) \
-> Callable[[NamespaceFunc[P]], SubCommandFunc[P]]:

    def wrapper(func: NamespaceFunc[P]) -> SubCommandFunc[P]:
   
        @wraps(func)
        def wrapped(parent_parser: Subparsers, *args: P.args, **kwargs: P.kwargs) -> None:
            parser = parent_parser.add_parser(subcommand_name)
            add_common_args(parser)
            parser.set_defaults(func=func)
            for adder in arg_adders:
                adder(parser)

        return wrapped

    return wrapper
