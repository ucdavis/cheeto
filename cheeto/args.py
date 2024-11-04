#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023-2024
# (c) The Regents of the University of California, Davis, 2023
# File   : __main__.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 31.10.2023

# pyright: reportMissingTypeArgument=true


from argparse import (Action,
                      ArgumentTypeError,
                      RawDescriptionHelpFormatter)
from enum import Enum
import re

from ponderosa import CmdTree, ArgParser, arggroup

from . import __version__
from .templating import PKG_TEMPLATES



splash = (PKG_TEMPLATES / 'banner.txt').read_text().rstrip('\n').format(version=f'v{__version__}')
banner = f'''
\b
{splash}
'''

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


class EnumAction(Action):
    """
    Argparse action for handling Enums
    """
    def __init__(self, **kwargs):
        # Pop off the type value
        enum = kwargs.pop("type", None)

        # Ensure an Enum subclass is provided
        if enum is None:
            raise ValueError("type must be assigned an Enum when using EnumAction")
        if not issubclass(enum, Enum):
            raise TypeError("type must be an Enum when using EnumAction")

        # Generate choices from the Enum
        kwargs.setdefault("choices", tuple(e.name for e in enum))

        super(EnumAction, self).__init__(**kwargs)

        self._enum = enum

    def __call__(self, parser, namespace, values, option_string=None):
        # Convert value back into an Enum
        enum = self._enum[values]
        setattr(namespace, self.dest, enum)


commands = CmdTree(description=banner,
                   formatter_class=RawDescriptionHelpFormatter)

