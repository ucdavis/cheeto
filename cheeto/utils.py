#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023-2024
# (c) The Regents of the University of California, Davis, 2023
# File   : utils.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 17.02.2023

import argparse
from dataclasses import is_dataclass
from datetime import datetime
from enum import Enum
import inspect
from pathlib import Path
from typing import TypeVar, Type, Callable, List, Dict, Any


__pkg_dir__ = Path(__file__).resolve().parent
TIMESTAMP_NOW = datetime.now()


def sanitize_timestamp(ts: datetime) -> str:
    return ts.strftime('%Y-%m-%d.%H-%M-%S')


def human_timestamp(ts: datetime) -> str:
    return ts.strftime('%Y-%m-%d %H:%M:%S')


def filter_nulls(d: dict) -> dict:
    return {key: val for key, val in d.items() if val}


def remove_nones(d: dict):
    for key in list(d.keys()):
        if d[key] is None:
            del d[key]


def removed_nones(d: dict) -> dict:
    return {k: v for k, v in d.items() if v is not None}


def check_filter(d: dict, filter_on: dict):
    for key, val in d.items():
        if val in filter_on.get(key, []):
            return True
    return False


def removed(d: dict, key: Any):
    try:
        del d[key]
    except KeyError:
        pass
    return d


def _ctx_name():
    return inspect.stack()[1].function


def make_ngrams(word: str,
                min_size: int = 2,
                prefix: bool = False,
                stop_chars: list[str] = ['@']) -> list[str]:
    length = len(word)
    size_range = range(min_size, max(length, min_size) + 1)
    if prefix:
        return [
            word[0:size]
            for size in size_range
        ]
    return list(set(
        word[i:i+size]
        for size in size_range
        for i in range(0, max(0, length-size) + 1)
        if not word[i] == ' ' and not word[i+size-1] == ' '
    ))


def get_relative_path(lower_path: Path, upper_path: Path):
    diff = lower_path.relative_to(upper_path)
    levels = len(diff.parents)
    return Path.joinpath(*([Path('..')] * levels))


def link_relative(link_dir: Path, target_filename: Path):
    relative = get_relative_path(link_dir, target_filename.parent)
    target_filename = relative / target_filename.name
    link_dir.joinpath(target_filename.name).symlink_to(target_filename)


def size_to_megs(size: str) -> int:
    size = size.strip()
    if size[-1] in 'Mm':
        return int(float(size[:-1]))
    if size[-1] in 'Gg':
        return int(float(size[:-1]) * 1024)
    if size[-1] in 'Tt':
        return int(float(size[:-1]) * 1024 * 1024)
    else:
        raise ValueError(f'{size} is not an allowed value.')


class EnumAction(argparse.Action):
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


_T = TypeVar("_T")
_Self = TypeVar("_Self")
_VarArgs = List[Any]
_KWArgs = Dict[str, Any]


#
# Dataclass utilities borrowed from: 
# https://gist.github.com/mikeholler/4be180627d3f8fceb55704b729464adb
#

def _kwarg_only_init_wrapper(
        self: _Self,
        init: Callable[..., None],
        *args: _VarArgs,
        **kwargs: _KWArgs
) -> None:
    if len(args) > 0:
        raise TypeError(
            f"{type(self).__name__}.__init__(self, ...) only allows keyword arguments. Found the "
            f"following positional arguments: {args}"
        )
    init(self, **kwargs)


def _positional_arg_only_init_wrapper(
        self: _Self,
        init: Callable[..., None],
        *args: _VarArgs,
        **kwargs: _KWArgs
) -> None:
    if len(kwargs) > 0:
        raise TypeError(
            f"{type(self).__name__}.__init__(self, ...) only allows positional arguments. Found "
            f"the following keyword arguments: {kwargs}"
        )
    init(self, *args)


def require_kwargs(cls: Type[_T]) -> Type[_T]:
    """
    Force a dataclass's init function to only work if called with keyword arguments.
    If parameters are not positional-only, a TypeError is thrown with a helpful message.
    This function may only be used on dataclasses.
    This works by wrapping the __init__ function and dynamically replacing it. Therefore,
    stacktraces for calls to the new __init__ might look a bit strange. Fear not though,
    all is well.
    Note: although this may be used as a decorator, this is not advised as IDEs will no longer
    suggest parameters in the constructor. Instead, this is the recommended usage::
        from dataclasses import dataclass
        @dataclass
        class Foo:
            bar: str
        require_kwargs_on_init(Foo)
    """

    if cls is None:
        raise TypeError("Cannot call with cls=None")
    if not is_dataclass(cls):
        raise TypeError(
            f"This decorator only works on dataclasses. {cls.__name__} is not a dataclass."
        )

    original_init = cls.__init__

    def new_init(self: _Self, *args: _VarArgs, **kwargs: _KWArgs) -> None:
        _kwarg_only_init_wrapper(self, original_init, *args, **kwargs)

    # noinspection PyTypeHints
    cls.__init__ = new_init  # type: ignore

    return cls


def require_positional_args(cls: Type[_T]) -> Type[_T]:
    """
    Force a dataclass's init function to only work if called with positional arguments.
    If parameters are not positional-only, a TypeError is thrown with a helpful message.
    This function may only be used on dataclasses.
    This works by wrapping the __init__ function and dynamically replacing it. Therefore,
    stacktraces for calls to the new __init__ might look a bit strange. Fear not though,
    all is well.
    Note: although this may be used as a decorator, this is not advised as IDEs will no longer
    suggest parameters in the constructor. Instead, this is the recommended usage::
        from dataclasses import dataclass
        @dataclass
        class Foo:
            bar: str
        require_positional_args_on_init(Foo)
    """

    if cls is None:
        raise TypeError("Cannot call with cls=None")
    if not is_dataclass(cls):
        raise TypeError(
            f"This decorator only works on dataclasses. {cls.__name__} is not a dataclass."
        )

    original_init = cls.__init__

    def new_init(self: _Self, *args: _VarArgs, **kwargs: _KWArgs) -> None:
        _positional_arg_only_init_wrapper(self, original_init, *args, **kwargs)

    # noinspection PyTypeHints
    cls.__init__ = new_init  # type: ignore

    return cls
