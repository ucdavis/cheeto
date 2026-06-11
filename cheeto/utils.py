#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2023-2024
# (c) The Regents of the University of California, Davis, 2023
# File   : utils.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 17.02.2023

from dataclasses import is_dataclass
from datetime import datetime
from decimal import Decimal
import inspect
from pathlib import Path
import re
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


def slugify(s: str) -> str:
    return re.sub(r'[^\w]+', '-', s).strip('-').lower()


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


_SIZE_UNIT_MEGS = {
    'M': Decimal(1),
    'G': Decimal(1024),
    'T': Decimal(1024 ** 2),
    'P': Decimal(1024 ** 3),
}


def size_to_megs_exact(size: str) -> Decimal:
    """Exact decimal megabytes for a size string: '45.8T' -> 48024780.8.

    Use this (with `megs_to_size`) wherever sizes are summed and rendered
    back to human units — float arithmetic turns 45.8T into 45.79999924T."""
    size = size.strip()
    unit = size[-1].upper()
    if unit not in _SIZE_UNIT_MEGS:
        raise ValueError(f'{size} is not an allowed value.')
    return Decimal(size[:-1]) * _SIZE_UNIT_MEGS[unit]


def size_to_megs(size: str) -> int:
    """Whole megabytes, truncated — what sacctmgr TRES values expect.
    (Truncation, not rounding, preserves what v1 wrote into slurm.)"""
    return int(size_to_megs_exact(size))


def megs_to_size(megs: Decimal | int) -> str:
    """Most compact M/G/T size string for a megabyte count, using exact
    decimal arithmetic (inverse of `size_to_megs_exact`)."""
    if not isinstance(megs, Decimal):
        megs = Decimal(str(megs))
    if megs >= _SIZE_UNIT_MEGS['T']:
        value, unit = megs / _SIZE_UNIT_MEGS['T'], 'T'
    elif megs >= _SIZE_UNIT_MEGS['G']:
        value, unit = megs / _SIZE_UNIT_MEGS['G'], 'G'
    else:
        value, unit = megs, 'M'
    # format(..., 'f') keeps normalize()'s stripped-zero form out of
    # scientific notation (Decimal('100').normalize() is 1E+2).
    return f'{format(value.normalize(), "f")}{unit}'



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
