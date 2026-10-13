#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) Camille Scott, 2026
# (c) The Regents of the University of California, Davis, 2026
# File   : tres.py
# License: Modified BSD
# Author : Camille Scott <cswel@ucdavis.edu>
# Date   : 13.10.2026

"""Parser for the Slurm TRES specs the CLI accepts ("cpus=128 mem=1T gpus=8").

This is a light module (stdlib + cheeto.utils). The argparse `type=` callables
below are referenced when the command tree is built, so importing beanie here
would make `cheeto --help` load it (see tests/test_lazy_imports.py).
"""

from argparse import ArgumentTypeError
from decimal import Decimal
import re

from .utils import _SIZE_UNIT_MEGS, size_to_megs_exact


class TRESParseError(ValueError):
    """A TRES spec, or one of its values, could not be parsed."""


TRES_KEYS = ('cpus', 'gpus', 'mem')
TRES_REQUIRED_KEYS = ('cpus', 'mem')

_KEY_ALIASES = {
    'cpus': 'cpus', 'cpu': 'cpus',
    'gpus': 'gpus', 'gpu': 'gpus', 'gres/gpu': 'gpus',
    'mem': 'mem', 'memory': 'mem',
}
_UNLIMITED = frozenset({'-1', 'inf', 'infinity', 'unlimited'})
_UNITS = ', '.join(_SIZE_UNIT_MEGS)   # 'M, G, T, P'
_MEM_RE = re.compile(r'^(?P<num>\d+(?:\.\d+)?|\.\d+)(?P<unit>[A-Za-z]*)$')
_SEP_RE = re.compile(r'[,\s]+')


def is_unlimited(value) -> bool:
    """True for the explicit 'no limit' spellings: -1, inf, infinity, unlimited."""
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return value == -1
    return isinstance(value, str) and value.strip().lower() in _UNLIMITED


def normalize_count(value, name: str) -> int | None:
    """A cpus/gpus value: None for unlimited, else a non-negative int."""
    if value is None or is_unlimited(value):
        return None
    if isinstance(value, int) and not isinstance(value, bool):
        if value >= 0:
            return value
    elif isinstance(value, str):
        text = value.strip()
        if text.isascii() and text.isdigit():
            return int(text)
    raise TRESParseError(
        f'{name}={value!r} is not a whole number, -1, inf or unlimited'
    )


def normalize_mem(value) -> str | None:
    """A mem value: None for unlimited, else the canonical '<number><UNIT>' form.

    The number is Decimal-normalized and the unit is uppercase with any B/iB
    suffix dropped, which is what `megs_to_size` emits, so stored values
    round-trip unchanged. A bare '0' is allowed and becomes '0M'; every other
    bare number is an error, because 768 could mean 768M or 768G.
    """
    if value is None or is_unlimited(value):
        return None
    text = str(value).strip()
    if text == '0':
        return '0M'
    match = _MEM_RE.match(text)
    if match is None:
        raise TRESParseError(
            f'mem={text!r} is not a size; expected <number><unit> '
            f'with unit {_UNITS}, or 0, or inf'
        )
    num, unit = match['num'], match['unit'].upper()
    if not unit:
        raise TRESParseError(
            f'mem={text!r}: a unit is required ({_UNITS}); for example {text}G'
        )
    unit = unit.removesuffix('IB').removesuffix('B')
    if unit not in _SIZE_UNIT_MEGS:
        raise TRESParseError(f'mem={text!r}: unit must be one of {_UNITS}')
    result = f'{format(Decimal(num).normalize(), "f")}{unit}'
    size_to_megs_exact(result)   # must be consumable by the renderers
    return result


def parse_tres(spec: str) -> dict[str, int | str | None]:
    """Parse a CLI TRES spec into `SlurmTRES` kwargs.

    Tokens are key=value, separated by commas and/or whitespace. Keys are
    case-insensitive and accept the singular, plural and sacctmgr spellings
    (cpu/cpus, gpu/gpus/gres/gpu, mem/memory). `cpus` and `mem` are required;
    `gpus` defaults to 0. Unlimited is only ever explicit (-1, inf, unlimited).
    """
    tokens = [t for t in _SEP_RE.split(spec.strip()) if t]
    if not tokens:
        raise TRESParseError('empty TRES specification')
    seen: dict[str, str] = {}
    values: dict[str, int | str | None] = {}
    for token in tokens:
        key, sep, value = token.partition('=')
        if not sep or not key or not value:
            raise TRESParseError(f'expected key=value, got {token!r}')
        canonical = _KEY_ALIASES.get(key.lower())
        if canonical is None:
            raise TRESParseError(
                f'unknown resource in {token!r}; '
                f'expected one of {", ".join(TRES_KEYS)}'
            )
        if canonical in seen:
            raise TRESParseError(
                f'duplicate resource {canonical!r}: '
                f'{seen[canonical]!r} and {token!r}'
            )
        seen[canonical] = token
        try:
            if canonical == 'mem':
                values['mem'] = normalize_mem(value)
            else:
                values[canonical] = normalize_count(value, canonical)
        except TRESParseError as e:
            # The normalizer names the canonical key; only an alias needs
            # the typed token to make the message traceable.
            if key.lower() != canonical:
                raise TRESParseError(f'in {token!r}: {e}') from e
            raise
    missing = [key for key in TRES_REQUIRED_KEYS if key not in values]
    if missing:
        raise TRESParseError(
            f'missing required resource(s): {", ".join(missing)} '
            f'(gpus defaults to 0 when omitted)'
        )
    values.setdefault('gpus', 0)
    return {key: values[key] for key in TRES_KEYS}


def tres_argtype(value: str) -> dict[str, int | str | None]:
    """argparse `type=` for a full TRES spec."""
    try:
        return parse_tres(value)
    except TRESParseError as e:
        raise ArgumentTypeError(str(e)) from e


def count_argtype(name: str):
    """argparse `type=` for a single cpus/gpus value.

    Validates and returns the raw string, so a `default=None` flag still reads
    as 'not given' when the value normalizes to None (unlimited). The handler
    runs `normalize_count` on it.
    """
    def inner(value: str) -> str:
        try:
            normalize_count(value, name)
        except TRESParseError as e:
            raise ArgumentTypeError(str(e)) from e
        return value.strip()
    inner.__name__ = f'{name}_argtype'
    return inner


def mem_argtype(value: str) -> str:
    """argparse `type=` for a single mem value; see `count_argtype`."""
    try:
        normalize_mem(value)
    except TRESParseError as e:
        raise ArgumentTypeError(str(e)) from e
    return value.strip()
