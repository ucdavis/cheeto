"""Unit tests for the CLI TRES parser (cheeto/tres.py). No Mongo needed."""

import argparse
from argparse import ArgumentTypeError
from decimal import Decimal

import pytest

from ..tres import (
    TRESParseError,
    count_argtype,
    is_unlimited,
    mem_argtype,
    normalize_count,
    normalize_mem,
    parse_tres,
    tres_argtype,
)
from ..utils import megs_to_size, size_to_megs_exact


UNLIMITED_SPELLINGS = [-1, '-1', 'inf', 'INF', 'Infinity', 'unlimited',
                       'UNLIMITED', ' inf ']


def test_parse_error_is_value_error():
    assert issubclass(TRESParseError, ValueError)


# ---------------------------------------------------------------------------
# is_unlimited
# ---------------------------------------------------------------------------


@pytest.mark.parametrize('value', UNLIMITED_SPELLINGS)
def test_is_unlimited_true(value):
    assert is_unlimited(value)


@pytest.mark.parametrize('value', [None, 0, 1, '0', '1', 'infinite', True, False,
                                   -2, '-2', '', 1.0])
def test_is_unlimited_false(value):
    assert not is_unlimited(value)


# ---------------------------------------------------------------------------
# normalize_mem
# ---------------------------------------------------------------------------


@pytest.mark.parametrize('value', [None, *UNLIMITED_SPELLINGS])
def test_normalize_mem_unlimited(value):
    assert normalize_mem(value) is None


@pytest.mark.parametrize('value', ['0', ' 0 ', 0])
def test_normalize_mem_bare_zero(value):
    assert normalize_mem(value) == '0M'


@pytest.mark.parametrize('value, expected', [
    # stored forms round-trip byte-identical
    ('512000M', '512000M'),
    ('1620G', '1620G'),
    ('1048576M', '1048576M'),
    # megs_to_size output round-trips
    ('2.9296875T', '2.9296875T'),
    ('45.8T', '45.8T'),
    ('0.5M', '0.5M'),
    ('100M', '100M'),
    # case and byte suffixes
    ('1t', '1T'),
    ('16gb', '16G'),
    ('16GB', '16G'),
    ('16GiB', '16G'),
    ('16gib', '16G'),
    ('1.5TB', '1.5T'),
    ('8P', '8P'),
    (' 1T ', '1T'),
    ('0G', '0G'),
    # number canonicalization
    ('.5G', '0.5G'),
    ('1.0T', '1T'),
    ('1.50G', '1.5G'),
    ('0100M', '100M'),
])
def test_normalize_mem_values(value, expected):
    assert normalize_mem(value) == expected


@pytest.mark.parametrize('value, needle', [
    ('16', 'unit is required'),
    ('512000', 'unit is required'),
    ('1.5', 'unit is required'),
    (1024, 'unit is required'),
    ('1K', 'M, G, T, P'),
    ('512K', 'M, G, T, P'),
    ('16KB', 'M, G, T, P'),
    ('16B', 'M, G, T, P'),
    ('-1T', 'not a size'),
    ('-5G', 'not a size'),
    ('1.T', 'not a size'),
    ('T', 'not a size'),
    ('', 'not a size'),
    ('abc', 'not a size'),
    ('1 T', 'not a size'),
    ('1e3M', 'not a size'),
    ('nan', 'not a size'),
    ('16GBB', 'M, G, T, P'),
])
def test_normalize_mem_errors(value, needle):
    with pytest.raises(TRESParseError) as excinfo:
        normalize_mem(value)
    assert needle in str(excinfo.value)
    assert repr(str(value).strip()) in str(excinfo.value)


@pytest.mark.parametrize('megs', [0, 1, 100, 1536, 1048576, 3000000,
                                  Decimal('0.5'), Decimal('48024780.8')])
def test_normalize_mem_round_trips_megs_to_size(megs):
    rendered = megs_to_size(megs)
    assert normalize_mem(rendered) == rendered
    assert size_to_megs_exact(normalize_mem(rendered)) == Decimal(str(megs))


# ---------------------------------------------------------------------------
# normalize_count
# ---------------------------------------------------------------------------


@pytest.mark.parametrize('value', [None, *UNLIMITED_SPELLINGS])
def test_normalize_count_unlimited(value):
    assert normalize_count(value, 'gpus') is None


@pytest.mark.parametrize('value, expected', [
    (0, 0), ('0', 0), (8, 8), ('8', 8), (' 8 ', 8), ('008', 8), (1024, 1024),
])
def test_normalize_count_values(value, expected):
    assert normalize_count(value, 'cpus') == expected


@pytest.mark.parametrize('value', [-5, '-5', '4.5', 4.5, 4.0, 'abc', '', True,
                                   False, '+8', '٨', '8 cores', [8]])
def test_normalize_count_errors(value):
    with pytest.raises(TRESParseError) as excinfo:
        normalize_count(value, 'gpus')
    assert 'gpus' in str(excinfo.value)


# ---------------------------------------------------------------------------
# parse_tres
# ---------------------------------------------------------------------------


FULL = {'cpus': 128, 'gpus': 8, 'mem': '1T'}


@pytest.mark.parametrize('spec', [
    'cpus=128 mem=1T gpus=8',
    'cpus=128,mem=1T,gpus=8',
    'cpus=128, mem=1T ,gpus=8',
    '  cpus=128\tmem=1T  gpus=8 ',
    'gpus=8 cpus=128 mem=1T',
    'mem=1T,,gpus=8 , cpus=128',
])
def test_parse_tres_separators_and_order(spec):
    assert parse_tres(spec) == FULL


@pytest.mark.parametrize('spec', [
    # issue #91: singular and sacctmgr spellings
    'cpus=256,mem=768G,gpu=8',
    'cpu=256 gres/gpu=8 mem=768G',
    'CPU=256 GRES/GPU=8 MEMORY=768G',
    'Cpus=256 Gpus=8 Mem=768g',
])
def test_parse_tres_key_aliases(spec):
    assert parse_tres(spec) == {'cpus': 256, 'gpus': 8, 'mem': '768G'}


def test_parse_tres_mem_zero_without_unit():
    # issue #92
    assert parse_tres('cpus=0 gpus=0 mem=0') == {'cpus': 0, 'gpus': 0, 'mem': '0M'}


@pytest.mark.parametrize('spec, expected', [
    ('cpus=128 mem=1T', {'cpus': 128, 'gpus': 0, 'mem': '1T'}),
    ('mem=1T cpus=128', {'cpus': 128, 'gpus': 0, 'mem': '1T'}),
    ('cpus=0 mem=0', {'cpus': 0, 'gpus': 0, 'mem': '0M'}),
])
def test_parse_tres_gpus_defaults_to_zero(spec, expected):
    assert parse_tres(spec) == expected


@pytest.mark.parametrize('spec, expected', [
    ('cpus=-1 gpus=inf mem=unlimited', {'cpus': None, 'gpus': None, 'mem': None}),
    ('cpus=inf mem=1T', {'cpus': None, 'gpus': 0, 'mem': '1T'}),
    ('cpus=4 mem=-1', {'cpus': 4, 'gpus': 0, 'mem': None}),
    ('cpus=4 mem=16G gpus=Infinity', {'cpus': 4, 'gpus': None, 'mem': '16G'}),
])
def test_parse_tres_explicit_unlimited(spec, expected):
    assert parse_tres(spec) == expected


def test_parse_tres_normalizes_mem():
    assert parse_tres('cpus=1 mem=16gb')['mem'] == '16G'
    assert parse_tres('cpus=1 mem=2.9296875T')['mem'] == '2.9296875T'
    assert parse_tres('cpus=1 mem=.5G')['mem'] == '0.5G'


@pytest.mark.parametrize('spec', [
    'cpus=128 mem=1T gpus=8', 'cpus=1 mem=0', 'cpus=inf mem=inf gpus=inf',
])
def test_parse_tres_result_keys(spec):
    assert list(parse_tres(spec)) == ['cpus', 'gpus', 'mem']


@pytest.mark.parametrize('spec, needles', [
    ('', ['empty']),
    ('   ', ['empty']),
    (' , ', ['empty']),
    ('cpus', ["'cpus'", 'key=value']),
    ('cpus=', ["'cpus='", 'key=value']),
    ('=128', ["'=128'", 'key=value']),
    ('cores=128 mem=1T', ["'cores=128'", 'unknown']),
    ('cpus=128 cpu=64 mem=1T', ['duplicate', "'cpus=128'", "'cpu=64'"]),
    ('gpus=8 gres/gpu=4 cpus=1 mem=1T', ['duplicate', "'gres/gpu=4'"]),
    ('cpus=1 mem=16', ["mem='16'", 'unit is required']),
    ('cpus=1 mem=1K', ["mem='1K'", 'M, G, T, P']),
    ('cpus=abc mem=1T', ["cpus='abc'", 'whole number']),
    ('cpus=128=64 mem=1T', ["cpus='128=64'"]),
    ('gres/gpu:a100=8 cpus=1 mem=1T', ["'gres/gpu:a100=8'", 'unknown']),
    ('cpus=128;mem=1T', ["cpus='128;mem=1T'"]),
    ('cpus=-5 mem=1T', ["cpus='-5'"]),
    # an alias in the token: the message carries both spellings
    ('gpu=abc cpus=1 mem=1T', ["in 'gpu=abc'", "gpus='abc'"]),
    ('cpu=1 memory=16 gpu=1', ["in 'memory=16'", "mem='16'", 'unit is required']),
    # required keys
    ('gpus=8', ['missing', 'cpus', 'mem']),
    ('cpus=128', ['missing', 'mem']),
    ('mem=1T', ['missing', 'cpus']),
    ('mem=1T gpus=2', ['missing', 'cpus']),
])
def test_parse_tres_errors(spec, needles):
    with pytest.raises(TRESParseError) as excinfo:
        parse_tres(spec)
    message = str(excinfo.value)
    for needle in needles:
        assert needle in message, (needle, message)


def test_parse_tres_missing_message_does_not_name_present_keys():
    with pytest.raises(TRESParseError) as excinfo:
        parse_tres('cpus=128')
    assert 'missing required resource(s): mem' in str(excinfo.value)


# ---------------------------------------------------------------------------
# argparse adapters
# ---------------------------------------------------------------------------


def test_tres_argtype_success_matches_parse_tres():
    assert tres_argtype('gpu=8 cpus=1 mem=1T') == parse_tres('gpu=8 cpus=1 mem=1T')


def test_tres_argtype_raises_argument_type_error():
    with pytest.raises(ArgumentTypeError) as excinfo:
        tres_argtype('cores=1')
    assert "'cores=1'" in str(excinfo.value)


def test_tres_argtype_through_argparse(capsys):
    parser = argparse.ArgumentParser(prog='x')
    parser.add_argument('--tres', type=tres_argtype, default=None)

    assert parser.parse_args(['--tres', 'gpu=8 cpus=1 mem=1T']).tres['gpus'] == 8
    assert parser.parse_args([]).tres is None

    with pytest.raises(SystemExit) as excinfo:
        parser.parse_args(['--tres', 'cores=1'])
    assert excinfo.value.code == 2
    err = capsys.readouterr().err
    assert 'argument --tres:' in err
    assert "'cores=1'" in err

    with pytest.raises(SystemExit):
        parser.parse_args(['--tres', 'cpus=1 mem=768'])
    assert 'a unit is required' in capsys.readouterr().err


@pytest.mark.parametrize('value, expected', [
    ('8', '8'), (' 8 ', '8'), ('inf', 'inf'), ('-1', '-1'),
])
def test_count_argtype_returns_raw_string(value, expected):
    assert count_argtype('cpus')(value) == expected


def test_count_argtype_error_names_resource():
    with pytest.raises(ArgumentTypeError) as excinfo:
        count_argtype('gpus')('4.5')
    assert 'gpus' in str(excinfo.value)
    assert count_argtype('gpus').__name__ == 'gpus_argtype'


@pytest.mark.parametrize('value, expected', [
    ('16G', '16G'), (' 16gb ', '16gb'), ('0', '0'), ('inf', 'inf'),
])
def test_mem_argtype_returns_raw_string(value, expected):
    assert mem_argtype(value) == expected


def test_mem_argtype_error():
    with pytest.raises(ArgumentTypeError) as excinfo:
        mem_argtype('768')
    assert 'unit is required' in str(excinfo.value)
