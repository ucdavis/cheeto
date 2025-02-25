import pytest
from ..types import parse_qos_tres


def test_parse_qos_tres():
    assert parse_qos_tres('mem=1000M,cpus=16,gpus=0') == {'mem': '1000M', 'cpus': '16', 'gpus': '0'}


def test_parse_qos_tres_none():
    assert parse_qos_tres(None) == {'mem': None, 'cpus': None, 'gpus': None}


def test_parse_qos_tres_empty():
    with pytest.raises(ValueError):
        print(parse_qos_tres(''))
