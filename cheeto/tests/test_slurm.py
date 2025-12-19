import pytest
from ..types import parse_qos_tres
from ..puppet import SlurmQOS, SlurmQOSTRES


def test_parse_qos_tres():
    assert parse_qos_tres('mem=1000M,cpus=16,gpus=0') == {'mem': '1000M', 'cpus': '16', 'gpus': '0'}
    assert parse_qos_tres('cpus=16,mem=1000M,gpus=0') == {'mem': '1000M', 'cpus': '16', 'gpus': '0'}
    assert parse_qos_tres('cpus=16,gpus=0,mem=1000M') == {'mem': '1000M', 'cpus': '16', 'gpus': '0'}

def test_parse_qos_tres_partial():
    assert parse_qos_tres('mem=1000M') == {'mem': '1000M', 'cpus': None, 'gpus': None}
    assert parse_qos_tres('cpus=16') == {'mem': None, 'cpus': '16', 'gpus': None}
    assert parse_qos_tres('gpus=2') == {'mem': None, 'cpus': None, 'gpus': '2'}

def test_parse_qos_tres_none():
    assert parse_qos_tres(None) == {'mem': None, 'cpus': None, 'gpus': None}


def test_parse_qos_tres_empty():
    assert parse_qos_tres('') == {'mem': None, 'cpus': None, 'gpus': None}


class TestSlurmQOS:
    """Test cases for SlurmQOS class, specifically the to_slurm method behavior."""

    def test_to_slurm_basic(self):
        """Test basic to_slurm functionality with all fields populated."""
        qos = SlurmQOS(
            group=SlurmQOSTRES(cpus=16, mem='1000M', gpus=2),
            user=SlurmQOSTRES(cpus=8, mem='500M', gpus=1),
            job=SlurmQOSTRES(cpus=4, mem='250M', gpus=0),
            priority=10,
            flags=['DenyOnLimit', 'NoReserve']
        )
        
        result = qos.to_slurm()
        expected = [
            'GrpTres=cpu=16,mem=1000,gres/gpu=2',
            'MaxTRESPerUser=cpu=8,mem=500,gres/gpu=1',
            'MaxTresPerJob=cpu=4,mem=250,gres/gpu=0',
            'Flags=DenyOnLimit,NoReserve',
            'Priority=10'
        ]
        assert result == expected

    def test_to_slurm_modify_false_flags_none(self):
        """Test that Flags is not included when modify=False and flags=None."""
        qos = SlurmQOS(
            group=SlurmQOSTRES(cpus=16, mem='1000M', gpus=2),
            user=SlurmQOSTRES(cpus=8, mem='500M', gpus=1),
            job=SlurmQOSTRES(cpus=4, mem='250M', gpus=0),
            priority=10,
            flags=None
        )
        
        result = qos.to_slurm(modify=False)
        expected = [
            'GrpTres=cpu=16,mem=1000,gres/gpu=2',
            'MaxTRESPerUser=cpu=8,mem=500,gres/gpu=1',
            'MaxTresPerJob=cpu=4,mem=250,gres/gpu=0',
            'Priority=10'
        ]
        assert result == expected
        # Ensure 'Flags=' is not in the result
        assert not any('Flags=' in token for token in result)

    def test_to_slurm_modify_true_flags_none(self):
        """Test that Flags=-1 is included when modify=True and flags=None."""
        qos = SlurmQOS(
            group=SlurmQOSTRES(cpus=16, mem='1000M', gpus=2),
            user=SlurmQOSTRES(cpus=8, mem='500M', gpus=1),
            job=SlurmQOSTRES(cpus=4, mem='250M', gpus=0),
            priority=10,
            flags=None
        )
        
        result = qos.to_slurm(modify=True)
        expected = [
            'GrpTres=cpu=16,mem=1000,gres/gpu=2',
            'MaxTRESPerUser=cpu=8,mem=500,gres/gpu=1',
            'MaxTresPerJob=cpu=4,mem=250,gres/gpu=0',
            'Flags=-1',
            'Priority=10'
        ]
        assert result == expected
        # Ensure 'Flags=-1' is in the result
        assert any('Flags=-1' in token for token in result)

    def test_to_slurm_modify_false_flags_empty_list(self):
        """Test that Flags is not included when modify=False and flags is empty list."""
        qos = SlurmQOS(
            group=SlurmQOSTRES(cpus=16, mem='1000M', gpus=2),
            user=SlurmQOSTRES(cpus=8, mem='500M', gpus=1),
            job=SlurmQOSTRES(cpus=4, mem='250M', gpus=0),
            priority=10,
            flags=[]
        )
        
        result = qos.to_slurm(modify=False)
        expected = [
            'GrpTres=cpu=16,mem=1000,gres/gpu=2',
            'MaxTRESPerUser=cpu=8,mem=500,gres/gpu=1',
            'MaxTresPerJob=cpu=4,mem=250,gres/gpu=0',
            'Priority=10'
        ]
        assert result == expected
        # Ensure 'Flags=' is not in the result
        assert not any('Flags=' in token for token in result)

    def test_to_slurm_modify_false_flags_populated(self):
        """Test that Flags is included when modify=False and flags has values."""
        qos = SlurmQOS(
            group=SlurmQOSTRES(cpus=16, mem='1000M', gpus=2),
            user=SlurmQOSTRES(cpus=8, mem='500M', gpus=1),
            job=SlurmQOSTRES(cpus=4, mem='250M', gpus=0),
            priority=10,
            flags=['DenyOnLimit']
        )
        
        result = qos.to_slurm(modify=False)
        expected = [
            'GrpTres=cpu=16,mem=1000,gres/gpu=2',
            'MaxTRESPerUser=cpu=8,mem=500,gres/gpu=1',
            'MaxTresPerJob=cpu=4,mem=250,gres/gpu=0',
            'Flags=DenyOnLimit',
            'Priority=10'
        ]
        assert result == expected
        # Ensure 'Flags=DenyOnLimit' is in the result
        assert any('Flags=DenyOnLimit' in token for token in result)

    def test_to_slurm_modify_true_flags_populated(self):
        """Test that Flags is included when modify=True and flags has values."""
        qos = SlurmQOS(
            group=SlurmQOSTRES(cpus=16, mem='1000M', gpus=2),
            user=SlurmQOSTRES(cpus=8, mem='500M', gpus=1),
            job=SlurmQOSTRES(cpus=4, mem='250M', gpus=0),
            priority=10,
            flags=['DenyOnLimit', 'NoReserve']
        )
        
        result = qos.to_slurm(modify=True)
        expected = [
            'GrpTres=cpu=16,mem=1000,gres/gpu=2',
            'MaxTRESPerUser=cpu=8,mem=500,gres/gpu=1',
            'MaxTresPerJob=cpu=4,mem=250,gres/gpu=0',
            'Flags=DenyOnLimit,NoReserve',
            'Priority=10'
        ]
        assert result == expected
        # Ensure 'Flags=DenyOnLimit,NoReserve' is in the result
        assert any('Flags=DenyOnLimit,NoReserve' in token for token in result)

    def test_to_slurm_minimal_fields(self):
        """Test to_slurm with minimal fields (all None except priority)."""
        qos = SlurmQOS(
            group=None,
            user=None,
            job=None,
            priority=0,
            flags=None
        )
        
        result = qos.to_slurm(modify=False)
        expected = [
            'GrpTres=cpu=-1,mem=-1,gres/gpu=-1',
            'MaxTRESPerUser=cpu=-1,mem=-1,gres/gpu=-1',
            'MaxTresPerJob=cpu=-1,mem=-1,gres/gpu=-1',
            'Priority=0'
        ]
        assert result == expected
        # Ensure 'Flags=' is not in the result
        assert not any('Flags=' in token for token in result)

    def test_to_slurm_minimal_fields_modify_true(self):
        """Test to_slurm with minimal fields and modify=True."""
        qos = SlurmQOS(
            group=None,
            user=None,
            job=None,
            priority=0,
            flags=None
        )
        
        result = qos.to_slurm(modify=True)
        expected = [
            'GrpTres=cpu=-1,mem=-1,gres/gpu=-1',
            'MaxTRESPerUser=cpu=-1,mem=-1,gres/gpu=-1',
            'MaxTresPerJob=cpu=-1,mem=-1,gres/gpu=-1',
            'Flags=-1',
            'Priority=0'
        ]
        assert result == expected
        # Ensure 'Flags=-1' is in the result
        assert any('Flags=-1' in token for token in result)

    def test_to_slurm_partial_tres(self):
        """Test to_slurm with some TRES fields None."""
        qos = SlurmQOS(
            group=SlurmQOSTRES(cpus=16, mem=None, gpus=2),
            user=SlurmQOSTRES(cpus=None, mem='500M', gpus=None),
            job=SlurmQOSTRES(cpus=4, mem=None, gpus=0),
            priority=5,
            flags=None
        )
        
        result = qos.to_slurm(modify=False)
        expected = [
            'GrpTres=cpu=16,mem=-1,gres/gpu=2',
            'MaxTRESPerUser=cpu=-1,mem=500,gres/gpu=-1',
            'MaxTresPerJob=cpu=4,mem=-1,gres/gpu=0',
            'Priority=5'
        ]
        assert result == expected
        # Ensure 'Flags=' is not in the result
        assert not any('Flags=' in token for token in result)

    def test_to_slurm_multiple_flags(self):
        """Test to_slurm with multiple flags."""
        qos = SlurmQOS(
            group=SlurmQOSTRES(cpus=16, mem='1000M', gpus=2),
            user=SlurmQOSTRES(cpus=8, mem='500M', gpus=1),
            job=SlurmQOSTRES(cpus=4, mem='250M', gpus=0),
            priority=10,
            flags=['DenyOnLimit', 'NoReserve', 'EnforceUsageThreshold']
        )
        
        result = qos.to_slurm(modify=False)
        expected = [
            'GrpTres=cpu=16,mem=1000,gres/gpu=2',
            'MaxTRESPerUser=cpu=8,mem=500,gres/gpu=1',
            'MaxTresPerJob=cpu=4,mem=250,gres/gpu=0',
            'Flags=DenyOnLimit,NoReserve,EnforceUsageThreshold',
            'Priority=10'
        ]
        assert result == expected
        # Ensure all flags are present
        assert any('Flags=DenyOnLimit,NoReserve,EnforceUsageThreshold' in token for token in result)
