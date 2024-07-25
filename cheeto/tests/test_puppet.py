from pathlib import Path

from .conftest import run_shell_cmd
from ..puppet import PuppetAccountMap, PuppetUserRecord
from ..errors import ExitCode


class TestUserRecord:

    def test_load(self, testdata):
        fn = testdata('testuser.yaml')
        record = PuppetAccountMap.load_yaml(fn)
        assert hasattr(record, 'user')
        assert 'testuser' in record.user
        user = record.user['testuser']
        assert user.fullname == 'Test Testerson'


class TestValidateMergeCommand:

    base_cmd = ['cheeto', 'puppet', 'validate', 
                '--merge', 'ALL', 
                '--strict', 
                '--dump']

    def test_basic(self, testdata , tmpdir):
        '''
        Test `cheeto puppet validate --merge ALL` with no postload validation.
        '''
        
        base_fn = testdata('testuser.yaml')
        site_fn = testdata('testuser.site.yaml')
        out_fn = tmpdir.join('merged.yaml')

        cmd = self.base_cmd + [out_fn, base_fn, site_fn]

        with tmpdir.as_cwd():
            retcode, stderr, p = run_shell_cmd(cmd)
            assert retcode == 0

            merged = PuppetAccountMap.load_yaml(out_fn)
            assert 'testuser' in merged.user
            
            user = merged.user['testuser']
            assert 'testgroup' in user.groups


    def test_override_value(self, testdata , tmpdir):
        '''
        Test `cheeto puppet validate --merge ALL` with an override in site.yaml.
        '''
        
        base_fn = testdata('testuser.yaml')
        site_fn = testdata('testuser.site.override-value.yaml')
        out_fn = tmpdir.join('merged.yaml')

        cmd = self.base_cmd + [out_fn, base_fn, site_fn]

        with tmpdir.as_cwd():
            retcode, stderr, p = run_shell_cmd(cmd)
            assert retcode == 0

            merged = PuppetAccountMap.load_yaml(out_fn)
            assert 'testuser' in merged.user
            
            user = merged.user['testuser']
            assert user.shell == '/bin/bash'


    def test_bad_override_value(self, testdata , tmpdir):
        '''
        Test `cheeto puppet validate --merge ALL` with an override in site.yaml.
        '''
        
        base_fn = testdata('testuser.yaml')
        site_fn = testdata('testuser.site.bad-override-value.yaml')
        out_fn = tmpdir.join('merged.yaml')

        cmd = self.base_cmd + [out_fn, base_fn, site_fn]

        with tmpdir.as_cwd():
            retcode, stderr, p = run_shell_cmd(cmd)
            assert retcode == ExitCode.VALIDATION_ERROR
            assert out_fn.exists() == False

    def test_postvalidate_fail(self, testdata , tmpdir):
        '''
        Test `cheeto puppet validate --merge ALL --postload-validate` fails due to missing group.
        '''
        
        base_fn = testdata('testuser.yaml')
        site_fn = testdata('testuser.site.yaml')
        out_fn = tmpdir.join('merged.yaml')

        cmd = self.base_cmd + [out_fn,
                               '--postload-validate',
                               base_fn, site_fn]

        with tmpdir.as_cwd():
            retcode, stderr, p = run_shell_cmd(cmd)
            assert retcode == ExitCode.VALIDATION_ERROR
            assert out_fn.exists() == False


    def test_postvalidate_success(self, testdata , tmpdir):
        '''
        Test `cheeto puppet validate --merge ALL --postload-validate` succeeds.
        '''
        
        base_fn = testdata('testuser.yaml')
        site_fn = testdata('testuser.site.yaml')
        group_fn = testdata('testgroup.yaml')
        out_fn = tmpdir.join('merged.yaml')

        cmd = self.base_cmd + [out_fn,
                               '--postload-validate',
                               base_fn, site_fn, group_fn]

        with tmpdir.as_cwd():
            retcode, stderr, p = run_shell_cmd(cmd)
            assert retcode == 0

            merged = PuppetAccountMap.load_yaml(out_fn)
            assert 'testuser' in merged.user

            assert hasattr(merged, 'group')
            assert 'testgroup' in merged.group
