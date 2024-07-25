from pathlib import Path

from .conftest import run_shell_cmd
from ..puppet import PuppetAccountMap
from ..errors import ExitCode

class TestValidateCommand:

    base_cmd = ['cheeto', 'puppet', 'validate']

    def test_validate_basic(self, testdata , tmpdir):
        '''
        Test `cheeto puppet validate --merge ALL` with no postload validation.
        '''
        
        base_fn = testdata('testuser.yaml')
        site_fn = testdata('testuser.site.yaml')
        out_fn = tmpdir.join('merged.yaml')

        cmd = self.base_cmd + ['--merge', 'ALL', 
                               '--strict', 
                               '--dump', out_fn,
                               base_fn, site_fn]

        with tmpdir.as_cwd():
            retcode, stderr, p = run_shell_cmd(cmd)
            assert retcode == 0

            merged = PuppetAccountMap.load_yaml(out_fn)
            assert 'testuser' in merged.user
            
            user = merged.user['testuser']
            assert 'testgroup' in user.groups

    def test_validate_basic_postfail(self, testdata , tmpdir):
        '''
        Test `cheeto puppet validate --merge ALL --postload-validate` fails due to missing group.
        '''
        
        base_fn = testdata('testuser.yaml')
        site_fn = testdata('testuser.site.yaml')
        out_fn = tmpdir.join('merged.yaml')

        cmd = self.base_cmd + ['--merge', 'ALL', 
                               '--strict', 
                               '--postload-validate', 
                               '--dump', out_fn, 
                               base_fn, site_fn]

        with tmpdir.as_cwd():
            retcode, stderr, p = run_shell_cmd(cmd)
            assert retcode == ExitCode.VALIDATION_ERROR
            assert out_fn.exists() == False


    def test_validate_basic_post(self, testdata , tmpdir):
        '''
        Test `cheeto puppet validate --merge ALL --postload-validate` succeeds.
        '''
        
        base_fn = testdata('testuser.yaml')
        site_fn = testdata('testuser.site.yaml')
        group_fn = testdata('testgroup.yaml')
        out_fn = tmpdir.join('merged.yaml')

        cmd = self.base_cmd + ['--merge', 'ALL', 
                               '--strict', 
                               '--postload-validate', 
                               '--dump', out_fn, 
                               base_fn, site_fn, group_fn]

        with tmpdir.as_cwd():
            retcode, stderr, p = run_shell_cmd(cmd)
            assert retcode == 0

            merged = PuppetAccountMap.load_yaml(out_fn)
            assert 'testuser' in merged.user

            assert hasattr(merged, 'group')
            assert 'testgroup' in merged.group
