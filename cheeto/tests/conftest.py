from collections.abc import Sequence
import os
from pathlib import Path
import shutil
import subprocess

import pytest

from cheeto.types import is_listlike


@pytest.fixture
def testdata(tmpdir, request):
    '''
    Fixture responsible for locating the test data directory and copying it
    into a temporary directory.
    '''
    data_dir = Path(request.module.__file__).parent / 'data'

    def getter(*filenames):
        filenames = list(filenames)
        copied = tuple((shutil.copy(data_dir / filename, tmpdir) for filename in filenames))
        if len(copied) == 1:
            return copied[0]
        else:
            return copied

    return getter


def using(**kwargs):

    def pretty(val):
        return str(val)

    def wrapped(fixture_func):
        for param, value in kwargs.items():
            if is_listlike(value):
                value = list(value)
                ids = ['{0}={1}'.format(param, pretty(v)) for v in value]
            else:
                ids = ['{0}={1}'.format(param, pretty(value))]
                value = [value]
            
            fixture_func = pytest.mark.parametrize(param,
                                                   value,
                                                   indirect=True,
                                                   ids=ids)(fixture_func)

        return fixture_func

    return wrapped


def run_shell_cmd(cmd, print_stderr=True, in_directory=None):
    cwd = os.getcwd()
    if in_directory:
        os.chdir(in_directory)
    
    cmd = ' '.join((str(c) for c in cmd))
    print('running: ', cmd)
    try:
        p = subprocess.run(cmd, shell=True, check=False,
                           capture_output=True, encoding='utf-8') 
        if print_stderr:
            print('stderr:', p.stderr)
        return p.returncode, p.stderr, p
    finally:
        os.chdir(cwd)
