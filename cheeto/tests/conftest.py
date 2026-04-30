from collections.abc import Sequence
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

from pymongo import MongoClient
import pytest

from ..config import get_config
from ..database import connect_mongoengine
from ..log import setup as _setup_logging
from ..types import is_listlike


MONGODB_PORT = 28080


# Default seed data for AccessGroup / StatusGroup records used by the async
# test fixtures. Mirrors operations.group.DEFAULT_*_GROUPS but kept inline
# here to avoid pulling beanie at conftest-import time (operations import
# beanie's Link, which requires init_beanie). The async beanie_client
# fixtures call seed_access_status_groups() after clean_db() so every test
# starts with these records present.
TEST_DEFAULT_ACCESS_GROUPS = (
    ('login-ssh', 'login-ssh-users'),
    ('ondemand', 'ondemand-users'),
    ('compute-ssh', 'compute-ssh-users'),
    ('root-ssh', 'root-ssh-users'),
    ('sudo', 'sudo-users'),
    ('slurm', 'slurm-users'),
)
TEST_DEFAULT_STATUS_GROUPS = (
    ('active', 'active-users'),
    ('inactive', 'inactive-users'),
    ('disabled', 'disabled-users'),
    ('offboarding', 'offboarding-users'),
)
TEST_SPECIAL_GROUP_GID_START = 6000


async def seed_access_status_groups():
    """Insert the standard AccessGroup / StatusGroup records used by tests.

    Async test fixtures call this after clean_db() so every test has the
    full set available for find_one(access_name=...) lookups in operations
    that require Links.
    """
    from cheeto.models.group import AccessGroup, StatusGroup

    gid = TEST_SPECIAL_GROUP_GID_START
    for access_name, ldap_name in TEST_DEFAULT_ACCESS_GROUPS:
        await AccessGroup(
            name=ldap_name, gid=gid, access_name=access_name, type='access',
        ).insert()
        gid += 1
    for status_name, ldap_name in TEST_DEFAULT_STATUS_GROUPS:
        await StatusGroup(
            name=ldap_name, gid=gid, status_name=status_name, type='status',
        ).insert()
        gid += 1


async def status_link(name: str):
    """Test helper: fetch the StatusGroup record by status_name."""
    from cheeto.models.group import StatusGroup
    return await StatusGroup.find_one(StatusGroup.status_name == name)


async def access_links(names: list[str]) -> list:
    """Test helper: fetch AccessGroup records for the given access_names."""
    from cheeto.models.group import AccessGroup
    out = []
    for n in names:
        ag = await AccessGroup.find_one(AccessGroup.access_name == n)
        if ag is None:
            raise RuntimeError(f'Test fixture missing AccessGroup {n!r}')
        out.append(ag)
    return out


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


@pytest.fixture
def run_cmd(config_file):
    def _run_cmd(*args):
        from logging import getLogger
        from ..cmds.__main__ import commands
        logger = getLogger(__name__)
        args = [str(arg) for arg in args]
        args.extend(['--config', str(config_file)])
        logger.info(f'running: {" ".join(args)}')
        try:
            retval = commands.run(args)
        except Exception as e:
            logger.error(f'command failed: {" ".join(args)}')
            raise e
        finally:
            commands.postprocessors_q = []
        return retval
    return _run_cmd


@pytest.fixture(scope='session', autouse=True)
def setup_logging():
    _setup_logging(sys.stdout)


# Timeouts for MongoDB replica set startup. Replica set election can take several
# seconds; 5ms was too short and caused intermittent ServerSelectionTimeoutError.
MONGODB_SERVER_SELECTION_TIMEOUT_MS = 30_000
MONGODB_INIT_RETRY_SLEEP_SEC = 0.5
MONGODB_INIT_MAX_WAIT_SEC = 30


@pytest.fixture(scope='session', autouse=True)
def start_mongodb(tmp_path_factory):
    '''
    Fixture that starts a MongoDB instance for the duration of the test session.
    '''
    db_dir = tmp_path_factory.mktemp('cheeto-mongodb')
    data_dir = db_dir / 'data'
    data_dir.mkdir()
    log_file = db_dir / 'mongod.log'

    proc = subprocess.Popen(
        [
            'mongod',
            '--dbpath', str(data_dir),
            '--port', str(MONGODB_PORT),
            '--logpath', str(log_file),
            '--replSet', 'mongoengine',
        ]
    )
    time.sleep(2)  # Give mongod time to bind and accept connections

    n_tries = 3
    retcode = -1
    err = ''
    for _ in range(n_tries):
        retcode, err, _ = run_shell_cmd(
            ['mongosh', '--port', str(MONGODB_PORT), '--eval', '"rs.initiate()"'],
            print_stderr=False,
        )
        if retcode == 0:
            break
        time.sleep(1)

    if retcode != 0:
        print('Failed to initiate MongoDB replica set')
        print('mongosh output:', err)
        print('mongod log:')
        with open(log_file) as f:
            print(f.read())
        proc.terminate()
        proc.wait()
        raise Exception('Failed to initiate MongoDB replica set')

    # Wait for replica set to elect primary. Election can take several seconds;
    # serverSelectionTimeoutMS must be long enough for discovery + election.
    client = MongoClient(
        port=MONGODB_PORT,
        serverSelectionTimeoutMS=MONGODB_SERVER_SELECTION_TIMEOUT_MS,
    )
    deadline = time.monotonic() + MONGODB_INIT_MAX_WAIT_SEC
    last_error = None
    while time.monotonic() < deadline:
        try:
            client.server_info()
            break
        except Exception as e:
            last_error = e
            time.sleep(MONGODB_INIT_RETRY_SLEEP_SEC)
    else:
        print(f'Failed to connect to MongoDB primary: {last_error}')
        print('mongod log:')
        with open(log_file) as f:
            print(f.read())
        proc.terminate()
        proc.wait()
        raise last_error

    yield
    proc.terminate()
    proc.wait()


@pytest.fixture(scope='session')
def config_file(tmp_path_factory):
    data_dir = Path(__file__).parent / 'data'
    config_file = tmp_path_factory.mktemp('cheeto-config') / 'config.yaml'
    shutil.copy(data_dir / 'config.yaml', config_file)
    return config_file


@pytest.fixture(scope='session')
def config(config_file):
    return get_config(config_file)


@pytest.fixture(scope='session')
def db_config(config):
    return config.mongo


@pytest.fixture(scope='session')
def hippo_config(config):
    return config.hippo


def drop_database(config):
    conn = connect_mongoengine(config, quiet=True)
    conn.drop_database(config.database)


@pytest.fixture
def drop_before(db_config):
    drop_database(db_config)


@pytest.fixture
def drop_after(db_config):
    yield
    drop_database(db_config)


@pytest.fixture
def drop_before_after(db_config):
    drop_database(db_config)
    yield
    drop_database(db_config)