from collections.abc import Sequence
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import re

from pymongo import MongoClient
import pytest

from ..config import get_config
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


def pytest_collection_modifyitems(config, items):
    """Skip the v1->v2 migration tests when the optional `legacy` extra
    (mongoengine) isn't installed. All such tests live in TestMigrate* classes,
    so their node ids contain 'Migrate'."""
    try:
        import mongoengine  # noqa: F401
        return
    except ImportError:
        skip = pytest.mark.skip(
            reason="needs the optional 'legacy' extra (mongoengine)"
        )
        for item in items:
            if 'Migrate' in item.nodeid:
                item.add_marker(skip)


# ---------------------------------------------------------------------------
# slapd: ephemeral OpenLDAP for async LDAP integration tests
# ---------------------------------------------------------------------------

# Port chosen to avoid collision with system slapd (389) and MongoDB (28080).
SLAPD_PORT = 28389
SLAPD_BASE_DN = 'dc=hpc,dc=test'
SLAPD_ADMIN_DN = 'cn=admin,dc=hpc,dc=test'
SLAPD_ADMIN_PASSWORD = 'test-admin-password'


# Minimal LDAP Public Key (LPK) schema — defines `sshPublicKey` attribute and
# `ldapPublicKey` auxiliary objectClass so slapd accepts user entries that
# carry SSH keys. Inlined here because Ubuntu's slapd package doesn't ship it.
_LPK_SCHEMA = '''\
attributetype ( 1.3.6.1.4.1.24552.500.1.1.1.13
    NAME 'sshPublicKey'
    DESC 'MANDATORY: OpenSSH Public key'
    EQUALITY octetStringMatch
    SYNTAX 1.3.6.1.4.1.1466.115.121.1.40 )

objectclass ( 1.3.6.1.4.1.24552.500.1.1.2.0
    NAME 'ldapPublicKey'
    DESC 'MANDATORY: OpenSSH LPK objectclass'
    SUP top AUXILIARY
    MAY ( sshPublicKey $ uid ) )
'''


def _slapd_config(data_dir, schema_dir, lpk_schema_path) -> str:
    """Build a slapd.conf string. Uses the older config-file style (vs.
    cn=config) so the fixture is straightforward to write inline."""
    # Hash the admin password — slapd accepts the {SSHA} or {PLAIN} form.
    # Passing it plain and prefixing with {CLEARTEXT} works on Ubuntu's slapd.
    return f'''\
include {schema_dir}/core.schema
include {schema_dir}/cosine.schema
include {schema_dir}/inetorgperson.schema
include {schema_dir}/nis.schema
include {lpk_schema_path}

# Automount schema (defines automount, automountMap objectClasses) is part
# of OpenLDAP's nis.schema — already included.

# Ubuntu's slapd ships backends as loadable modules.
modulepath /usr/lib/ldap
moduleload back_mdb.so

pidfile  {data_dir}/slapd.pid
argsfile {data_dir}/slapd.args

database   mdb
maxsize    1073741824
suffix     "{SLAPD_BASE_DN}"
rootdn     "{SLAPD_ADMIN_DN}"
rootpw     {{CLEARTEXT}}{SLAPD_ADMIN_PASSWORD}
directory  {data_dir}

# Permissive ACL — this is a test instance.
access to *
    by * write
    by * read
'''


def _seed_ldif(base_dn: str) -> str:
    """Bootstrap entries needed to host users/groups: the base dc, the
    cn=admin entry, ou=users."""
    parts = base_dn.split(',')
    # Build the dcObject for the base dn (dc=hpc,dc=test → dc=hpc)
    first_dc = parts[0].split('=', 1)[1]
    return f'''\
dn: {base_dn}
objectClass: dcObject
objectClass: organization
o: HPC Test
dc: {first_dc}

dn: ou=users,{base_dn}
objectClass: organizationalUnit
ou: users
'''


def _convert_schema_ldif_to_legacy(ldif_path, out_path):
    """Convert an Ubuntu slapd cn=config-style schema LDIF into the legacy
    schema include format that the older config-file style accepts.

    Ubuntu ships `core.ldif`, `cosine.ldif`, etc. instead of the .schema
    files. The conversion:
      1) Unfold LDIF line-continuations (lines starting with a single space).
      2) Drop the LDIF header (dn:/cn:/objectClass:).
      3) Rewrite `olcAttributeTypes: ...` -> `attributetype ...`,
         `olcObjectClasses: ...` -> `objectclass ...`.
      4) Strip the `{N}` index from olc entries (e.g. `{0}( 2.5.4.2 ...)`).
    """
    import re as _re
    with open(ldif_path) as f:
        content = f.read()

    # Unfold: LDIF treats a line starting with ' ' as a continuation of
    # the prior line. Join them into single logical lines.
    unfolded: list[str] = []
    for line in content.splitlines():
        if line.startswith(' ') and unfolded:
            unfolded[-1] += line[1:]
        else:
            unfolded.append(line)

    out_lines = []
    for line in unfolded:
        if line.startswith('dn:') or line.startswith('cn:'):
            continue
        if line.startswith('objectClass:'):
            continue
        # Strip "olcAttributeTypes: " / "olcObjectClasses: " prefix and
        # the `{N}` index that olcAttributeTypes uses for ordering.
        line = _re.sub(
            r'^olcAttributeTypes:\s*(\{\d+\})?', 'attributetype ', line,
        )
        line = _re.sub(
            r'^olcObjectClasses:\s*(\{\d+\})?', 'objectclass ', line,
        )
        out_lines.append(line)
    with open(out_path, 'w') as f:
        f.write('\n'.join(out_lines))


@pytest.fixture(scope='session')
def start_slapd():
    '''Start an ephemeral slapd for the duration of the test session.

    Skips with a clear reason if `slapd` isn't available. Schemas: core +
    cosine + inetorgperson + nis (for autofs/posixGroup) + an inlined LPK
    schema for sshPublicKey support.

    Uses /var/tmp instead of /tmp because Ubuntu's slapd apparmor profile
    only allows /etc/ldap, /var/lib/ldap, and /var/tmp.
    '''
    if shutil.which('slapd') is None:
        pytest.skip('slapd not available; install with apt install slapd')

    import tempfile
    base_dir = Path(tempfile.mkdtemp(prefix='cheeto-slapd-', dir='/var/tmp'))
    # Loosen base perms — mkdtemp defaults to 0700 which prevents the
    # slapd child (or apparmor-confined process) from traversing.
    os.chmod(base_dir, 0o755)
    data_dir = base_dir / 'data'
    data_dir.mkdir(mode=0o755)
    schema_dir = base_dir / 'schema'
    schema_dir.mkdir(mode=0o755)

    # Convert Ubuntu's cn=config-style schema LDIFs to the legacy include
    # format slapd's older slapd.conf accepts. openldap.schema is skipped
    # because it carries `olcObjectIdentifier:` directives that don't belong
    # in a global config-file schema include (and we don't need them).
    src_schema_dir = Path('/etc/ldap/schema')
    for name in ('core', 'cosine', 'inetorgperson', 'nis'):
        src = src_schema_dir / f'{name}.ldif'
        if not src.exists():
            pytest.skip(f'missing slapd schema {src}; install slapd properly')
        _convert_schema_ldif_to_legacy(src, schema_dir / f'{name}.schema')

    lpk_schema_path = schema_dir / 'lpk.schema'
    lpk_schema_path.write_text(_LPK_SCHEMA)

    conf_path = base_dir / 'slapd.conf'
    conf_path.write_text(_slapd_config(data_dir, schema_dir, lpk_schema_path))

    log_path = base_dir / 'slapd.log'

    # Start slapd in the foreground (-d 0) so we can kill it cleanly.
    # Run slapd as the current user (default is openldap, which can't read
    # files we created under /var/tmp/cheeto-slapd-*).
    proc = subprocess.Popen(
        [
            '/usr/sbin/slapd',
            '-h', f'ldap://127.0.0.1:{SLAPD_PORT}/',
            '-f', str(conf_path),
            '-d', '256',  # config processing only — quiet but reports errors
            '-u', str(os.getuid()),
            '-g', str(os.getgid()),
        ],
        stderr=open(log_path, 'wb'),
        stdout=subprocess.DEVNULL,
    )

    # Wait for slapd to bind by polling with ldapsearch
    deadline = time.monotonic() + 15
    ready = False
    while time.monotonic() < deadline:
        rc, _, _ = run_shell_cmd(
            [
                'ldapsearch', '-x', '-LLL',
                '-H', f'ldap://127.0.0.1:{SLAPD_PORT}/',
                '-D', SLAPD_ADMIN_DN, '-w', SLAPD_ADMIN_PASSWORD,
                '-b', '""', '-s', 'base', "'(objectClass=*)'",
            ],
            print_stderr=False,
        )
        if rc == 0:
            ready = True
            break
        time.sleep(0.3)

    if not ready:
        proc.terminate()
        proc.wait()
        log_text = log_path.read_text() if log_path.exists() else '(no log)'
        # Common dev-box failure: Ubuntu's apparmor profile for slapd
        # denies file_lock on /var/tmp/**, which LMDB needs. Skip rather
        # than fail when we recognize that signature so the rest of the
        # suite still runs on locked-down hosts.
        if 'Permission denied' in log_text and (
            'lock.mdb' in log_text or 'mdb_db_open' in log_text
        ):
            shutil.rmtree(base_dir, ignore_errors=True)
            pytest.skip(
                'slapd cannot open LMDB under /var/tmp; likely apparmor '
                'on the host. Run integration tests in a container or '
                'disable the slapd apparmor profile.'
            )
        raise RuntimeError(f'slapd did not start in time. Log:\n{log_text}')

    # Seed the base DN and ou=users.
    seed_path = base_dir / 'seed.ldif'
    seed_path.write_text(_seed_ldif(SLAPD_BASE_DN))
    rc, err, _ = run_shell_cmd(
        [
            'ldapadd', '-x',
            '-H', f'ldap://127.0.0.1:{SLAPD_PORT}/',
            '-D', SLAPD_ADMIN_DN, '-w', SLAPD_ADMIN_PASSWORD,
            '-f', str(seed_path),
        ],
        print_stderr=False,
    )
    if rc != 0:
        proc.terminate()
        proc.wait()
        raise RuntimeError(f'ldapadd seed failed: {err}')

    yield {
        'port': SLAPD_PORT,
        'base_dn': SLAPD_BASE_DN,
        'admin_dn': SLAPD_ADMIN_DN,
        'admin_password': SLAPD_ADMIN_PASSWORD,
    }
    proc.terminate()
    proc.wait()
    shutil.rmtree(base_dir, ignore_errors=True)


@pytest.fixture
def slapd_ldap_config(start_slapd):
    """An LDAPConfig pointing at the ephemeral slapd. Matches the production
    config schema from cheeto-dev-config.yaml but with localhost:SLAPD_PORT."""
    from ..config import LDAPConfig
    info = start_slapd
    return LDAPConfig(
        servers=[f'ldap://127.0.0.1:{info["port"]}/'],
        searchbase=info['base_dn'],
        login_dn=info['admin_dn'],
        password=info['admin_password'],
        user_base=f'ou=users,{info["base_dn"]}',
        request_timeout_seconds=5.0,
        pool_max_connections=3,
        pool_idle_connections=1,
    )