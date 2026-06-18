"""Tests for the cheeto daemon: config blocks, celery app and beat-schedule
construction, task body coroutines, and the FastAPI service.

Requires the session-scoped start_mongodb fixture from conftest.py. No
RabbitMQ is needed: celery wiring is exercised with task_always_eager, and
the task bodies are tested as plain coroutines against the test mongo.
"""

from dataclasses import replace
from datetime import datetime
from pathlib import Path

import httpx
import pytest
import pytest_asyncio
from beanie import init_beanie
from celery.schedules import crontab
from pymongo import AsyncMongoClient

from ..config import MongoConfig, get_config
from ..daemon.api import create_api
from ..daemon.app import (
    app,
    configure_celery_app,
    daemon_config,
    mongo_result_backend,
)
from ..daemon.schedule import (
    build_beat_schedule,
    build_enqueue_entries,
    parse_schedule,
)
from ..daemon.tasks import _puppet_sync, _reap, _sympa_export
from ..models import ALL_MODELS
from ..models.history import History
from ..models.site import Site
from ..models.user import SshKey, User
from ..models.user_site_info import UserSiteInfo
from ..operations.iam import ReapOffboardedUsers
from ..operations.site import ExportRootSSHKeys, ExportSympaEmails
from ..yaml import dumps as dumps_yaml, parse_yaml
from .conftest import (
    MONGODB_PORT,
    access_links,
    seed_access_status_groups,
    status_link,
)

BEANIE_TEST_DB = 'cheeto_daemon_test'


@pytest_asyncio.fixture(scope='session', loop_scope='session')
async def beanie_client(start_mongodb):
    client = AsyncMongoClient(f'127.0.0.1:{MONGODB_PORT}')
    await init_beanie(
        database=client[BEANIE_TEST_DB],
        document_models=ALL_MODELS,
    )
    yield client
    await client.close()


# NOT autouse (unlike test_beanie.py): this module mixes async DB tests with
# sync config/schedule tests that must not require the event-loop fixtures.
@pytest_asyncio.fixture(loop_scope='session')
async def clean_db(beanie_client):
    # Re-init first: the eager-mode celery test runs run_op, whose
    # connect_beanie rebinds beanie's global document state to a throwaway
    # client on its own asyncio.run loop. Rebinding back to the session
    # client here keeps the DB tests order-independent.
    await init_beanie(
        database=beanie_client[BEANIE_TEST_DB],
        document_models=ALL_MODELS,
    )
    for model in ALL_MODELS:
        await model.find_all().delete()
    await History.find_all().delete()
    await seed_access_status_groups()


# ---------------------------------------------------------------------------
# Config parsing
# ---------------------------------------------------------------------------


class TestDaemonConfig:

    def test_daemon_section_parses(self, config):
        d = config.daemon
        assert d is not None
        assert d.broker_url.startswith('amqp://')
        assert d.sites == ['test-site']
        assert d.author == 'cheeto-daemon'

    def test_schedule_types(self, config):
        tasks = config.daemon.tasks
        assert tasks.hippo.schedule == 300          # int stays int
        assert tasks.iam_sync.schedule == '0 2 * * *'  # crontab stays str
        assert tasks.iam_sync.concurrency == 2
        assert tasks.slurm_sync.max_deletions == 10

    def test_absent_tasks_disabled(self, config):
        assert config.daemon.tasks.ldap_sync is None
        assert config.daemon.tasks.reap is None

    def test_iam_sync_notify_defaults_true(self, config):
        # Not set in the test config, so this comes from the dataclass default.
        assert config.daemon.tasks.iam_sync.notify is True

    def test_reap_task_notify_default_true(self):
        # reap is absent from the test config; verify the dataclass default
        # that the daemon body relies on.
        from ..config import ReapTaskConfig
        assert ReapTaskConfig().notify is True

    def test_puppet_sync_config_parses(self, config):
        tcfg = config.daemon.tasks.puppet_sync
        assert tcfg.repo == '/tmp/cheeto-test-puppet'
        assert tcfg.schedule == 1800
        assert tcfg.base_branch == 'main'
        assert tcfg.push is True
        assert tcfg.write_keys is True
        assert tcfg.delete_branch is True

    def test_api_section_parses(self, config):
        assert config.api is not None
        assert config.api.port == 8810
        assert config.api.api_key == 'test-api-key'

    def test_config_without_daemon_sections_parses(self, config_file, tmp_path):
        data = parse_yaml(str(config_file))
        del data['daemon']
        del data['api']
        stripped = tmp_path / 'config-stripped.yaml'
        stripped.write_text(dumps_yaml(data))

        cfg = get_config(config_path=stripped)
        assert cfg is not None
        assert cfg.daemon is None
        assert cfg.api is None

    def test_profile_fallback_to_default(self, config_file):
        cfg = get_config(config_path=config_file, profile='nonexistent')
        assert cfg is not None
        assert cfg.daemon is not None
        assert cfg.daemon.sites == ['test-site']
        assert cfg.api is not None


# ---------------------------------------------------------------------------
# Schedule building
# ---------------------------------------------------------------------------


class TestParseSchedule:

    def test_interval(self):
        assert parse_schedule(300) == 300.0
        assert isinstance(parse_schedule(300), float)

    def test_crontab(self):
        sched = parse_schedule('0 2 * * *')
        assert sched == crontab(minute='0', hour='2')

    def test_bad_crontab_raises(self):
        with pytest.raises(ValueError, match='need 5 fields'):
            parse_schedule('0 2 * *')


class TestBuildBeatSchedule:

    def test_entries(self, config):
        sched = build_beat_schedule(config)
        assert set(sched) == {
            'hippo-process',
            'iam-sync',
            'slurm-sync-test-site',
            'sympa-export-test-site',
            'puppet-sync-test-site',
        }
        # disabled tasks (no config entry) produce no schedule entries
        assert not any(name.startswith('ldap-sync') for name in sched)
        assert 'reap-offboarded' not in sched

    def test_slurm_routed_to_site_queue(self, config):
        sched = build_beat_schedule(config)
        entry = sched['slurm-sync-test-site']
        assert entry['task'] == 'cheeto.slurm_sync'
        assert entry['args'] == ['test-site']
        assert entry['options']['queue'] == 'slurm.test-site'
        # everything else lands on the default queue
        for name in ('hippo-process', 'iam-sync', 'sympa-export-test-site',
                     'puppet-sync-test-site'):
            assert 'queue' not in sched[name]['options']

    def test_interval_entries_expire(self, config):
        sched = build_beat_schedule(config)
        assert sched['hippo-process']['options']['expires'] == 300.0
        assert sched['sympa-export-test-site']['options']['expires'] == 3600.0
        assert sched['puppet-sync-test-site']['options']['expires'] == 1800.0
        # crontab entries must not be silently dropped
        assert 'expires' not in sched['iam-sync']['options']

    def test_per_task_sites_override(self, config):
        tasks = replace(
            config.daemon.tasks,
            sympa=replace(config.daemon.tasks.sympa, sites=['a', 'b']),
        )
        cfg = replace(config, daemon=replace(config.daemon, tasks=tasks))
        sched = build_beat_schedule(cfg)
        assert 'sympa-export-a' in sched
        assert 'sympa-export-b' in sched
        assert 'sympa-export-test-site' not in sched


class TestBuildEnqueueEntries:

    def test_singleton_routes_to_default_queue(self, config):
        entries = build_enqueue_entries(config, 'reap')
        assert entries == [{
            'task': 'cheeto.reap', 'args': [],
            'options': {'queue': 'cheeto'},
        }]

    def test_slurm_routes_to_site_queue(self, config):
        entries = build_enqueue_entries(config, 'slurm_sync')
        assert entries == [{
            'task': 'cheeto.slurm_sync', 'args': ['test-site'],
            'options': {'queue': 'slurm.test-site'},
        }]

    def test_explicit_sites_fan_out(self, config):
        entries = build_enqueue_entries(
            config, 'sympa_export', sites=['a', 'b'],
        )
        assert [e['args'] for e in entries] == [['a'], ['b']]
        assert all(e['options']['queue'] == 'cheeto' for e in entries)

    def test_per_site_default_uses_task_sites_override(self, config):
        tasks = replace(
            config.daemon.tasks,
            sympa=replace(config.daemon.tasks.sympa, sites=['x']),
        )
        cfg = replace(config, daemon=replace(config.daemon, tasks=tasks))
        entries = build_enqueue_entries(cfg, 'sympa_export')
        assert [e['args'] for e in entries] == [['x']]

    def test_unconfigured_per_site_task_uses_daemon_sites(self, config):
        # ldap_sync has no daemon.tasks entry in the test config; an
        # explicit enqueue still resolves sites from the daemon defaults.
        entries = build_enqueue_entries(config, 'ldap_sync')
        assert [e['args'] for e in entries] == [['test-site']]

    def test_no_expiry_on_one_off_submissions(self, config):
        for task in ('reap', 'slurm_sync', 'sympa_export'):
            for entry in build_enqueue_entries(config, task):
                assert 'expires' not in entry['options']

    def test_singleton_rejects_site(self, config):
        with pytest.raises(ValueError, match='does not take a site'):
            build_enqueue_entries(config, 'iam_sync', sites=['a'])

    def test_unknown_task_rejected(self, config):
        with pytest.raises(ValueError, match='Unknown task'):
            build_enqueue_entries(config, 'frobnicate')

    def test_puppet_sync_routes_to_default_queue(self, config):
        entries = build_enqueue_entries(config, 'puppet_sync')
        assert entries == [{
            'task': 'cheeto.puppet_sync', 'args': ['test-site'],
            'options': {'queue': 'cheeto'},
        }]

    def test_all_enqueueable_tasks_registered(self, config):
        from ..daemon.schedule import TASK_SPECS
        for task in TASK_SPECS:
            sites = ['s'] if TASK_SPECS[task][1] else None
            for entry in build_enqueue_entries(config, task, sites=sites):
                assert entry['task'] in app.tasks


# ---------------------------------------------------------------------------
# Celery app construction
# ---------------------------------------------------------------------------


class TestCeleryApp:

    def test_mongo_result_backend_no_auth(self, config):
        url, settings = mongo_result_backend(config.mongo)
        assert url == f'mongodb://127.0.0.1:{MONGODB_PORT}/'
        assert settings == {'database': 'hpccf',
                            'taskmeta_collection': 'celery_taskmeta'}

    def test_mongo_result_backend_auth_tls(self):
        mongo = MongoConfig(
            uri='db.example.com', port=27017, tls=True,
            user='svc@user', password='p@ss:word', database='cheeto',
            tls_ca_file='/etc/ssl/ca.pem',
        )
        url, settings = mongo_result_backend(mongo)
        assert url == (
            'mongodb://svc%40user:p%40ss%3Aword@db.example.com:27017/'
            '?tls=true&tlsCAFile=%2Fetc%2Fssl%2Fca.pem'
        )
        assert settings['database'] == 'cheeto'

    def test_configure_celery_app(self, config):
        configured = configure_celery_app(config, app)
        assert configured.conf.broker_url == config.daemon.broker_url
        assert configured.conf.task_default_queue == 'cheeto'
        assert configured.conf.worker_concurrency == 1
        assert configured.conf.task_acks_late is False
        assert set(configured.conf.beat_schedule) == \
            set(build_beat_schedule(config))

    def test_all_scheduled_tasks_registered(self, config):
        # every task name beat schedules must be a registered celery task
        for entry in build_beat_schedule(config).values():
            assert entry['task'] in app.tasks


# ---------------------------------------------------------------------------
# Task bodies (plain coroutines against the test mongo — no broker)
# ---------------------------------------------------------------------------


def _with_sympa_outdir(config, outdir: Path):
    tasks = replace(
        config.daemon.tasks,
        sympa=replace(config.daemon.tasks.sympa, output_dir=str(outdir)),
    )
    return replace(config, daemon=replace(config.daemon, tasks=tasks))


def _with_puppet_repo(config, repo: Path):
    tasks = replace(
        config.daemon.tasks,
        puppet_sync=replace(config.daemon.tasks.puppet_sync, repo=str(repo)),
    )
    return replace(config, daemon=replace(config.daemon, tasks=tasks))


@pytest.fixture
def puppet_repo(tmp_path):
    """Bare origin + working clone, like test_git_async.py's fixture."""
    import os
    import sh
    if os.getenv('GITHUB_ACTIONS') == 'true':
        sh.git('config', '--global', 'user.name', 'Test User')
        sh.git('config', '--global', 'user.email', 'test@example.com')
    origin = tmp_path / 'origin.git'
    sh.git('init', '--bare', '-b', 'main', str(origin))
    clone = tmp_path / 'repo'
    sh.git('clone', str(origin), str(clone))
    sh.git('commit', '--allow-empty', '-m', 'Root commit', _cwd=str(clone))
    sh.git('push', '-u', 'origin', 'main', _cwd=str(clone))
    return origin, clone


async def _seed_user(name, uid, *, type='user', status='active', site=None,
                     access=('login-ssh',), keys=()):
    user = User(
        name=name, email=f'{name}@x.test', uid=uid, gid=uid,
        fullname=name, home_directory=f'/home/{name}', type=type,
        status=await status_link(status),
        access=await access_links(list(access)),
    )
    await user.insert()
    if site is not None:
        await UserSiteInfo(
            user=user, site=site, status=await status_link(status),
        ).insert()
    for k in keys:
        await SshKey(key=k, user=user).insert()
    return user


class TestTaskBodies:

    async def test_sympa_export_writes_atomically(
        self, beanie_client, clean_db, config, tmp_path,
    ):
        site = Site(name='test-site', fqdn='test.site')
        await site.insert()
        await _seed_user('dm_alice', 70001, site=site)
        await _seed_user('dm_bob', 70002, type='admin', site=site)
        await _seed_user('dm_gone', 70003, status='inactive', site=site)

        cfg = _with_sympa_outdir(config, tmp_path)
        result = await _sympa_export(cfg, beanie_client, None, 'test-site')

        target = tmp_path / 'test-site.txt'
        assert result['path'] == str(target)
        assert result['emails'] == 2
        expected = await ExportSympaEmails.run(
            beanie_client, None, sitename='test-site',
        )
        assert target.read_text() == expected
        assert target.read_text() == 'dm_alice@x.test\ndm_bob@x.test\n'
        # the temp file was renamed away, not left behind
        assert list(tmp_path.iterdir()) == [target]

    async def test_reap_flips_expired_offboarding_users(
        self, beanie_client, clean_db, config,
    ):
        expired = User(
            name='dm_expired', email='e@x.test', uid=70010, gid=70010,
            fullname='Expired', home_directory='/home/dm_expired',
            type='user', status=await status_link('offboarding'),
            expires_at=datetime(2020, 1, 1),
        )
        await expired.insert()

        reaped = await _reap(config, beanie_client, None)
        assert reaped == ['dm_expired']

        fetched = await User.find_one(User.name == 'dm_expired',
                                      fetch_links=True, nesting_depth=1)
        assert fetched.status.status_name == 'inactive'

    @staticmethod
    async def _origin_show(origin: Path, path: str) -> str:
        import sh
        return str(await sh.git(
            'show', f'main:{path}',
            _cwd=str(origin), _async=True, _tty_out=False,
        ))

    async def test_puppet_sync_writes_yaml_and_keys(
        self, beanie_client, clean_db, config, puppet_repo,
    ):
        import sh
        from ..models.history import History
        from ..puppet import PuppetAccountMap
        from ..queries import site_to_puppet_legacy

        origin, clone = puppet_repo
        site = Site(name='test-site', fqdn='test.site')
        await site.insert()
        await _seed_user('dm_alice', 70001, site=site,
                         keys=('ssh-ed25519 AAAA alice',))
        await _seed_user('dm_bob', 70002, site=site)

        cfg = _with_puppet_repo(config, clone)
        result = await _puppet_sync(cfg, beanie_client, None, 'test-site')

        assert result['changed'] is True
        assert result['pushed'] is True
        assert result['users_with_keys'] == 1

        expected = PuppetAccountMap.Schema().dumps(
            await site_to_puppet_legacy(site)
        ) + '\n'
        yaml_on_origin = await self._origin_show(
            origin, 'domains/test.site/merged/all.yaml',
        )
        assert yaml_on_origin == expected
        key_file = await self._origin_show(origin, 'keys/dm_alice.pub')
        assert key_file == 'ssh-ed25519 AAAA alice\n'

        # No leftover branches on origin (the v1 accumulation bug).
        branches = str(await sh.git(
            'branch', '--format=%(refname:short)',
            _cwd=str(origin), _async=True, _tty_out=False,
        ))
        assert branches.split() == ['main']

        history = await History.find_one(History.op == 'sync_old_puppet')
        assert history is not None
        assert history.changes['branch'] == result['branch']

    async def test_puppet_sync_second_run_no_changes(
        self, beanie_client, clean_db, config, puppet_repo,
    ):
        import sh
        origin, clone = puppet_repo
        site = Site(name='test-site', fqdn='test.site')
        await site.insert()
        await _seed_user('dm_alice', 70001, site=site)

        cfg = _with_puppet_repo(config, clone)
        first = await _puppet_sync(cfg, beanie_client, None, 'test-site')
        assert first['changed'] is True

        rev = str(await sh.git('rev-parse', 'main', _cwd=str(origin),
                               _async=True, _tty_out=False)).strip()
        second = await _puppet_sync(cfg, beanie_client, None, 'test-site')
        assert second['changed'] is False
        assert second['pushed'] is False
        rev_after = str(await sh.git('rev-parse', 'main', _cwd=str(origin),
                                     _async=True, _tty_out=False)).strip()
        assert rev_after == rev

    async def test_puppet_sync_unknown_site_raises(
        self, beanie_client, clean_db, config, puppet_repo,
    ):
        _, clone = puppet_repo
        cfg = _with_puppet_repo(config, clone)
        with pytest.raises(ValueError, match='does not exist'):
            await _puppet_sync(cfg, beanie_client, None, 'nope')

    async def test_puppet_sync_missing_clone_raises(
        self, beanie_client, clean_db, config, tmp_path,
    ):
        site = Site(name='test-site', fqdn='test.site')
        await site.insert()
        cfg = _with_puppet_repo(config, tmp_path / 'not-a-repo')
        with pytest.raises(ValueError, match='not a git repository'):
            await _puppet_sync(cfg, beanie_client, None, 'test-site')


class TestCeleryEagerSmoke:
    """End-to-end through the celery wrapper: env-var config loading,
    run_op's fresh event loop + fresh beanie client, and JSON-serializable
    results — without a broker."""

    def test_reap_task_eager(self, config_file, monkeypatch):
        from ..daemon import tasks

        monkeypatch.setenv('CHEETO_CONFIG', str(config_file))
        monkeypatch.setenv('CHEETO_PROFILE', 'default')
        daemon_config.cache_clear()
        old_eager = app.conf.task_always_eager
        app.conf.task_always_eager = True
        try:
            result = tasks.reap.delay()
            assert result.get() == []
        finally:
            app.conf.task_always_eager = old_eager
            daemon_config.cache_clear()


# ---------------------------------------------------------------------------
# FastAPI service
# ---------------------------------------------------------------------------


def _api_client(api):
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=api),
                             base_url='http://test')


class TestDaemonApi:

    @pytest_asyncio.fixture(loop_scope='session')
    async def rk_site(self, beanie_client, clean_db):
        site = Site(name='api-site', fqdn='api.test')
        await site.insert()
        await _seed_user(
            'api_admin', 70020, type='admin',
            access=('login-ssh', 'root-ssh'), site=site,
            keys=('ssh-ed25519 AAAAapi admin@laptop',),
        )
        return site

    async def test_root_keys_requires_api_key(self, beanie_client, config, rk_site):
        api = create_api(config, client=beanie_client)
        async with _api_client(api) as client:
            resp = await client.get('/puppet/root-keys/api-site')
            assert resp.status_code == 401
            resp = await client.get('/puppet/root-keys/api-site',
                                    headers={'X-API-Key': 'wrong'})
            assert resp.status_code == 401

    async def test_root_keys_with_api_key(self, beanie_client, config, rk_site):
        api = create_api(config, client=beanie_client)
        async with _api_client(api) as client:
            resp = await client.get('/puppet/root-keys/api-site',
                                    headers={'X-API-Key': 'test-api-key'})
        assert resp.status_code == 200
        expected = await ExportRootSSHKeys.run(
            beanie_client, None, sitename='api-site',
        )
        assert resp.text == expected
        assert 'REMOTE_SSH_USER=api_admin' in resp.text

    async def test_root_keys_unknown_site_404(self, beanie_client, config, rk_site):
        api = create_api(config, client=beanie_client)
        async with _api_client(api) as client:
            resp = await client.get('/puppet/root-keys/nope',
                                    headers={'X-API-Key': 'test-api-key'})
        assert resp.status_code == 404

    async def test_endpoint_accepts_fqdn(self, beanie_client, config, rk_site):
        """`{site}` may be the site name or its fqdn; both resolve to the same
        canonical site."""
        api = create_api(config, client=beanie_client)
        async with _api_client(api) as client:
            by_name = await client.get('/puppet/root-keys/api-site',
                                       headers={'X-API-Key': 'test-api-key'})
            by_fqdn = await client.get('/puppet/root-keys/api.test',
                                       headers={'X-API-Key': 'test-api-key'})
        assert by_name.status_code == 200
        assert by_fqdn.status_code == 200
        assert by_fqdn.text == by_name.text
        assert 'REMOTE_SSH_USER=api_admin' in by_fqdn.text

    async def test_no_api_key_configured_is_open(
        self, beanie_client, config, rk_site,
    ):
        open_config = replace(config, api=replace(config.api, api_key=None))
        api = create_api(open_config, client=beanie_client)
        async with _api_client(api) as client:
            resp = await client.get('/puppet/root-keys/api-site')
        assert resp.status_code == 200
        assert 'api_admin' in resp.text

    async def test_puppet_storage(self, beanie_client, config, rk_site):
        from ..models.group import Group
        from ..models.storage import (
            NFSExportConfig,
            Storage,
            StorageAllocation,
            StorageVolume,
            ZFSConfig,
        )
        from ..operations.storage import ExportPuppetStorage

        owner = await User.find_one(User.name == 'api_admin')
        group = Group(name='api_admin', gid=70020)
        await group.insert()
        volume = StorageVolume(
            name='apigrp', site=rk_site, backend='zfs', zfs=ZFSConfig(),
            host='nas-9', host_path='/nas-9/apigrp',
            allocations=[StorageAllocation(quota='10T')],
            nfs_export=NFSExportConfig(
                export_options='rw,sync', export_ranges=['10.0.0.0/8'],
            ),
        )
        await volume.insert()
        await Storage(
            name='apigrp', site=rk_site, category='group',
            owner=owner, group=group, volume=volume,
        ).insert()

        api = create_api(config, client=beanie_client)
        async with _api_client(api) as client:
            resp = await client.get('/puppet/storage/api-site',
                                    headers={'X-API-Key': 'test-api-key'})
            unauth = await client.get('/puppet/storage/api-site')
            missing = await client.get('/puppet/storage/nope',
                                       headers={'X-API-Key': 'test-api-key'})

        assert unauth.status_code == 401
        assert missing.status_code == 404
        assert resp.status_code == 200
        data = resp.json()
        assert data == await ExportPuppetStorage.run(
            beanie_client, None, sitename='api-site',
        )
        assert data['zfs']['group'] == {'nas-9': [{
            'name': 'apigrp',
            'owner': 'api_admin',
            'group': 'api_admin',
            'path': '/nas-9/apigrp',
            'export_options': 'rw,sync',
            'export_ranges': ['10.0.0.0/8'],
            'quota': '10T',
            'permissions': '2770',
        }]}
