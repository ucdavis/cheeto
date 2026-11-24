"""Tests for the read-only REST API (`cheeto/daemon/routes/`).

One seeded site covers the cases the routes must tell apart: a sticky
group, a sponsor without a `UserSiteInfo`, a per-site status override, a
user without Slurm access, a sticky Slurm account, and home / group / share
storages. Requires the session-scoped start_mongodb fixture from
conftest.py.
"""

import json
import threading
from dataclasses import replace

import httpx
import pytest
import pytest_asyncio
from beanie import init_beanie
from pymongo import AsyncMongoClient
from pymongo.monitoring import CommandListener

from ..daemon.api import create_api
from ..daemon.routes import puppet as puppet_routes
from ..models import ALL_MODELS
from ..models.group import Group
from ..models.group_membership import GroupMembership
from ..models.group_site_info import GroupSiteInfo
from ..models.history import History
from ..models.site import Site
from ..models.slurm import (
    SlurmAccount,
    SlurmAllocation,
    SlurmAssociation,
    SlurmPartition,
    SlurmQOS,
    SlurmTRES,
)
from ..models.storage import (
    AutomountMap,
    Storage,
    StorageAllocation,
    StorageVolume,
    ZFSConfig,
)
from ..models.user import SshKey, User
from ..models.user_site_info import UserSiteInfo
from ..puppet import PuppetAccountMap
from ..queries.puppet_legacy import site_to_puppet_legacy
from .conftest import (
    MONGODB_PORT,
    access_links,
    seed_access_status_groups,
    status_link,
)

API_TEST_DB = 'cheeto_api_test'
KEY = {'X-API-Key': 'test-api-key'}
SITE = 'apisite'


class _ReadCounter(CommandListener):
    """Counts the read commands the client sends while `active`."""

    def __init__(self):
        self.active = False
        self.count = 0

    def started(self, event):
        if self.active and event.command_name in ('find', 'aggregate'):
            self.count += 1

    def succeeded(self, event):
        pass

    def failed(self, event):
        pass


_counter = _ReadCounter()


@pytest_asyncio.fixture(scope='session', loop_scope='session')
async def beanie_client(start_mongodb):
    client = AsyncMongoClient(f'127.0.0.1:{MONGODB_PORT}',
                              event_listeners=[_counter])
    await init_beanie(database=client[API_TEST_DB], document_models=ALL_MODELS)
    yield client
    await client.close()


@pytest_asyncio.fixture(loop_scope='session')
async def clean_db(beanie_client):
    await init_beanie(database=beanie_client[API_TEST_DB],
                      document_models=ALL_MODELS)
    for model in ALL_MODELS:
        await model.find_all().delete()
    await History.find_all().delete()
    await seed_access_status_groups()


async def _user(name, uid, site, *, access=('login-ssh', 'slurm'),
                status='active', site_status=None, at_site=True, **kwargs):
    user = User(
        name=name, email=f'{name}@api.test', uid=uid, gid=uid,
        fullname=name.title(), home_directory=f'/home/{name}',
        status=await status_link(status),
        access=await access_links(list(access)), **kwargs,
    )
    await user.insert()
    if at_site:
        await UserSiteInfo(
            user=user, site=site,
            status=await status_link(site_status) if site_status else None,
        ).insert()
    return user


async def _group(name, gid, site, *, present=True, **kwargs):
    group = Group(name=name, gid=gid, **kwargs)
    await group.insert()
    if present:
        await GroupSiteInfo(group=group, site=site).insert()
    return group


async def _edge(user, group, site, *roles):
    await GroupMembership(
        user=user, group=group, site=site, roles=list(roles),
    ).insert()


async def _home(user, group, site, volume, amap):
    await Storage(
        name=user.name, site=site, category='home', owner=user, group=group,
        volume=volume, subpath=user.name, automount_map=amap,
    ).insert()


@pytest_asyncio.fixture(loop_scope='session')
async def seeded(beanie_client, clean_db):
    """The reference site. Returns the Site."""
    site = Site(name=SITE, fqdn='api.hpc.test', aliases=['apis'])
    await site.insert()
    other = Site(name='othersite', fqdn='other.hpc.test')
    await other.insert()

    lab = await _group('lab', 73100, site)
    everyone = await _group('everyone', 73101, site)
    pub = await _group('pub', 73102, site)
    await _group('offsite', 73103, other)

    alice = await _user('alice', 73001, site, password='$y$hash',
                        comments=['SECRET-user-comment'])
    bob = await _user('bob', 73002, site, access=('login-ssh',))
    carol = await _user('carol', 73003, site, site_status='disabled')
    dave = await _user('dave', 73004, site)
    pi = await _user('pi', 73005, site, at_site=False)
    alice_group = await _group('alice', 73001, site, type='user')

    await _edge(alice, lab, site, 'member', 'sudoer')
    await _edge(bob, lab, site, 'member')
    await _edge(carol, lab, site, 'slurmer')
    await _edge(pi, lab, site, 'sponsor')
    await SshKey(key='ssh-ed25519 AAAAalice alice@laptop', user=alice).insert()

    # Slurm: `lab` reaches its account through membership; `pub` is a
    # sticky account and the site default.
    hi = SlurmPartition(name='hi', site=site)
    low = SlurmPartition(name='low', site=site)
    await hi.insert()
    await low.insert()
    lab_alloc = SlurmAllocation(tres=SlurmTRES(cpus=8, mem='16G'),
                                comment='SECRET-slurm-comment')
    pub_alloc = SlurmAllocation(tres=SlurmTRES(cpus=2, mem='4G'))
    await lab_alloc.insert()
    await pub_alloc.insert()
    lab_q = SlurmQOS(name='lab-hi-qos', site=site, group_limits=[lab_alloc],
                     priority=10)
    pub_q = SlurmQOS(name='pub-low-qos', site=site, user_limits=[pub_alloc])
    await lab_q.insert()
    await pub_q.insert()
    lab_acct = SlurmAccount(group=lab, site=site, coordinators=[alice])
    pub_acct = SlurmAccount(group=pub, site=site)
    await lab_acct.insert()
    await pub_acct.insert()
    await SlurmAssociation(site=site, account=lab_acct, partition=hi,
                           qos=lab_q).insert()
    await SlurmAssociation(site=site, account=pub_acct, partition=low,
                           qos=pub_q).insert()

    site.group.sticky = [everyone]
    site.slurm.sticky = [pub_acct]
    site.slurm.default_account = pub_acct
    await site.save()

    # Storage: one volume, three automount maps.
    volume = StorageVolume(
        name='nas1', site=site, backend='zfs', zfs=ZFSConfig(),
        host='nas1', host_path='/nas1/pool',
        allocations=[StorageAllocation(quota='10T',
                                       comment='SECRET-storage-comment')],
    )
    await volume.insert()
    maps = {}
    for name in ('home', 'group', 'share'):
        maps[name] = AutomountMap(name=name, site=site, prefix=f'/{name}',
                                  options=['fstype=nfs', 'rw'])
        await maps[name].insert()
    await _home(alice, alice_group, site, volume, maps['home'])
    await Storage(
        name='labdata', site=site, category='group', owner=pi, group=lab,
        volume=volume, subpath='labdata', automount_map=maps['group'],
    ).insert()
    await Storage(
        name='refdata', site=site, category='share', owner=pi, group=pub,
        volume=volume, subpath='refdata', automount_map=maps['share'],
    ).insert()

    return await Site.find_one(Site.name == SITE)


@pytest_asyncio.fixture(loop_scope='session')
async def api(beanie_client, config, seeded):
    client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=create_api(config,
                                                     client=beanie_client)),
        base_url='http://test', headers=KEY,
    )
    async with client:
        yield client


async def _get(api, path, **params):
    resp = await api.get(path, params=params or None)
    assert resp.status_code == 200, (path, resp.status_code, resp.text)
    return resp.json()


def _by_name(rows):
    return {row['name']: row for row in rows}


# Not in ALL_ROUTES: the tests below add them where they apply.
PUPPET_ROUTES = (
    f'/puppet/root-keys/{SITE}',
    f'/puppet/storage/{SITE}',
    f'/puppet/accounts/{SITE}',
)

ALL_ROUTES = (
    '/sites',
    f'/sites/{SITE}',
    f'/sites/{SITE}/users',
    f'/sites/{SITE}/groups',
    '/users',
    '/users/alice',
    '/users/alice/ssh-keys',
    '/users/alice/sites',
    f'/users/alice/sites/{SITE}',
    f'/users/alice/sites/{SITE}/groups',
    f'/users/alice/sites/{SITE}/slurm',
    f'/users/alice/sites/{SITE}/storage',
    '/groups',
    '/groups/lab',
    f'/groups/lab/sites/{SITE}',
    f'/groups/lab/sites/{SITE}/members',
    f'/groups/lab/sites/{SITE}/slurm',
    f'/groups/lab/sites/{SITE}/storage',
    f'/sites/{SITE}/slurm/accounts',
    f'/sites/{SITE}/slurm/partitions',
    f'/sites/{SITE}/slurm/qos',
    f'/sites/{SITE}/slurm/qos/lab-hi-qos',
    f'/sites/{SITE}/slurm/associations',
    f'/sites/{SITE}/slurm/allocations',
    f'/sites/{SITE}/slurm/user-associations',
    f'/sites/{SITE}/storage/storages',
    f'/sites/{SITE}/storage/volumes',
    f'/sites/{SITE}/storage/automount-maps',
)


class TestApiContract:

    async def test_every_route_answers(self, api):
        for path in ALL_ROUTES:
            await _get(api, path)

    async def test_every_route_is_listed(self, beanie_client, config):
        """ALL_ROUTES stays complete: a new route must be added to it."""
        app = create_api(config, client=beanie_client)
        templates = {
            r.path for r in app.routes
            if getattr(r, 'methods', None) and not r.path.startswith(
                ('/puppet', '/openapi', '/docs', '/redoc'),
            )
        }
        covered = {
            p.replace(SITE, '{site}').replace('alice', '{username}')
             .replace('/groups/lab', '/groups/{groupname}')
             .replace('lab-hi-qos', '{name}')
            for p in ALL_ROUTES
        }
        assert templates == covered

    async def test_only_get_routes(self, beanie_client, config):
        app = create_api(config, client=beanie_client)
        for route in app.routes:
            methods = getattr(route, 'methods', None) or set()
            assert methods <= {'GET', 'HEAD'}, route.path

    async def test_api_key_is_required(self, api):
        for path in ALL_ROUTES:
            resp = await api.get(path, headers={'X-API-Key': 'wrong'})
            assert resp.status_code == 401, path

    async def test_unknown_names_are_404(self, api):
        for path in (
            '/sites/nope', '/sites/nope/users', '/sites/nope/slurm/qos',
            '/users/nope', f'/users/nope/sites/{SITE}',
            '/users/alice/sites/nope',
            '/groups/nope', f'/groups/nope/sites/{SITE}',
            f'/sites/{SITE}/slurm/qos/nope',
            f'/sites/{SITE}/slurm/associations?group=nope',
            f'/sites/{SITE}/storage/storages?owner=nope',
            '/users?site=nope',
        ):
            assert (await api.get(path)).status_code == 404, path

    async def test_absent_at_site_is_404(self, api):
        # `pi` sponsors a group at the site but has no UserSiteInfo there.
        for path in (
            f'/users/pi/sites/{SITE}', f'/users/pi/sites/{SITE}/groups',
            f'/users/pi/sites/{SITE}/slurm', f'/users/pi/sites/{SITE}/storage',
            f'/groups/offsite/sites/{SITE}',
            f'/groups/everyone/sites/{SITE}/slurm',   # present, no account
        ):
            assert (await api.get(path)).status_code == 404, path

    async def test_bad_filter_value_is_422(self, api):
        assert (await api.get('/users?status=nope')).status_code == 422

    async def test_no_comment_fields_in_the_schema(self, beanie_client,
                                                   config):
        """No response model has a `comment` or `comments` field."""
        schema = create_api(config, client=beanie_client).openapi()
        for name, model in schema['components']['schemas'].items():
            fields = set(model.get('properties', {}))
            assert not fields & {'comment', 'comments'}, name

    async def test_no_comment_values_anywhere(self, api):
        """The seeded comment values reach no response, on any route,
        including the puppet exports."""
        paths = ALL_ROUTES + PUPPET_ROUTES + (
            f'/sites/{SITE}/groups?personal=true',
        )
        for path in paths:
            body = json.dumps(await _get(api, path))
            assert 'SECRET-' not in body, path
            assert '"comment' not in body, path

    async def test_no_password_hash_anywhere(self, api):
        for path in ALL_ROUTES + PUPPET_ROUTES:
            assert '$y$hash' not in json.dumps(await _get(api, path)), path

    async def test_no_history_rows(self, api):
        for path in ALL_ROUTES:
            await _get(api, path)
        assert await History.find_all().count() == 0

    async def test_site_accepts_fqdn_and_alias(self, api):
        by_name = await _get(api, f'/sites/{SITE}/slurm/partitions')
        assert await _get(api, '/sites/api.hpc.test/slurm/partitions') == by_name
        assert await _get(api, '/sites/apis/slurm/partitions') == by_name

    async def test_prefix(self, beanie_client, config, seeded):
        cfg = replace(config, api=replace(config.api, prefix='/cheeto/api/v1'))
        transport = httpx.ASGITransport(
            app=create_api(cfg, client=beanie_client),
        )
        async with httpx.AsyncClient(transport=transport,
                                     base_url='http://test',
                                     headers=KEY) as client:
            assert (await client.get('/cheeto/api/v1/sites')).status_code == 200
            assert (await client.get('/sites')).status_code == 404

    async def test_docs_and_schema_are_below_the_prefix(self, beanie_client,
                                                        config):
        """A reverse proxy forwards only the prefix. The docs page must be
        there, and must load the schema from there."""
        cfg = replace(config, api=replace(config.api, prefix='/cheeto/api/v1'))
        transport = httpx.ASGITransport(
            app=create_api(cfg, client=beanie_client),
        )
        async with httpx.AsyncClient(transport=transport,
                                     base_url='http://test') as client:
            docs = await client.get('/cheeto/api/v1/docs')
            assert docs.status_code == 200
            assert "url: '/cheeto/api/v1/openapi.json'" in docs.text
            schema = await client.get('/cheeto/api/v1/openapi.json')
            assert schema.status_code == 200
            assert '/cheeto/api/v1/sites' in schema.json()['paths']
            assert (await client.get('/cheeto/api/v1/redoc')
                    ).status_code == 200
            for path in ('/docs', '/openapi.json', '/redoc'):
                assert (await client.get(path)).status_code == 404, path

    async def test_docs_without_a_prefix(self, beanie_client, config):
        transport = httpx.ASGITransport(
            app=create_api(config, client=beanie_client),
        )
        async with httpx.AsyncClient(transport=transport,
                                     base_url='http://test') as client:
            docs = await client.get('/docs')
            assert docs.status_code == 200
            assert "url: '/openapi.json'" in docs.text
            assert (await client.get('/openapi.json')).status_code == 200


class TestUsers:

    async def test_global_record(self, api):
        alice = await _get(api, '/users/alice')
        assert alice['uid'] == 73001
        assert alice['status'] == 'active'
        assert alice['access'] == ['login-ssh', 'slurm']
        assert alice['has_password'] is True
        assert alice['sites'] == [SITE]
        assert 'password' not in alice
        assert (await _get(api, '/users/bob'))['has_password'] is False
        assert (await _get(api, '/users/pi'))['sites'] == []

    async def test_ssh_keys(self, api):
        keys = await _get(api, '/users/alice/ssh-keys')
        assert keys == {'user': 'alice',
                        'keys': ['ssh-ed25519 AAAAalice alice@laptop']}

    async def test_list_filters(self, api):
        names = lambda rows: [r['name'] for r in rows]
        assert names(await _get(api, '/users', site=SITE)) == [
            'alice', 'bob', 'carol', 'dave',
        ]
        assert names(await _get(api, '/users', site=SITE, access='slurm')) == [
            'alice', 'carol', 'dave',
        ]
        # The per-site override counts when `site` is given.
        assert names(await _get(api, '/users', site=SITE,
                                status='disabled')) == ['carol']
        assert names(await _get(api, '/users', group='lab')) == [
            'alice', 'bob',
        ]

    async def test_site_override_and_effective_values(self, api):
        carol = await _get(api, f'/users/carol/sites/{SITE}')
        assert carol['status_override'] == 'disabled'
        assert carol['status'] == 'disabled'
        assert carol['access_override'] == []
        assert carol['access'] == ['login-ssh', 'slurm']
        # The global record still says active.
        assert (await _get(api, '/users/carol'))['status'] == 'active'

        alice = await _get(api, f'/users/alice/sites/{SITE}')
        assert alice['status_override'] is None
        assert alice['status'] == 'active'
        assert await _get(api, '/users/alice/sites') == [alice]

    async def test_groups_carry_all_roles(self, api):
        groups = _by_name(await _get(api, f'/users/alice/sites/{SITE}/groups'))
        assert groups['lab']['roles'] == ['member', 'sudoer']
        assert groups['lab']['sticky'] is False
        assert groups['everyone'] == {
            'name': 'everyone', 'gid': 73101, 'roles': [], 'sticky': True,
        }
        dave = _by_name(await _get(api, f'/users/dave/sites/{SITE}/groups'))
        assert set(dave) == {'everyone'}

    async def test_slurm(self, api):
        alice = await _get(api, f'/users/alice/sites/{SITE}/slurm')
        assert alice['eligible'] is True
        assert alice['max_jobs'] == -1
        assert alice['default_account'] == 'pub'
        assert alice['accounts'] == [
            {'account': 'lab', 'via': ['member'],
             'associations': [{'partition': 'hi', 'qos': 'lab-hi-qos'}]},
            {'account': 'pub', 'via': ['sticky'],
             'associations': [{'partition': 'low', 'qos': 'pub-low-qos'}]},
        ]
        # No slurm access: the accounts are listed, the user is not eligible.
        bob = await _get(api, f'/users/bob/sites/{SITE}/slurm')
        assert bob['eligible'] is False
        assert bob['max_jobs'] is None
        assert bob['default_account'] is None
        assert [a['account'] for a in bob['accounts']] == ['lab', 'pub']
        # Disabled at the site: associations stay, MaxJobs is 0.
        carol = await _get(api, f'/users/carol/sites/{SITE}/slurm')
        assert (carol['eligible'], carol['max_jobs']) == (True, 0)
        assert carol['accounts'][0]['via'] == ['slurmer']

    async def test_storage(self, api):
        alice = await _get(api, f'/users/alice/sites/{SITE}/storage')
        assert [s['name'] for s in alice['owned']] == ['alice']
        home = alice['owned'][0]
        assert home['category'] == 'home'
        assert home['host'] == 'nas1'
        assert home['host_path'] == '/nas1/pool/alice'
        assert home['mount_type'] == 'automount'
        assert home['mount_source'] == 'home'
        assert home['mount_path'] == '/home/alice'
        assert [s['name'] for s in alice['group']] == ['labdata']
        # carol is a slurmer of lab, not a member: no group storage.
        carol = await _get(api, f'/users/carol/sites/{SITE}/storage')
        assert carol == {'owned': [], 'group': []}


class TestGroups:

    async def test_global_record_and_list(self, api):
        lab = await _get(api, '/groups/lab')
        assert (lab['gid'], lab['type'], lab['sites']) == (73100, 'group',
                                                           [SITE])
        names = [g['name'] for g in await _get(api, '/groups', site=SITE)]
        assert names == ['everyone', 'lab', 'pub']     # no personal group
        assert [g['name'] for g in await _get(api, '/groups', site=SITE,
                                              user='alice')] == ['lab']
        assert 'alice' in [
            g['name'] for g in await _get(api, '/groups', type='user')
        ]

    async def test_roster(self, api):
        roster = await _get(api, f'/groups/lab/sites/{SITE}/members')
        assert roster == {
            'members': ['alice', 'bob'], 'sponsors': ['pi'],
            'sudoers': ['alice'], 'slurmers': ['carol'],
        }
        summary = await _get(api, f'/groups/lab/sites/{SITE}')
        assert summary['members'] == 2
        assert summary['sponsors'] == 1
        assert summary['slurm_account'] is True
        assert summary['sticky'] is False
        everyone = await _get(api, f'/groups/everyone/sites/{SITE}/members')
        assert everyone['members'] == ['alice', 'bob', 'carol', 'dave']

    async def test_slurm_and_storage(self, api):
        slurm = await _get(api, f'/groups/lab/sites/{SITE}/slurm')
        assert slurm['account']['name'] == 'lab'
        assert slurm['account']['coordinators'] == ['alice']
        assert slurm['account']['limits']['max_user_jobs'] == -1
        assert slurm['associations'] == [
            {'partition': 'hi', 'qos': 'lab-hi-qos'},
        ]
        storage = await _get(api, f'/groups/lab/sites/{SITE}/storage')
        assert [(s['name'], s['owner'], s['mount_path']) for s in storage] == [
            ('labdata', 'pi', '/group/labdata'),
        ]


class TestSlurm:

    async def test_configured_state(self, api):
        base = f'/sites/{SITE}/slurm'
        assert await _get(api, f'{base}/partitions') == ['hi', 'low']
        accounts = _by_name(await _get(api, f'{base}/accounts'))
        assert set(accounts) == {'lab', 'pub'}
        assert accounts['lab']['coordinators'] == ['alice']

        qos = await _get(api, f'{base}/qos/lab-hi-qos')
        assert qos['priority'] == 10
        assert qos['group_limits'] == [{
            'tres': {'cpus': 8, 'gpus': None, 'mem': '16G'},
            'expires_at': None,
        }]
        assert qos['group_total'] == {'cpus': 8, 'gpus': None, 'mem': '16G'}
        # No limit in a bucket: unlimited, as null.
        assert qos['user_total'] == {'cpus': None, 'gpus': None, 'mem': None}
        assert qos in await _get(api, f'{base}/qos')

        assert await _get(api, f'{base}/associations') == [
            {'account': 'lab', 'partition': 'hi', 'qos': 'lab-hi-qos'},
            {'account': 'pub', 'partition': 'low', 'qos': 'pub-low-qos'},
        ]
        assert await _get(api, f'{base}/associations', group='lab',
                          partition='low') == []
        allocations = await _get(api, f'{base}/allocations', group='pub')
        assert [(a['qos'], a['limit'], a['tres']['cpus'])
                for a in allocations] == [('pub-low-qos', 'user_limits', 2)]

    async def test_effective_state(self, api):
        data = await _get(api, f'/sites/{SITE}/slurm/user-associations')
        rows = {(a['user'], a['account'], a['partition']): a
                for a in data['associations']}
        # bob has no slurm access; pi is not at the site.
        assert set(rows) == {
            ('alice', 'lab', 'hi'), ('alice', 'pub', 'low'),
            ('carol', 'lab', 'hi'), ('carol', 'pub', 'low'),
            ('dave', 'pub', 'low'),
        }
        assert rows[('alice', 'lab', 'hi')]['qos'] == 'lab-hi-qos'
        assert rows[('alice', 'lab', 'hi')]['max_jobs'] == -1
        assert rows[('carol', 'lab', 'hi')]['max_jobs'] == 0
        assert data['default_accounts'] == {
            'alice': 'pub', 'carol': 'pub', 'dave': 'pub',
        }
        assert data['coordinators'] == {'lab': ['alice']}

    async def test_user_view_agrees_with_the_sync_state(self, api):
        """The per-user route and the site-wide sync state are two code
        paths; they must describe the same associations."""
        data = await _get(api, f'/sites/{SITE}/slurm/user-associations')
        site_users = _by_name(await _get(api, f'/sites/{SITE}/users'))
        for name in ('alice', 'bob', 'carol', 'dave'):
            expected = {
                (a['account'], a['partition'], a['qos'], a['max_jobs'])
                for a in data['associations'] if a['user'] == name
            }
            view = await _get(api, f'/users/{name}/sites/{SITE}/slurm')
            got = {
                (acct['account'], pq['partition'], pq['qos'], view['max_jobs'])
                for acct in view['accounts'] for pq in acct['associations']
            } if view['eligible'] else set()
            assert got == expected, name
            assert view['default_account'] == data['default_accounts'].get(name)

            # The bulk user list carries the same gate and account names.
            bulk = site_users[name]['slurm']
            assert bulk['eligible'] == view['eligible'], name
            assert bulk['max_jobs'] == view['max_jobs'], name
            assert bulk['accounts'] == [a['account'] for a in view['accounts']]


class TestStorage:

    async def test_storages_and_filters(self, api):
        base = f'/sites/{SITE}/storage'
        names = lambda rows: [r['name'] for r in rows]
        assert names(await _get(api, f'{base}/storages')) == [
            'alice', 'labdata', 'refdata',
        ]
        assert names(await _get(api, f'{base}/storages',
                                category='share')) == ['refdata']
        assert names(await _get(api, f'{base}/storages', owner='pi')) == [
            'labdata', 'refdata',
        ]
        assert names(await _get(api, f'{base}/storages', group='lab')) == [
            'labdata',
        ]
        assert (await api.get(f'{base}/storages?category=nope')
                ).status_code == 422

    async def test_volumes_and_maps(self, api):
        base = f'/sites/{SITE}/storage'
        volumes = await _get(api, f'{base}/volumes')
        assert [(v['name'], v['backend'], v['host_path'], v['quota'])
                for v in volumes] == [('nas1', 'zfs', '/nas1/pool', '10T')]
        maps = await _get(api, f'{base}/automount-maps')
        assert [(m['name'], m['prefix']) for m in maps] == [
            ('group', '/group'), ('home', '/home'), ('share', '/share'),
        ]


class TestSiteViews:

    async def test_site_detail(self, api):
        site = await _get(api, f'/sites/{SITE}')
        assert site == {
            'name': SITE, 'fqdn': 'api.hpc.test', 'aliases': ['apis'],
            'sticky_groups': ['everyone'],
            'sticky_slurm_accounts': ['pub'],
            'default_slurm_account': 'pub',
        }
        assert [s['name'] for s in await _get(api, '/sites')] == [
            SITE, 'othersite',
        ]

    async def test_users(self, api):
        users = _by_name(await _get(api, f'/sites/{SITE}/users'))
        assert set(users) == {'alice', 'bob', 'carol', 'dave'}
        alice = users['alice']
        assert alice['status'] == 'active'
        assert alice['has_password'] is True
        assert {g['name']: g['roles'] for g in alice['groups']} == {
            'everyone': [], 'lab': ['member', 'sudoer'],
        }
        assert alice['slurm'] == {
            'eligible': True, 'max_jobs': -1, 'accounts': ['lab', 'pub'],
        }
        assert alice['home']['mount_path'] == '/home/alice'
        assert users['bob']['home'] is None
        assert users['carol']['status'] == 'disabled'
        assert users['carol']['slurm']['max_jobs'] == 0

    async def test_groups(self, api):
        groups = _by_name(await _get(api, f'/sites/{SITE}/groups'))
        assert set(groups) == {'everyone', 'lab', 'pub'}
        lab = groups['lab']
        assert lab['members'] == ['alice', 'bob']
        assert lab['sponsors'] == ['pi']       # not at the site, still named
        assert lab['sudoers'] == ['alice']
        assert lab['slurmers'] == ['carol']
        assert lab['slurm_account'] is True
        assert [s['name'] for s in lab['storages']] == ['labdata']
        assert groups['everyone']['sticky'] is True
        assert groups['everyone']['members'] == [
            'alice', 'bob', 'carol', 'dave',
        ]
        personal = _by_name(
            await _get(api, f'/sites/{SITE}/groups', personal='true'),
        )
        assert personal['alice']['type'] == 'user'

    async def test_parity_with_the_legacy_export(self, api, seeded):
        """The bulk routes carry everything `all.yaml` carries (the
        password hash excepted)."""
        legacy = await site_to_puppet_legacy(seeded)
        users = _by_name(await _get(api, f'/sites/{SITE}/users'))
        groups = _by_name(
            await _get(api, f'/sites/{SITE}/groups', personal='true'),
        )

        assert set(users) == set(legacy.user)
        for name, rec in legacy.user.items():
            got = users[name]
            for field in ('uid', 'gid', 'fullname', 'email', 'shell'):
                assert got[field] == getattr(rec, field), (name, field)
            assert got['home_directory'] == rec.home
            member_of = sorted(
                g['name'] for g in got['groups']
                if (g['sticky'] or 'member' in g['roles'])
                and g['name'] != name
            )
            assert member_of == sorted(rec.groups or []), name
            sudo_of = sorted(
                g['name'] for g in got['groups'] if 'sudoer' in g['roles']
            )
            assert sudo_of == sorted(rec.group_sudo or []), name
            # The API follows the Slurm sync, which also counts plain
            # members; the legacy list is a subset.
            legacy_accounts = set(rec.slurm.account) if rec.slurm else set()
            assert legacy_accounts <= set(got['slurm']['accounts']), name
            if rec.storage is not None:
                assert got['home']['host'] == rec.storage.autofs.nas

        # The legacy map adds the access/status/type groups (global rows);
        # every other legacy group is in the API list.
        special = await Group.find(
            {'type': {'$in': ['access', 'status', 'usertype']}},
            with_children=True,
        ).to_list()
        special_names = {g.name for g in special}
        for name, rec in legacy.group.items():
            if name in special_names:
                continue
            got = groups[name]
            assert got['gid'] == rec.gid
            assert got['sponsors'] == sorted(rec.sponsors or []), name
            assert sorted(s['name'] for s in got['storages']) == sorted(
                s.name for s in (rec.storage or [])
            ), name

        shares = await _get(api, f'/sites/{SITE}/storage/storages',
                            category='share')
        assert {s['name'] for s in shares} == set(legacy.share)


class TestLegacyExport:
    """`/puppet/accounts/{site}`: the `all.yaml` of a site as JSON."""

    PATH = f'/puppet/accounts/{SITE}'

    async def test_same_shape_as_the_yaml_export(self, api, seeded):
        """Equal to the document that `puppet_sync` writes to `all.yaml`,
        key for key, the `password` of the user records excepted."""
        expected = json.loads(json.dumps(
            PuppetAccountMap.Schema().dump(await site_to_puppet_legacy(seeded)),
        ))
        # The seed must have a hash, or the test proves nothing.
        assert expected['user']['alice'].pop('password') == '$y$hash'
        assert not any('password' in rec for rec in expected['user'].values())

        resp = await api.get(self.PATH)
        assert resp.status_code == 200
        assert resp.headers['content-type'] == 'application/json'
        got = resp.json()
        assert got == expected
        # The reference site has all three maps, so none is left out.
        assert set(got) == {'user', 'group', 'share'}
        assert set(got['user']) == {'alice', 'bob', 'carol', 'dave'}
        assert got['user']['alice']['storage']['autofs']['nas'] == 'nas1'
        assert got['group']['lab']['sponsors'] == ['pi']
        assert set(got['share']) == {'refdata'}

    async def test_same_order_as_the_yaml_export(self, api, seeded):
        """The maps are sorted like the YAML, so a text diff is useful."""
        got = (await api.get(self.PATH)).json()
        for name in ('user', 'group', 'share'):
            assert list(got[name]) == sorted(got[name]), name

    async def test_site_alias_and_unknown_site(self, api):
        by_alias = await api.get('/puppet/accounts/apis')
        assert by_alias.json() == (await api.get(self.PATH)).json()
        assert (await api.get('/puppet/accounts/nope')).status_code == 404

    async def test_build_runs_outside_the_event_loop_thread(self, api,
                                                            monkeypatch):
        """Tripwire: the CPU half of the export must not run in the thread
        of the event loop, where it stops every other request."""
        threads = []
        render = puppet_routes._accounts_json

        def spy(data):
            threads.append(threading.current_thread())
            return render(data)

        monkeypatch.setattr(puppet_routes, '_accounts_json', spy)
        assert (await api.get(self.PATH)).status_code == 200
        assert len(threads) == 1
        assert threads[0] is not threading.current_thread()

    async def test_fixed_query_count(self, api, seeded):
        cost = TestQueryCost()
        before = await cost._reads(api, self.PATH)
        lab = await Group.find_one(Group.name == 'lab')
        for i in range(10):
            user = await _user(f'more{i}', 76000 + i, seeded)
            group = await _group(f'moregrp{i}', 77000 + i, seeded)
            await _edge(user, group, seeded, 'member', 'sponsor')
            await _edge(user, lab, seeded, 'member')
        assert await cost._reads(api, self.PATH) == before
        assert before <= 20, before


class TestQueryCost:

    async def _reads(self, api, path) -> int:
        _counter.count = 0
        _counter.active = True
        try:
            await _get(api, path)
        finally:
            _counter.active = False
        return _counter.count

    async def test_bulk_routes_have_a_fixed_query_count(self, api, seeded):
        """More users and groups must not mean more queries."""
        paths = (
            f'/sites/{SITE}/users',
            f'/sites/{SITE}/groups',
            f'/sites/{SITE}/storage/storages',
            f'/sites/{SITE}/slurm/accounts',
        )
        before = {p: await self._reads(api, p) for p in paths}

        volume = await StorageVolume.find_one(StorageVolume.name == 'nas1')
        amap = await AutomountMap.find_one(AutomountMap.name == 'home')
        lab = await Group.find_one(Group.name == 'lab')
        for i in range(25):
            user = await _user(f'extra{i}', 74000 + i, seeded)
            personal = await _group(f'extra{i}', 74000 + i, seeded,
                                    type='user')
            group = await _group(f'grp{i}', 75000 + i, seeded)
            await _edge(user, group, seeded, 'member', 'sponsor')
            await _edge(user, lab, seeded, 'member')
            await _home(user, personal, seeded, volume, amap)
            await SlurmAccount(group=group, site=seeded,
                               coordinators=[user]).insert()

        after = {p: await self._reads(api, p) for p in paths}
        assert after == before
        assert max(before.values()) <= 16, before

    async def test_per_user_routes_have_a_fixed_query_count(self, api):
        for path in (
            '/users/alice',
            f'/users/alice/sites/{SITE}',
            f'/users/alice/sites/{SITE}/groups',
            f'/users/alice/sites/{SITE}/storage',
        ):
            assert await self._reads(api, path) <= 16, path

    @pytest.mark.parametrize('model, fields', [
        (GroupMembership, ('user', 'site')),
        (GroupMembership, ('group', 'site')),
        (GroupMembership, ('site',)),
        (UserSiteInfo, ('user', 'site')),
        (UserSiteInfo, ('site',)),
        (GroupSiteInfo, ('site',)),
        (GroupSiteInfo, ('group',)),
        (SshKey, ('user',)),
        (Storage, ('site',)),
        (Storage, ('owner',)),
        (Storage, ('group',)),
        (StorageVolume, ('site',)),
        (AutomountMap, ('site',)),
        (SlurmAccount, ('site',)),
        (SlurmAccount, ('group',)),
        (SlurmAssociation, ('site',)),
        (SlurmAssociation, ('account',)),
        (SlurmQOS, ('site',)),
        (SlurmPartition, ('site',)),
    ])
    async def test_link_filters_use_an_index(self, seeded, model, fields):
        """A filter on `<link>.$id` (what `Model.link.id == x` compiles to)
        must be an index scan, not a collection scan."""
        query = {f'{f}.$id': seeded.id for f in fields}
        plan = await model.get_pymongo_collection().find(query).explain()
        winning = json.dumps(plan['queryPlanner']['winningPlan'])
        assert 'IXSCAN' in winning, winning
        assert 'COLLSCAN' not in winning, winning
