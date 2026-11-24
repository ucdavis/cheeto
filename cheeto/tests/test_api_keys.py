"""Tests for REST API keys: the key format, the issue/revoke operations,
and the key and scope check of every route (`cheeto/daemon/deps.py`).

Uses the seeded site and the fixtures of `test_api.py`.
"""

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import httpx
import pytest
import pytest_asyncio
from fastapi import APIRouter, Security
from fastapi.routing import APIRoute

from ..apikey import (
    generate_api_key,
    hash_api_secret,
    normalize_scopes,
    parse_api_key,
)
from ..config import ApiCacheConfig, ApiConfig
from ..constants import API_KEY_PREFIX, API_SCOPES
from ..daemon.api import create_api
from ..daemon.cache import CachedRoute
from ..daemon.deps import KEY_TOUCH_INTERVAL, require_api_key
from ..models.apikey import ApiKey
from ..models.history import History
from ..models.site import Site
from ..models.user import SshKey
from ..operations import CreateApiKey, RevokeApiKey
from ..queries import find_api_key, list_api_keys
# The fixtures are used by name; pytest finds them in this namespace.
from .test_api import (  # noqa: F401
    ALL_ROUTES,
    KEY,
    PUPPET_ROUTES,
    SITE,
    _counter,
    _user,
    beanie_client,
    clean_db,
    seeded,
)

# The scope each route must need. Keyed by path prefix, longest first.
EXPECTED_SCOPE = (
    (f'/sites/{SITE}/slurm/', 'slurm'),
    (f'/sites/{SITE}/storage/', 'storage'),
    (f'/sites/{SITE}/users', 'users'),
    (f'/sites/{SITE}/groups', 'groups'),
    ('/puppet/accounts/', 'legacy'),
    ('/puppet/', 'puppet'),
    ('/users', 'users'),
    ('/groups', 'groups'),
    ('/sites', 'sites'),
)


def expected_scope(path: str) -> str:
    return next(scope for prefix, scope in EXPECTED_SCOPE
                if path.startswith(prefix))


class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


async def _issue(client, *scopes, **kwargs) -> tuple[ApiKey, dict]:
    """Issue a key; returns the document and the request headers."""
    key, token = await CreateApiKey.run(
        client, None, name=kwargs.pop('name', 'test-client'),
        scopes=list(scopes), **kwargs,
    )
    return key, {'X-API-Key': token}


def _client(app) -> httpx.AsyncClient:
    # No default key: each request names its own.
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                             base_url='http://test')


@pytest_asyncio.fixture(loop_scope='session')
async def app(beanie_client, config, seeded):  # noqa: F811
    """The app with a fake clock for the last-use writes. The static key of
    the test config stays valid."""
    app = create_api(config, client=beanie_client)
    clock = Clock()
    app.state.key_touches.clock = clock
    async with _client(app) as client:
        yield SimpleNamespace(client=client, clock=clock, app=app,
                              db=beanie_client)


class TestKeyFormat:

    def test_generate_and_parse(self):
        token, key_id, secret_hash = generate_api_key()
        assert token.startswith(f'{API_KEY_PREFIX}_{key_id}_')
        assert len(key_id) == 12
        parsed_id, secret = parse_api_key(token)
        assert parsed_id == key_id
        assert hash_api_secret(secret) == secret_hash
        assert secret not in secret_hash
        assert len(secret) >= 43          # 32 random bytes, urlsafe base64

    def test_keys_are_unique(self):
        tokens = {generate_api_key()[0] for _ in range(50)}
        assert len(tokens) == 50

    def test_secret_can_contain_underscores(self):
        assert parse_api_key('cheeto_0123456789ab_a_b__c') == (
            '0123456789ab', 'a_b__c',
        )

    @pytest.mark.parametrize('token', [
        '', 'test-api-key', 'cheeto', 'cheeto_0123456789ab',
        'cheeto_0123456789ab_', 'other_0123456789ab_secret',
        'cheeto_0123_secret', 'cheeto_0123456789AB_secret',
        'cheeto_0123456789ag_secret',
    ])
    def test_bad_tokens(self, token):
        assert parse_api_key(token) is None

    def test_normalize_scopes(self):
        assert normalize_scopes(['users', 'slurm', 'users']) == [
            'slurm', 'users',
        ]
        assert normalize_scopes(['users', 'ALL']) == ['ALL']
        # Not a wildcard: the old spelling is an unknown scope.
        with pytest.raises(ValueError, match='unknown API scope'):
            normalize_scopes(['*'])
        with pytest.raises(ValueError, match='at least one'):
            normalize_scopes([])
        with pytest.raises(ValueError, match='unknown API scope'):
            normalize_scopes(['users', 'admin'])


class TestOperations:

    async def test_create_stores_only_the_hash(self, beanie_client,
                                               clean_db):
        key, token = await CreateApiKey.run(
            beanie_client, None, name='hippo', scopes=['users', 'slurm'],
            comment='for the HiPPO team',
        )
        key_id, secret = parse_api_key(token)
        stored = await find_api_key(key_id)
        assert stored.name == 'hippo'
        assert stored.scopes == ['slurm', 'users']
        assert stored.secret_hash == hash_api_secret(secret)
        assert stored.state() == 'active'
        assert stored.last_used_at is None

        raw = await ApiKey.get_pymongo_collection().find_one(
            {'key_id': key_id},
        )
        assert secret not in str(raw)
        assert token not in str(raw)

    async def test_history_has_no_secret_and_no_hash(self, beanie_client,
                                                     clean_db):
        key, token = await CreateApiKey.run(
            beanie_client, None, name='hippo', scopes=['users'],
        )
        await RevokeApiKey.run(beanie_client, None, key_id=key.key_id)
        rows = await History.find_all().to_list()
        assert sorted(r.op for r in rows) == [
            'create_api_key', 'revoke_api_key',
        ]
        _, secret = parse_api_key(token)
        for row in rows:
            text = str(row.changes)
            assert key.key_id in text
            assert secret not in text
            assert key.secret_hash not in text

    async def test_create_rejects_bad_input(self, beanie_client, clean_db):
        with pytest.raises(ValueError, match='unknown API scope'):
            await CreateApiKey.run(beanie_client, None, name='x',
                                   scopes=['admin'])
        with pytest.raises(ValueError, match='at least one'):
            await CreateApiKey.run(beanie_client, None, name='x', scopes=[])
        with pytest.raises(ValueError, match='not in the future'):
            await CreateApiKey.run(
                beanie_client, None, name='x', scopes=['users'],
                expires_at=datetime.now(timezone.utc) - timedelta(days=1),
            )
        assert await ApiKey.find_all().count() == 0

    async def test_names_can_repeat_for_rotation(self, beanie_client,
                                                 clean_db):
        old, _ = await CreateApiKey.run(beanie_client, None, name='hippo',
                                        scopes=['users'])
        new, _ = await CreateApiKey.run(beanie_client, None, name='hippo',
                                        scopes=['users'])
        assert old.key_id != new.key_id
        await RevokeApiKey.run(beanie_client, None, key_id=old.key_id)
        assert [k.key_id for k in await list_api_keys()] == [new.key_id]
        assert len(await list_api_keys(include_revoked=True)) == 2

    async def test_revoke(self, beanie_client, clean_db):
        key, _ = await CreateApiKey.run(beanie_client, None, name='hippo',
                                        scopes=['users'])
        revoked = await RevokeApiKey.run(beanie_client, None,
                                         key_id=key.key_id)
        assert revoked.state() == 'revoked'
        first = (await find_api_key(key.key_id)).revoked_at
        # Idempotent: the time of the first revoke stays.
        await RevokeApiKey.run(beanie_client, None, key_id=key.key_id)
        assert (await find_api_key(key.key_id)).revoked_at == first
        with pytest.raises(ValueError, match='does not exist'):
            await RevokeApiKey.run(beanie_client, None,
                                   key_id='000000000000')

    async def test_expired_state(self, beanie_client, clean_db):
        key, _ = await CreateApiKey.run(
            beanie_client, None, name='x', scopes=['users'],
            expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
        )
        assert key.state() == 'active'
        later = datetime.now(timezone.utc) + timedelta(hours=2)
        # Read back from MongoDB, where datetimes come back naive.
        assert (await find_api_key(key.key_id)).state(later) == 'expired'


class TestAuthentication:

    PATH = '/sites'

    async def test_no_key_and_bad_keys_get_401(self, app):
        key, headers = await _issue(app.db, 'sites')
        wrong_secret = headers['X-API-Key'][:-4] + 'AAAA'
        unknown = generate_api_key()[0]
        for bad in (None, '', 'garbage', 'wrong', wrong_secret, unknown):
            resp = await app.client.get(
                self.PATH, headers={'X-API-Key': bad} if bad is not None
                else {},
            )
            assert resp.status_code == 401, bad
            assert resp.json() == {'detail': 'invalid or missing API key'}
        assert (await app.client.get(self.PATH, headers=headers)
                ).status_code == 200

    async def test_revoked_key_gets_401_at_once(self, app):
        key, headers = await _issue(app.db, 'sites')
        assert (await app.client.get(self.PATH, headers=headers)
                ).status_code == 200
        await RevokeApiKey.run(app.db, None, key_id=key.key_id)
        assert (await app.client.get(self.PATH, headers=headers)
                ).status_code == 401

    async def test_expired_key_gets_401(self, app):
        key, headers = await _issue(
            app.db, 'sites',
            expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
        )
        assert (await app.client.get(self.PATH, headers=headers)
                ).status_code == 200
        await ApiKey.find_one(ApiKey.key_id == key.key_id).update(
            {'$set': {'expires_at': datetime.now(timezone.utc)
                      - timedelta(seconds=1)}},
        )
        assert (await app.client.get(self.PATH, headers=headers)
                ).status_code == 401

    async def test_static_config_key_has_every_scope(self, app):
        for path in ALL_ROUTES + PUPPET_ROUTES:
            assert (await app.client.get(path, headers=KEY)
                    ).status_code == 200, path

    async def test_no_static_key_does_not_open_the_api(self, beanie_client,
                                                       config, seeded):  # noqa: F811
        """Removing `api_key` from the config must not remove the check."""
        cfg = replace(config, api=replace(config.api, api_key=None))
        key, headers = await _issue(beanie_client, 'sites')
        async with _client(create_api(cfg, client=beanie_client)) as client:
            assert (await client.get(self.PATH)).status_code == 401
            assert (await client.get(self.PATH, headers=KEY)
                    ).status_code == 401
            assert (await client.get(self.PATH, headers=headers)
                    ).status_code == 200

    async def test_no_api_section_does_not_open_the_api(self, beanie_client,
                                                        config, seeded):  # noqa: F811
        cfg = replace(config, api=None)
        async with _client(create_api(cfg, client=beanie_client)) as client:
            assert (await client.get(self.PATH)).status_code == 401

    async def test_allow_anonymous(self, beanie_client, config, seeded):  # noqa: F811
        assert ApiConfig.Schema().load({}).allow_anonymous is False
        cfg = replace(config, api=replace(config.api, api_key=None,
                                          allow_anonymous=True))
        async with _client(create_api(cfg, client=beanie_client)) as client:
            for path in ALL_ROUTES + PUPPET_ROUTES:
                assert (await client.get(path)).status_code == 200, path


class TestScopes:

    async def test_every_route_declares_one_known_scope(self, beanie_client,
                                                        config):
        """Tripwire: a new route must declare a scope, or it refuses every
        request (`authorize` denies a route with no scope)."""
        app = create_api(config, client=beanie_client)
        routes = [r for r in app.routes if isinstance(r, APIRoute)]
        assert len(routes) == len(ALL_ROUTES) + len(PUPPET_ROUTES)
        for route in routes:
            assert len(route.required_scopes) == 1, route.path
            assert route.required_scopes[0] in API_SCOPES, route.path

    async def test_scope_matrix(self, app):
        """A key with only the scope of a route can read it; a key with
        every OTHER scope cannot."""
        only = {scope: (await _issue(app.db, scope))[1]
                for scope in API_SCOPES}
        all_but = {
            scope: (await _issue(
                app.db, *(s for s in API_SCOPES if s != scope),
            ))[1]
            for scope in API_SCOPES
        }
        seen = set()
        for path in ALL_ROUTES + PUPPET_ROUTES:
            scope = expected_scope(path)
            seen.add(scope)
            ok = await app.client.get(path, headers=only[scope])
            assert ok.status_code == 200, (path, scope)
            denied = await app.client.get(path, headers=all_but[scope])
            assert denied.status_code == 403, (path, scope)
            assert denied.json() == {
                'detail': f'API key lacks scope: {scope}',
            }
        assert seen == set(API_SCOPES)

    async def test_wildcard_key(self, app):
        _, headers = await _issue(app.db, 'ALL')
        for path in ALL_ROUTES + PUPPET_ROUTES:
            assert (await app.client.get(path, headers=headers)
                    ).status_code == 200, path

    async def test_scope_is_checked_before_the_route_runs(self, app):
        """A 403 must not depend on the data: an unknown site still gives
        403, not 404, to a key without the scope."""
        _, headers = await _issue(app.db, 'users')
        resp = await app.client.get('/sites/nope/slurm/qos', headers=headers)
        assert resp.status_code == 403

    async def test_route_without_a_scope_refuses(self, beanie_client, config,
                                                 seeded):  # noqa: F811
        app = create_api(config, client=beanie_client)
        router = APIRouter(route_class=CachedRoute)

        @router.get('/forgotten',
                    dependencies=[Security(require_api_key)])
        async def forgotten():
            return {'data': 'secret'}

        app.include_router(router)
        async with _client(app) as client:
            resp = await client.get('/forgotten', headers=KEY)
        assert resp.status_code == 403
        assert 'secret' not in resp.text


class TestKeysAndTheCache:

    @pytest_asyncio.fixture(loop_scope='session')
    async def cached(self, beanie_client, config, seeded):  # noqa: F811
        cfg = replace(config, api=replace(
            config.api, cache=ApiCacheConfig(ttl=60, stale=600),
        ))
        async with _client(create_api(cfg, client=beanie_client)) as client:
            yield client

    async def test_cached_response_needs_the_scope(self, cached,
                                                   beanie_client):
        path = f'/sites/{SITE}/users'
        _, users = await _issue(beanie_client, 'users')
        _, slurm = await _issue(beanie_client, 'slurm')
        first = await cached.get(path, headers=users)
        assert first.headers['x-cache'] == 'miss'
        assert (await cached.get(path, headers=users)
                ).headers['x-cache'] == 'hit'

        denied = await cached.get(path, headers=slurm)
        assert denied.status_code == 403
        assert 'alice' not in denied.text
        assert (await cached.get(path)).status_code == 401

    async def test_revoked_key_loses_a_cached_response_at_once(
        self, cached, beanie_client,
    ):
        path = f'/sites/{SITE}/users'
        key, headers = await _issue(beanie_client, 'users')
        await cached.get(path, headers=headers)
        assert (await cached.get(path, headers=headers)
                ).headers['x-cache'] == 'hit'
        await RevokeApiKey.run(beanie_client, None, key_id=key.key_id)
        resp = await cached.get(path, headers=headers)
        assert resp.status_code == 401
        assert 'alice' not in resp.text

    async def test_hit_costs_one_key_lookup(self, cached, beanie_client):
        path = f'/sites/{SITE}/users'
        _, headers = await _issue(beanie_client, 'users')
        await cached.get(path, headers=headers)
        _counter.count = 0
        _counter.active = True
        try:
            resp = await cached.get(path, headers=headers)
        finally:
            _counter.active = False
        assert resp.headers['x-cache'] == 'hit'
        assert _counter.count == 1          # the ApiKey lookup, nothing else

    async def test_miss_looks_the_key_up_once(self, cached, beanie_client):
        """The route class and the dependency both authorize; the second
        one must reuse the first result."""
        _, headers = await _issue(beanie_client, 'sites')
        plain_key = await self._reads(cached, '/sites', KEY)
        issued_key = await self._reads(cached, f'/sites/{SITE}', headers)
        static_same_route = await self._reads(
            cached, f'/sites/{SITE}', {**KEY, 'Cache-Control': 'no-cache'},
        )
        assert plain_key >= 1
        assert issued_key == static_same_route + 1

    async def _reads(self, client, path, headers) -> int:
        _counter.count = 0
        _counter.active = True
        try:
            resp = await client.get(path, headers=headers)
        finally:
            _counter.active = False
        assert resp.status_code == 200
        return _counter.count

    # -- The cache key must be what the router matched ----------------------
    #
    # `request.url` is rebuilt from the Host header and the percent-DECODED
    # path, then parsed again: a `?` or `#` in either cuts `url.path` short.
    # A key taken from it lets one route (and its scope) answer from, or
    # write to, the entry of another route.

    @pytest.mark.parametrize('host', [
        f'x/sites/{SITE}/users?',
        f'x/sites/{SITE}/users#',
    ])
    async def test_forged_host_cannot_read_another_route(
        self, cached, beanie_client, host,
    ):
        _, users = await _issue(beanie_client, 'users')
        _, sites = await _issue(beanie_client, 'sites')
        warm = await cached.get(f'/sites/{SITE}/users', headers=users)
        assert 'alice' in warm.text

        resp = await cached.get('/sites', headers={**sites, 'Host': host})
        assert resp.status_code == 200
        assert 'alice' not in resp.text
        assert [s['name'] for s in resp.json()] == [SITE, 'othersite']

    async def test_forged_host_cannot_poison_another_route(
        self, cached, beanie_client,
    ):
        _, users = await _issue(beanie_client, 'users')
        _, sites = await _issue(beanie_client, 'sites')
        path = f'/sites/{SITE}/users'
        good = await cached.get(path, headers=users)

        await cached.get('/sites', headers={
            **sites, 'Host': f'x{path}?', 'Cache-Control': 'no-cache',
        })
        after = await cached.get(path, headers=users)
        assert after.headers['x-cache'] == 'hit'
        assert after.content == good.content

    @pytest.mark.parametrize('mark', ['%3F', '%23'])
    async def test_encoded_delimiter_in_path_is_its_own_key(
        self, cached, beanie_client, mark,
    ):
        """`/sites/<site>%3Fx/users` is the users route with the site
        `<site>?x`; it must not answer from the `/sites/<site>` entry."""
        _, users = await _issue(beanie_client, 'users')
        _, sites = await _issue(beanie_client, 'sites')
        detail = await cached.get(f'/sites/{SITE}', headers=sites)
        assert detail.json()['fqdn'] == 'api.hpc.test'

        resp = await cached.get(f'/sites/{SITE}{mark}x/users', headers=users)
        assert resp.status_code == 404
        assert 'api.hpc.test' not in resp.text

    async def test_unknown_site_cannot_poison_the_root_keys_of_a_site(
        self, cached, beanie_client,
    ):
        """`/puppet/root-keys/<unknown>` answers 200 with the GLOBAL key
        set. Asked as `<site>%3Fx`, that body must not land in the entry of
        `<site>`."""
        other = await Site.find_one(Site.name == 'othersite')
        admin = await _user('rootadmin', 73900, other, type='admin',
                            access=('login-ssh', 'root-ssh'))
        await SshKey(key='ssh-ed25519 AAAAroot root@laptop',
                     user=admin).insert()
        _, puppet = await _issue(beanie_client, 'puppet')
        path = f'/puppet/root-keys/{SITE}'

        good = await cached.get(path, headers=puppet)
        assert 'rootadmin' not in good.text      # not an admin of this site
        fallback = await cached.get(f'{path}%3Fx', headers={
            **puppet, 'Cache-Control': 'no-cache',
        })
        assert 'rootadmin' in fallback.text      # the global set

        after = await cached.get(path, headers=puppet)
        assert after.headers['x-cache'] == 'hit'
        assert after.content == good.content
        assert 'rootadmin' not in after.text

    async def test_cache_key_ignores_the_host_header(self, cached,
                                                     beanie_client):
        """One entry per route and parameters, whatever the Host is."""
        _, sites = await _issue(beanie_client, 'sites')
        first = await cached.get('/sites', headers=sites)
        again = await cached.get('/sites',
                                 headers={**sites, 'Host': 'other.test'})
        assert (first.headers['x-cache'], again.headers['x-cache']) == (
            'miss', 'hit',
        )


class TestLastUsed:

    async def test_write_is_limited_per_key(self, app):
        key, headers = await _issue(app.db, 'sites')
        other, other_headers = await _issue(app.db, 'sites')

        await app.client.get('/sites', headers=headers)
        first = (await find_api_key(key.key_id)).last_used_at
        assert first is not None
        assert (await find_api_key(other.key_id)).last_used_at is None

        app.clock.now += KEY_TOUCH_INTERVAL - 1
        await app.client.get('/sites', headers=headers)
        assert (await find_api_key(key.key_id)).last_used_at == first

        # Another key has its own limit.
        await app.client.get('/sites', headers=other_headers)
        assert (await find_api_key(other.key_id)).last_used_at is not None

        app.clock.now += 2
        await app.client.get('/sites', headers=headers)
        assert (await find_api_key(key.key_id)).last_used_at > first

    async def test_refused_requests_do_not_count_as_use(self, app):
        key, headers = await _issue(app.db, 'sites')
        assert (await app.client.get('/users', headers=headers)
                ).status_code == 403
        assert (await find_api_key(key.key_id)).last_used_at is None

    async def test_no_history_rows_from_requests(self, app):
        _, headers = await _issue(app.db, 'ALL')
        before = await History.find_all().count()
        for path in ALL_ROUTES:
            await app.client.get(path, headers=headers)
        assert await History.find_all().count() == before

    async def test_failed_write_does_not_fail_the_request(self, app,
                                                          monkeypatch):
        _, headers = await _issue(app.db, 'sites')

        def broken(*args, **kwargs):
            raise RuntimeError('read-only database user')

        monkeypatch.setattr(ApiKey, 'find_one', broken)
        monkeypatch.setattr(
            'cheeto.daemon.deps.find_api_key',
            lambda key_id: ApiKey.find({'key_id': key_id}).first_or_none(),
        )
        assert (await app.client.get('/sites', headers=headers)
                ).status_code == 200
