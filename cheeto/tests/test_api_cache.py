"""Tests for the API response cache (`cheeto/daemon/cache.py`).

`TestResponseCache` drives the cache alone, with a fake clock and a
counting producer. `TestCachedRoutes` drives it through the app, with the
seeded site of `test_api.py`, and counts the database reads.
"""

import asyncio
from dataclasses import replace
from types import SimpleNamespace

import httpx
import pytest
import pytest_asyncio
from fastapi import HTTPException, Response

from ..config import ApiCacheConfig, ApiConfig
from ..daemon.api import create_api
from ..daemon.cache import ResponseCache
from ..models.group import Group
from ..models.group_membership import GroupMembership
from ..models.user import User
# The fixtures are used by name; pytest finds them in this namespace.
from .test_api import (  # noqa: F401
    ALL_ROUTES,
    KEY,
    SITE,
    _counter,
    api,
    beanie_client,
    clean_db,
    seeded,
)


class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class Producer:
    """Returns a new body on each call and counts the calls. `gate`, when
    set, holds every call until the test releases it."""

    def __init__(self):
        self.calls = 0
        self.gate: asyncio.Event | None = None
        self.error: Exception | None = None
        self.status = 200

    async def __call__(self) -> Response:
        self.calls += 1
        n = self.calls
        if self.gate is not None:
            await self.gate.wait()
        else:
            await asyncio.sleep(0)
        if self.error is not None:
            raise self.error
        return Response(content=f'body-{n}'.encode(),
                        status_code=self.status,
                        media_type='application/json')


@pytest.fixture
def clock():
    return Clock()


@pytest.fixture
def cache(clock):
    return ResponseCache(ttl=60, stale=600, max_entries=3, clock=clock)


class TestResponseCache:

    async def test_miss_then_hit(self, cache):
        produce = Producer()
        entry, state = await cache.get('k', produce)
        assert (entry.body, state) == (b'body-1', 'miss')
        assert entry.content_type == 'application/json'
        again, state = await cache.get('k', produce)
        assert (again, state) == (entry, 'hit')
        assert produce.calls == 1

    async def test_concurrent_cold_calls_share_one_computation(self, cache):
        produce = Producer()
        produce.gate = asyncio.Event()
        calls = [asyncio.create_task(cache.get('k', produce))
                 for _ in range(10)]
        await asyncio.sleep(0)
        produce.gate.set()
        results = await asyncio.gather(*calls)
        assert produce.calls == 1
        assert {entry.body for entry, _ in results} == {b'body-1'}
        assert {state for _, state in results} == {'miss'}

    async def test_keys_are_independent(self, cache):
        produce = Producer()
        a, _ = await cache.get('a', produce)
        b, _ = await cache.get('b', produce)
        assert (a.body, b.body) == (b'body-1', b'body-2')

    async def test_stale_is_served_at_once_and_refreshed_once(self, cache,
                                                             clock):
        produce = Producer()
        await cache.get('k', produce)
        clock.advance(61)

        produce.gate = asyncio.Event()      # the refresh cannot finish yet
        for _ in range(5):
            entry, state = await cache.get('k', produce)
            assert (entry.body, state) == (b'body-1', 'stale')
        await asyncio.sleep(0)
        assert produce.calls == 2           # one refresh for five requests

        produce.gate.set()
        await cache.drain()
        entry, state = await cache.get('k', produce)
        assert (entry.body, state) == (b'body-2', 'hit')
        assert produce.calls == 2

    async def test_failed_refresh_keeps_the_old_entry(self, cache, clock):
        produce = Producer()
        await cache.get('k', produce)
        clock.advance(61)
        produce.error = RuntimeError('database is down')
        entry, state = await cache.get('k', produce)
        assert (entry.body, state) == (b'body-1', 'stale')
        await cache.drain()
        # Still there, still stale; the next request tries again.
        entry, state = await cache.get('k', produce)
        assert (entry.body, state) == (b'body-1', 'stale')
        await cache.drain()
        assert produce.calls == 3

        produce.error = None
        await cache.get('k', produce)
        await cache.drain()
        entry, state = await cache.get('k', produce)
        assert (entry.body, state) == (b'body-4', 'hit')

    async def test_refresh_that_is_not_a_200_drops_the_entry(self, cache,
                                                            clock):
        """A 404 at refresh time means the old entry is wrong, not stale."""
        produce = Producer()
        await cache.get('k', produce)
        clock.advance(61)
        produce.error = HTTPException(status_code=404)
        _, state = await cache.get('k', produce)
        assert state == 'stale'
        await cache.drain()
        assert len(cache) == 0
        with pytest.raises(HTTPException):
            await cache.get('k', produce)

    async def test_expired_entry_is_recomputed(self, cache, clock):
        produce = Producer()
        await cache.get('k', produce)
        clock.advance(60 + 600 + 1)
        entry, state = await cache.get('k', produce)
        assert (entry.body, state) == (b'body-2', 'miss')

    async def test_errors_and_other_statuses_are_not_stored(self, cache):
        produce = Producer()
        produce.error = HTTPException(status_code=404)
        with pytest.raises(HTTPException):
            await cache.get('k', produce)
        produce.error = None
        produce.status = 503
        response, state = await cache.get('k', produce)
        assert isinstance(response, Response)
        assert (response.status_code, state) == (503, 'miss')
        assert len(cache) == 0
        assert produce.calls == 2

    async def test_force_recomputes(self, cache):
        produce = Producer()
        await cache.get('k', produce)
        entry, state = await cache.get('k', produce, force=True)
        assert (entry.body, state) == (b'body-2', 'miss')
        entry, state = await cache.get('k', produce)
        assert (entry.body, state) == (b'body-2', 'hit')

    async def test_lru_bound(self, cache):
        produce = Producer()
        for key in 'abc':
            await cache.get(key, produce)
        await cache.get('a', produce)       # 'a' is now the newest
        await cache.get('d', produce)       # drops 'b', the oldest
        assert len(cache) == 3
        calls = produce.calls
        await cache.get('a', produce)
        assert produce.calls == calls
        await cache.get('b', produce)
        assert produce.calls == calls + 1

    async def test_cancelled_request_does_not_cancel_the_computation(
        self, cache,
    ):
        produce = Producer()
        produce.gate = asyncio.Event()
        first = asyncio.create_task(cache.get('k', produce))
        second = asyncio.create_task(cache.get('k', produce))
        await asyncio.sleep(0)
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        produce.gate.set()
        entry, _ = await second
        assert entry.body == b'body-1'
        entry, state = await cache.get('k', produce)
        assert (entry.body, state) == (b'body-1', 'hit')
        assert produce.calls == 1

    async def test_etag_follows_the_body(self, cache):
        produce = Producer()
        first, _ = await cache.get('a', produce)
        second, _ = await cache.get('b', produce)
        assert first.etag != second.etag
        assert first.etag.startswith('"') and first.etag.endswith('"')


class TestCacheConfig:

    def test_defaults(self):
        api = ApiConfig.Schema().load({'cache': {}})
        assert (api.cache.ttl, api.cache.stale, api.cache.max_entries) == (
            60, 600, 4096,
        )
        api = ApiConfig.Schema().load({'cache': {'ttl': 5, 'stale': 0}})
        assert (api.cache.ttl, api.cache.stale) == (5, 0)

    def test_no_block_means_no_cache(self, config):
        assert config.api.cache is None
        assert create_api(config, client=object()).state.cache is None

    def test_block_builds_the_cache(self, config):
        cfg = replace(config, api=replace(
            config.api, cache=ApiCacheConfig(ttl=5, stale=7, max_entries=9),
        ))
        cache = create_api(cfg, client=object()).state.cache
        assert (cache.ttl, cache.stale, cache.max_entries) == (5, 7, 9)


@pytest_asyncio.fixture(loop_scope='session')
async def cached(beanie_client, config, seeded):  # noqa: F811
    """The app with the cache on and a fake clock."""
    cfg = replace(config, api=replace(
        config.api, cache=ApiCacheConfig(ttl=60, stale=600),
    ))
    app = create_api(cfg, client=beanie_client)
    clock = Clock()
    app.state.cache.clock = clock
    client = httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                               base_url='http://test', headers=KEY)
    async with client:
        yield SimpleNamespace(client=client, clock=clock,
                              cache=app.state.cache)
    await app.state.cache.drain()


async def _counted(client, path, **kwargs):
    """GET `path`; returns (response, database reads)."""
    _counter.count = 0
    _counter.active = True
    try:
        resp = await client.get(path, **kwargs)
    finally:
        _counter.active = False
    return resp, _counter.count


class TestCachedRoutes:

    async def test_second_request_reads_nothing(self, cached):
        path = f'/sites/{SITE}/users'
        first, reads = await _counted(cached.client, path)
        assert first.status_code == 200
        assert first.headers['x-cache'] == 'miss'
        assert reads > 0

        cached.clock.advance(10)
        second, reads = await _counted(cached.client, path)
        assert reads == 0
        assert second.headers['x-cache'] == 'hit'
        assert second.content == first.content
        assert second.headers['content-type'] == first.headers['content-type']
        assert second.headers['etag'] == first.headers['etag']
        assert second.headers['age'] == '10'
        assert second.headers['cache-control'] == 'private, max-age=50'

    async def test_concurrent_requests_share_one_computation(self, cached):
        path = f'/sites/{SITE}/users'
        _, single = await _counted(cached.client, path,
                                   headers={'Cache-Control': 'no-cache'})
        cached.cache.clear()

        _counter.count = 0
        _counter.active = True
        try:
            responses = await asyncio.gather(*(
                cached.client.get(path) for _ in range(8)
            ))
        finally:
            _counter.active = False
        assert {r.status_code for r in responses} == {200}
        assert len({r.content for r in responses}) == 1
        assert _counter.count == single

    async def test_every_route_gives_the_same_body(self, cached, api):  # noqa: F811
        for path in ALL_ROUTES + (f'/puppet/root-keys/{SITE}',
                                  f'/puppet/storage/{SITE}'):
            plain = await api.get(path)
            miss = await cached.client.get(path)
            hit = await cached.client.get(path)
            assert plain.status_code == 200, path
            assert 'x-cache' not in plain.headers, path
            assert (miss.headers['x-cache'], hit.headers['x-cache']) == (
                'miss', 'hit',
            ), path
            assert miss.json() == plain.json(), path
            assert hit.content == miss.content, path

    async def test_query_string_is_part_of_the_key(self, cached):
        everyone = await cached.client.get('/users')
        at_site = await cached.client.get(f'/users?site={SITE}')
        assert at_site.headers['x-cache'] == 'miss'
        assert at_site.json() != everyone.json()
        # The order of the parameters does not matter.
        a = await cached.client.get(f'/users?site={SITE}&access=slurm')
        b = await cached.client.get(f'/users?access=slurm&site={SITE}')
        assert (a.headers['x-cache'], b.headers['x-cache']) == ('miss', 'hit')

    async def test_api_key_is_checked_on_a_cached_route(self, cached):
        path = f'/sites/{SITE}/users'
        assert (await cached.client.get(path)).status_code == 200
        for headers in ({'X-API-Key': 'wrong'}, {'X-API-Key': ''}):
            resp, reads = await _counted(cached.client, path, headers=headers)
            assert resp.status_code == 401
            assert reads == 0
            assert 'alice' not in resp.text

    async def test_404_is_not_stored(self, cached):
        for _ in range(2):
            resp, reads = await _counted(cached.client, '/users/nope')
            assert resp.status_code == 404
            assert reads > 0
        assert len(cached.cache) == 0

    async def test_if_none_match_gives_304(self, cached):
        path = f'/sites/{SITE}/groups'
        first = await cached.client.get(path)
        etag = first.headers['etag']
        resp, reads = await _counted(cached.client, path,
                                     headers={'If-None-Match': etag})
        assert resp.status_code == 304
        assert resp.content == b''
        assert resp.headers['etag'] == etag
        assert reads == 0
        other = await cached.client.get(path,
                                        headers={'If-None-Match': '"other"'})
        assert other.status_code == 200
        assert other.content == first.content

    async def test_no_cache_request_recomputes(self, cached):
        path = '/users/alice'
        await cached.client.get(path)
        await User.find_one(User.name == 'alice').update(
            {'$set': {'fullname': 'Changed'}},
        )
        assert (await cached.client.get(path)).json()['fullname'] == 'Alice'
        resp, reads = await _counted(cached.client, path,
                                     headers={'Cache-Control': 'no-cache'})
        assert resp.headers['x-cache'] == 'miss'
        assert reads > 0
        assert resp.json()['fullname'] == 'Changed'
        assert (await cached.client.get(path)).json()['fullname'] == 'Changed'

    async def test_stale_then_refreshed(self, cached):
        """Past the ttl the old body comes back at once, with no database
        read in the request, and a background task puts the new body in
        place."""
        path = '/users/alice'
        old = await cached.client.get(path)
        await User.find_one(User.name == 'alice').update(
            {'$set': {'fullname': 'Changed'}},
        )

        cached.clock.advance(61)
        stale = await cached.client.get(path)
        assert stale.headers['x-cache'] == 'stale'
        assert stale.content == old.content
        assert stale.headers['cache-control'] == 'private, max-age=0'
        await cached.cache.drain()

        fresh, reads = await _counted(cached.client, path)
        assert reads == 0
        assert fresh.headers['x-cache'] == 'hit'
        assert fresh.json()['fullname'] == 'Changed'
        assert fresh.headers['etag'] != stale.headers['etag']
        assert fresh.headers['age'] == '0'

    async def test_detached_refresh_resolves_path_dependencies(self, cached):
        """The refresh runs the route handler after its request ended.
        The chained dependencies (user, site, user at site) must still
        resolve there."""
        path = f'/users/alice/sites/{SITE}/groups'
        first = await cached.client.get(path)
        assert [g['name'] for g in first.json()] == ['everyone', 'lab']
        lab = await Group.find_one(Group.name == 'lab')
        await GroupMembership.find(
            GroupMembership.group.id == lab.id,
        ).delete()

        cached.clock.advance(61)
        stale = await cached.client.get(path)
        assert stale.headers['x-cache'] == 'stale'
        assert stale.content == first.content
        await cached.cache.drain()
        fresh = await cached.client.get(path)
        assert fresh.headers['x-cache'] == 'hit'
        assert [g['name'] for g in fresh.json()] == ['everyone']

    async def test_deleted_record_stops_being_served_after_a_refresh(
        self, cached,
    ):
        path = '/users/dave'
        assert (await cached.client.get(path)).status_code == 200
        await User.find_one(User.name == 'dave').delete()

        cached.clock.advance(61)
        stale = await cached.client.get(path)
        assert (stale.status_code, stale.headers['x-cache']) == (200, 'stale')
        await cached.cache.drain()
        assert (await cached.client.get(path)).status_code == 404

    async def test_expired_entry_waits_for_a_new_response(self, cached):
        path = '/users/alice'
        await cached.client.get(path)
        cached.clock.advance(60 + 600 + 1)
        resp, reads = await _counted(cached.client, path)
        assert resp.headers['x-cache'] == 'miss'
        assert reads > 0
