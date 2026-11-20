"""In-process response cache for the read-only REST API.

The API is one uvicorn process on one event loop, and its slow routes are
CPU work in that loop: without a cache, N concurrent requests for a slow
route take N times as long and delay every other route. The data changes
rarely and only in OTHER processes (CLI, celery workers), so entries expire
by time; nothing in this process can know about a write.

`ResponseCache` stores the finished response bytes and gives three
guarantees:

- single-flight: concurrent requests for one key share one computation;
- stale-while-revalidate: an entry older than `ttl` is still served, at
  once, for `stale` more seconds while ONE background task replaces it;
- stale-if-error: a refresh that fails keeps the old entry.

`CachedRoute` applies it to every route of a router, so the handlers do
not change. The cache is per app instance (`app.state.cache`); None turns
it off.
"""

import asyncio
import hashlib
import logging
import time
from collections import OrderedDict
from collections.abc import Awaitable, Callable, Hashable
from dataclasses import dataclass
from typing import Literal

from fastapi import HTTPException, Request, Response
from fastapi.routing import APIRoute

from .deps import check_api_key

logger = logging.getLogger(__name__)

CacheState = Literal['hit', 'stale', 'miss']


@dataclass(frozen=True)
class CacheEntry:
    body: bytes
    content_type: str
    etag: str
    born: float     # value of the cache clock when the entry was stored


class ResponseCache:
    """Single-flight LRU of `200` responses with stale-while-revalidate.

    Only for a single event loop: no locks, the dict operations between
    two awaits are atomic. `clock` is injectable so tests control time.
    """

    def __init__(self, ttl: float = 60, stale: float = 600,
                 max_entries: int = 4096,
                 clock: Callable[[], float] = time.monotonic):
        self.ttl = ttl
        self.stale = stale
        self.max_entries = max_entries
        self.clock = clock
        self._entries: OrderedDict[Hashable, CacheEntry] = OrderedDict()
        # Also the strong reference that keeps a background task alive.
        self._inflight: dict[Hashable, asyncio.Task] = {}

    def __len__(self) -> int:
        return len(self._entries)

    def age(self, entry: CacheEntry) -> float:
        return self.clock() - entry.born

    async def get(
        self, key: Hashable,
        produce: Callable[[], Awaitable[Response]],
        *, force: bool = False,
    ) -> tuple[CacheEntry | Response, CacheState]:
        """The entry for `key`, computed with `produce` when needed.

        `produce` returns the response of the route. A `200` is stored and
        comes back as a `CacheEntry`; any other response comes back as it
        is and is not stored. An exception from `produce` (a 404
        `HTTPException` included) reaches every request that waits on it.
        `force` skips the stored entry; it still joins a computation that
        is already in flight.
        """
        entry = None if force else self._entries.get(key)
        if entry is not None:
            age = self.age(entry)
            if age < self.ttl:
                self._entries.move_to_end(key)
                return entry, 'hit'
            if age < self.ttl + self.stale:
                self._entries.move_to_end(key)
                self._start(key, produce, background=True)
                return entry, 'stale'
            del self._entries[key]

        # shield: a client that disconnects must not cancel the computation
        # other requests wait on (and that fills the cache).
        task = self._start(key, produce, background=False)
        return await asyncio.shield(task), 'miss'

    def _start(self, key, produce, *, background: bool) -> asyncio.Task:
        task = self._inflight.get(key)
        if task is None:
            task = asyncio.create_task(self._run(key, produce, background))
            self._inflight[key] = task
            task.add_done_callback(lambda t: self._finish(key, t))
        return task

    def _finish(self, key, task: asyncio.Task) -> None:
        if self._inflight.get(key) is task:
            del self._inflight[key]
        # Mark the exception as retrieved; a background refresh has no
        # waiter, and `_run` already logged it.
        if not task.cancelled():
            task.exception()

    async def _run(self, key, produce, background: bool):
        try:
            response = await produce()
        except HTTPException:
            # The route answered, and the answer is not a 200 (the user was
            # deleted, for example). The old entry is wrong, not stale.
            self._entries.pop(key, None)
            raise
        except Exception:
            if background:
                # stale-if-error: the old entry stays until ttl + stale.
                logger.warning('cache refresh failed for %r', key,
                               exc_info=True)
            raise
        body = getattr(response, 'body', None)
        if response.status_code != 200 or body is None:
            self._entries.pop(key, None)
            return response
        entry = CacheEntry(
            body=bytes(body),
            content_type=response.headers.get('content-type',
                                              'application/json'),
            etag='"' + hashlib.blake2b(body, digest_size=16).hexdigest() + '"',
            born=self.clock(),
        )
        self._entries[key] = entry
        self._entries.move_to_end(key)
        while len(self._entries) > self.max_entries:
            self._entries.popitem(last=False)
        return entry

    def clear(self) -> None:
        self._entries.clear()

    async def drain(self) -> None:
        """Wait until no computation is in flight (errors are ignored).
        For shutdown, and for tests that must see a background refresh."""
        while self._inflight:
            await asyncio.gather(*self._inflight.values(),
                                 return_exceptions=True)
            # Let the done callbacks remove the finished tasks.
            await asyncio.sleep(0)


def _etag_matches(header: str, etag: str) -> bool:
    if header.strip() == '*':
        return True
    return etag in (
        candidate.strip().removeprefix('W/')
        for candidate in header.split(',')
    )


def _cached_response(request: Request, cache: ResponseCache,
                     entry: CacheEntry, state: CacheState) -> Response:
    age = max(0.0, cache.age(entry))
    headers = {
        'ETag': entry.etag,
        # private: the response depends on the API key, so a shared proxy
        # must not store it. max-age: how long the entry stays fresh here.
        'Cache-Control': f'private, max-age={max(0, int(cache.ttl - age))}',
        'Age': str(int(age)),
        'X-Cache': state,
    }
    if _etag_matches(request.headers.get('if-none-match', ''), entry.etag):
        return Response(status_code=304, headers=headers)
    headers['Content-Type'] = entry.content_type
    return Response(content=entry.body, headers=headers)


class CachedRoute(APIRoute):
    """An `APIRoute` whose `GET` responses go through `app.state.cache`.

    Use it as `APIRouter(route_class=CachedRoute)`. The key is the path
    plus the sorted query parameters. The API key is checked on every
    request, before the lookup: a stored response is never an answer to a
    request the route would refuse. A request with `Cache-Control:
    no-cache` computes a new response.

    The wrapped handler can run in a task that outlives the request (the
    background refresh). That is safe here: FastAPI's handler opens its own
    exit stack, a GET reads no body, and no dependency of the API uses
    `yield`. Keep it that way, or compute such a route outside the cache.
    """

    def get_route_handler(self):
        handler = super().get_route_handler()

        async def cached_handler(request: Request) -> Response:
            cache = getattr(request.app.state, 'cache', None)
            if cache is None or request.method != 'GET':
                return await handler(request)

            check_api_key(request)
            key = (
                request.url.path,
                tuple(sorted(request.query_params.multi_items())),
            )
            force = 'no-cache' in request.headers.get('cache-control',
                                                      '').lower()
            result, state = await cache.get(
                key, lambda: handler(request), force=force,
            )
            if isinstance(result, Response):
                return result
            return _cached_response(request, cache, result, state)

        return cached_handler
