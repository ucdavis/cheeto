"""Regression tests for the shared HiPPO client factory (cheeto.hippo).

The generated AuthenticatedClient builds BOTH a sync and an async httpx client
from one shared ``httpx_args``, and the async client *awaits* its event hooks.
A plain (sync) hook therefore breaks every async request with
``TypeError: object NoneType can't be used in 'await' expression`` — these
tests guard against re-introducing one.
"""

import inspect

import httpx

from ..config import HippoConfig
from ..hippo import hippoapi_client


def _cfg() -> HippoConfig:
    return HippoConfig(api_key='test-key', base_url='https://hippo.test',
                       site_aliases={}, max_tries=1)


async def test_async_event_hooks_are_coroutine_functions():
    client = hippoapi_client(_cfg(), quiet=True)
    async_client = client.get_async_httpx_client()
    try:
        for phase, hooks in async_client.event_hooks.items():
            for hook in hooks:
                assert inspect.iscoroutinefunction(hook), (
                    f'async HiPPO client {phase} hook {hook!r} must be a '
                    'coroutine function or httpx fails awaiting it'
                )
    finally:
        await async_client.aclose()


async def test_async_request_succeeds_through_mock_transport():
    """End-to-end: an async request through the factory-built client must not
    raise. Swaps in a MockTransport so no network is hit; this is the exact
    path (`get_async_httpx_client().request`) that failed in `ng hippo events`.
    """
    client = hippoapi_client(_cfg(), quiet=True)
    async_client = client.get_async_httpx_client()
    async_client._transport = httpx.MockTransport(
        lambda request: httpx.Response(200, json=[]),
    )
    try:
        resp = await async_client.get('/api/PendingEvents')
        assert resp.status_code == 200
    finally:
        await async_client.aclose()
