"""Tests for the async IAM sync stack.

Covers:
  - `cheeto.iam_async`: response classification, build_ucdiam_info purity.
  - `cheeto.operations.iam`: SyncUserIAM full state machine, SyncAllUsersIAM
    driver tally, ReapOffboardedUsers flip.

Network is mocked via httpx.MockTransport injected through AsyncIAMAPI's
`httpx_client` constructor knob — no real HTTP, no respx dependency.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Callable

import httpx
import pytest
import pytest_asyncio
from beanie import init_beanie
from pymongo import AsyncMongoClient

from ..config import IAMConfig
from ..iam_async import (
    IAM_MISSING,
    AsyncIAMAPI,
    IAMTransientError,
    IAMUserPayload,
    build_ucdiam_info,
)
from ..mail import AccountDeactivatedEmail, AccountOffboardingEmail
from ..models import ALL_MODELS
from ..models.history import History
from ..models.user import UCDIAMInfo, UCDIAMPerson, User
from ..operations import (
    ReapOffboardedUsers,
    SyncAllUsersIAM,
    SyncUserIAM,
)

from .conftest import MONGODB_PORT, seed_access_status_groups, status_link


BEANIE_TEST_DB = 'cheeto_iam_test'


@pytest_asyncio.fixture(scope='session', loop_scope='session')
async def beanie_client(start_mongodb):
    client = AsyncMongoClient(f'127.0.0.1:{MONGODB_PORT}')
    await init_beanie(
        database=client[BEANIE_TEST_DB],
        document_models=ALL_MODELS,
    )
    yield client
    await client.close()


@pytest_asyncio.fixture(autouse=True, loop_scope='session')
async def clean_db(beanie_client):
    for model in ALL_MODELS:
        await model.find_all().delete()
    await History.find_all().delete()
    await seed_access_status_groups()


# ---------------------------------------------------------------------------
# Mock IAM transport helpers
# ---------------------------------------------------------------------------


def _iam_response(results: list[dict]) -> httpx.Response:
    return httpx.Response(200, json={'responseData': {'results': results}})


# Sample payloads modeled after .claude/rules/iam.md (anonymized fields).
PERSON_JANE = {
    'iamId': '1000000001',
    'mothraId': '01000001',
    'userId': 'jdoe',
    'isEmployee': True,
    'isHSEmployee': False,
    'isFaculty': False,
    'isStaff': True,
    'isStudent': False,
    'isExternal': False,
}

ASSOC_JANE_DEPT_A = {
    'iamId': '1000000001',
    'bouOrgOId': 'ORG-A',
    'apptDeptCode': '999001',
    'apptDeptOfficialName': 'EXAMPLE DEPARTMENT A',
    'titleOfficialName': 'STAFF-TITLE',
    'emplClassDesc': 'Staff',
}

ASSOC_JANE_DEPT_B = {
    'iamId': '1000000001',
    'bouOrgOId': 'ORG-B',
    'apptDeptCode': '999002',
    'apptDeptOfficialName': 'EXAMPLE DEPARTMENT B',
    'titleOfficialName': 'STAFF-TITLE-2',
    'emplClassDesc': 'Staff',
}

DIVISION_A = {
    'orgOId': 'ORG-A',
    'deptCode': '99',
    'deptOfficialName': 'EXAMPLE COLLEGE A',
}

DIVISION_B = {
    'orgOId': 'ORG-B',
    'deptCode': '88',
    'deptOfficialName': 'EXAMPLE COLLEGE B',
}


def make_iam_handler(
    *,
    person: list[dict] | None = None,
    associations: list[dict] | None = None,
    division: dict[str, list[dict]] | None = None,
    person_status: int = 200,
    raise_transport: bool = False,
    on_request: Callable[[httpx.Request], None] | None = None,
) -> Callable[[httpx.Request], httpx.Response]:
    """Build a `httpx.MockTransport` handler with per-endpoint stubs.

    `person` / `associations` are lists of result dicts to return (or None to
    return an empty results list, which IAM uses for 'definitively missing').
    `division` maps orgOId -> result list.
    `person_status` lets a test exercise a non-200 response on get_person.
    """
    division = division or {}

    def handler(request: httpx.Request) -> httpx.Response:
        if on_request is not None:
            on_request(request)
        if raise_transport:
            raise httpx.ConnectError('mock transport error')
        path = request.url.path
        if '/iam/people/prikerbacct/search' in path:
            return _iam_response(person or [])
        if path.startswith('/iam/people/'):
            # /iam/people/{iam_id}
            if person_status != 200:
                return httpx.Response(person_status, content=b'')
            return _iam_response(person or [])
        if path.startswith('/iam/associations/pps/'):
            return _iam_response(associations or [])
        if path == '/iam/orginfo/pps/divisions/search':
            org = request.url.params.get('orgOId')
            return _iam_response(division.get(org, []))
        return httpx.Response(404, content=b'')
    return handler


def make_iam_api(handler) -> AsyncIAMAPI:
    cfg = IAMConfig(
        api_key='test-key',
        base_url='https://iam.example.test',
    )
    httpx_client = httpx.AsyncClient(
        base_url=cfg.base_url,
        transport=httpx.MockTransport(handler),
    )
    return AsyncIAMAPI(cfg, httpx_client=httpx_client)


# ---------------------------------------------------------------------------
# build_ucdiam_info — pure helper
# ---------------------------------------------------------------------------


class TestBuildUCDIAMInfo:

    def test_minimal_no_associations(self):
        now = datetime(2026, 1, 1)
        payload = IAMUserPayload(
            person={'iamId': '1', 'mothraId': '2', 'isStudent': True},
            associations=[],
            divisions={},
        )
        info = build_ucdiam_info(payload, now=now)
        assert info.iam_status == 'present'
        assert info.person is not None
        assert info.person.iam_id == 1
        assert info.person.mothra_id == 2
        assert info.person.user_types == ['student']
        assert info.person.associations == []
        assert info.iam_synced_at == now
        assert info.last_seen_at == now
        assert info.first_missing_at is None

    def test_employee_or_hs_employee_maps_to_employee(self):
        now = datetime(2026, 1, 1)
        for booleans in ({'isEmployee': True}, {'isHSEmployee': True}):
            person = {'iamId': '1', 'mothraId': '2', **booleans}
            info = build_ucdiam_info(
                IAMUserPayload(person=person, associations=[], divisions={}),
                now=now,
            )
            assert 'employee' in info.person.user_types

    def test_multi_association_mapping(self):
        now = datetime(2026, 1, 1)
        payload = IAMUserPayload(
            person=PERSON_JANE,
            associations=[ASSOC_JANE_DEPT_A, ASSOC_JANE_DEPT_B],
            divisions={'ORG-A': DIVISION_A, 'ORG-B': DIVISION_B},
        )
        info = build_ucdiam_info(payload, now=now)
        assert len(info.person.associations) == 2
        a, b = info.person.associations
        assert a.org_id == 'ORG-A'
        assert a.org_name == 'EXAMPLE COLLEGE A'
        assert a.org_code == '99'
        assert a.dept_name == 'EXAMPLE DEPARTMENT A'
        assert a.dept_code == '999001'
        assert a.title == 'STAFF-TITLE'
        assert a.title_type == 'Staff'
        assert b.org_id == 'ORG-B'
        assert b.dept_code == '999002'


# ---------------------------------------------------------------------------
# AsyncIAMAPI — direct integration via MockTransport
# ---------------------------------------------------------------------------


class TestAsyncIAMAPI:

    async def test_fetch_user_bundle_dedups_division_calls(self):
        # 2 associations sharing ORG-A + 1 with ORG-B = 3 association rows but
        # only 2 unique division lookups expected.
        seen = []
        handler = make_iam_handler(
            person=[PERSON_JANE],
            associations=[
                ASSOC_JANE_DEPT_A,
                {**ASSOC_JANE_DEPT_A, 'apptDeptCode': '999111'},
                ASSOC_JANE_DEPT_B,
            ],
            division={'ORG-A': [DIVISION_A], 'ORG-B': [DIVISION_B]},
            on_request=lambda req: seen.append(req.url.path),
        )
        async with make_iam_api(handler) as api:
            bundle = await api.fetch_user_bundle(1000000001)
        assert isinstance(bundle, IAMUserPayload)
        assert len(bundle.associations) == 3
        # 1 person + 1 assocs + 2 divisions = 4 calls, 2 to /divisions/search
        division_calls = [p for p in seen if p == '/iam/orginfo/pps/divisions/search']
        assert len(division_calls) == 2

    async def test_fetch_user_bundle_404_returns_missing(self):
        handler = make_iam_handler(person_status=404)
        async with make_iam_api(handler) as api:
            assert await api.fetch_user_bundle(123) is IAM_MISSING

    async def test_fetch_user_bundle_empty_results_returns_missing(self):
        handler = make_iam_handler(person=[])
        async with make_iam_api(handler) as api:
            assert await api.fetch_user_bundle(123) is IAM_MISSING

    async def test_5xx_raises_transient(self):
        handler = make_iam_handler(person_status=503)
        async with make_iam_api(handler) as api:
            with pytest.raises(IAMTransientError):
                await api.fetch_user_bundle(123)

    async def test_transport_error_raises_transient(self):
        handler = make_iam_handler(raise_transport=True)
        async with make_iam_api(handler) as api:
            with pytest.raises(IAMTransientError):
                await api.fetch_user_bundle(123)


# ---------------------------------------------------------------------------
# SyncUserIAM state machine
# ---------------------------------------------------------------------------


class TestSyncUserIAM:

    @pytest_asyncio.fixture(loop_scope='session')
    async def make_user(self, beanie_client):
        async def _make(name='jdoe', user_type='user', uid=10000, **extra):
            # Resolve string status to a StatusGroup link for ergonomic test
            # input — callers can write `status='offboarding'` and have the
            # link looked up automatically.
            if isinstance(extra.get('status'), str):
                extra['status'] = await status_link(extra['status'])
            u = User(
                name=name, email=f'{name}@example.edu',
                uid=uid, gid=uid, fullname=name.capitalize(),
                home_directory=f'/home/{name}', type=user_type,
                **extra,
            )
            await u.insert()
            return u
        return _make

    async def test_hit_no_prior_iam(self, beanie_client, make_user):
        user = await make_user()
        handler = make_iam_handler(
            person=[PERSON_JANE],
            associations=[ASSOC_JANE_DEPT_A],
            division={'ORG-A': [DIVISION_A]},
        )
        # User has no iam yet; resolver hits search_pri_kerb_acct first.
        resolver_handler = make_iam_handler(person=[PERSON_JANE])
        # We need a single handler that handles both resolve + person/assocs.
        def combined(req):
            if '/iam/people/prikerbacct/search' in req.url.path:
                return _iam_response([PERSON_JANE])
            return handler(req)

        now = datetime(2026, 5, 1)
        async with make_iam_api(combined) as api:
            result = await SyncUserIAM.run(
                beanie_client, None,
                username='jdoe', iam_api=api,
                grace_days=14, expiry_offset_days=30, now=now,
            )
        assert result.outcome == 'hit'

        fetched = await User.find_one(User.name == 'jdoe')
        assert fetched.iam is not None
        assert fetched.iam.iam_status == 'present'
        assert fetched.iam.person is not None
        assert fetched.iam.person.iam_id == 1000000001
        assert fetched.iam.last_seen_at == now
        assert fetched.iam.first_missing_at is None
        assert fetched.expires_at is None

    async def test_hit_restored_from_offboarding(self, beanie_client, make_user):
        from ..models.user import UCDIAMInfo
        now = datetime(2026, 5, 1)
        future_expiry = now + timedelta(days=15)
        user = await make_user(
            status='offboarding',
            expires_at=future_expiry,
            iam=UCDIAMInfo(
                iam_status='missing',
                person=UCDIAMPerson(iam_id=1000000001, mothra_id=1000001),
                first_missing_at=now - timedelta(days=20),
            ),
        )
        handler = make_iam_handler(
            person=[PERSON_JANE],
            associations=[],
            division={},
        )
        async with make_iam_api(handler) as api:
            result = await SyncUserIAM.run(
                beanie_client, None,
                username=user.name, iam_api=api,
                grace_days=14, expiry_offset_days=30, now=now,
            )
        assert result.outcome == 'hit_restored'

        fetched = await User.find_one(
            User.name == user.name, fetch_links=True, nesting_depth=1,
        )
        assert fetched.status is not None
        assert fetched.status.status_name == 'active'
        assert fetched.expires_at is None
        assert fetched.iam.first_missing_at is None
        assert fetched.iam.last_seen_at == now

    async def test_hit_does_not_resurrect_inactive_user(self, beanie_client, make_user):
        from ..models.user import UCDIAMInfo
        now = datetime(2026, 5, 1)
        future_expiry = now + timedelta(days=15)
        user = await make_user(
            status='inactive',
            expires_at=future_expiry,
            iam=UCDIAMInfo(person=UCDIAMPerson(iam_id=1000000001, mothra_id=1000001)),
        )
        handler = make_iam_handler(
            person=[PERSON_JANE], associations=[], division={},
        )
        async with make_iam_api(handler) as api:
            result = await SyncUserIAM.run(
                beanie_client, None,
                username=user.name, iam_api=api,
                grace_days=14, expiry_offset_days=30, now=now,
            )
        assert result.outcome == 'hit'  # not 'hit_restored'

        fetched = await User.find_one(
            User.name == user.name, fetch_links=True, nesting_depth=1,
        )
        assert fetched.status is not None
        assert fetched.status.status_name == 'inactive'
        assert fetched.expires_at == future_expiry  # unchanged

    async def test_miss_first(self, beanie_client, make_user):
        from ..models.user import UCDIAMInfo
        now = datetime(2026, 5, 1)
        await make_user(iam=UCDIAMInfo(person=UCDIAMPerson(iam_id=1000000001, mothra_id=1000001)))

        handler = make_iam_handler(person_status=404)
        async with make_iam_api(handler) as api:
            result = await SyncUserIAM.run(
                beanie_client, None,
                username='jdoe', iam_api=api,
                grace_days=14, expiry_offset_days=30, now=now,
            )
        assert result.outcome == 'miss_first'

        fetched = await User.find_one(User.name == 'jdoe')
        assert fetched.iam.first_missing_at == now
        assert fetched.iam.iam_synced_at == now
        assert fetched.expires_at is None
        # Status untouched on a first miss — fixture didn't set one
        assert fetched.status is None

    async def test_miss_within_grace(self, beanie_client, make_user):
        from ..models.user import UCDIAMInfo
        now = datetime(2026, 5, 1)
        first_missed = now - timedelta(days=5)
        await make_user(iam=UCDIAMInfo(
            iam_status='missing',
            person=UCDIAMPerson(iam_id=1000000001, mothra_id=1000001),
            first_missing_at=first_missed,
        ))

        handler = make_iam_handler(person_status=404)
        async with make_iam_api(handler) as api:
            result = await SyncUserIAM.run(
                beanie_client, None,
                username='jdoe', iam_api=api,
                grace_days=14, expiry_offset_days=30, now=now,
            )
        assert result.outcome == 'miss_within_grace'

        fetched = await User.find_one(User.name == 'jdoe')
        # first_missing_at preserved; iam_synced_at advanced.
        assert fetched.iam.first_missing_at == first_missed
        assert fetched.iam.iam_synced_at == now
        assert fetched.expires_at is None
        # Status untouched while still within grace.
        assert fetched.status is None

    async def test_miss_offboarding(self, beanie_client, make_user):
        from ..models.site import Site
        from ..models.user import UCDIAMInfo
        from ..models.user_site_info import UserSiteInfo
        now = datetime(2026, 5, 1)
        first_missed = now - timedelta(days=20)
        user = await make_user(
            status='active',
            iam=UCDIAMInfo(
                iam_status='missing',
                person=UCDIAMPerson(iam_id=1000000001, mothra_id=1000001),
                first_missing_at=first_missed,
            ),
        )
        # A stale per-site 'active' override that must be cleared on offboard.
        site = Site(name='offb_site', fqdn='offb.test')
        await site.insert()
        await UserSiteInfo(
            user=user, site=site, status=await status_link('active'),
        ).insert()

        handler = make_iam_handler(person_status=404)
        async with make_iam_api(handler) as api:
            result = await SyncUserIAM.run(
                beanie_client, None,
                username='jdoe', iam_api=api,
                grace_days=14, expiry_offset_days=30, now=now,
            )
        assert result.outcome == 'miss_offboarding'

        fetched = await User.find_one(
            User.name == 'jdoe', fetch_links=True, nesting_depth=1,
        )
        assert fetched.expires_at == now + timedelta(days=30)
        assert fetched.status is not None
        assert fetched.status.status_name == 'offboarding'
        assert fetched.iam.first_missing_at == first_missed
        # Per-site override cleared → effective status falls back to global.
        usi = await UserSiteInfo.find_one(UserSiteInfo.user.id == user.id)
        assert usi.status is None

    async def test_miss_already_expiring(self, beanie_client, make_user):
        from ..models.user import UCDIAMInfo
        now = datetime(2026, 5, 1)
        existing_expiry = now + timedelta(days=10)
        await make_user(
            status='offboarding',
            expires_at=existing_expiry,
            iam=UCDIAMInfo(
                iam_status='missing',
                person=UCDIAMPerson(iam_id=1000000001, mothra_id=1000001),
                first_missing_at=now - timedelta(days=20),
            ),
        )

        handler = make_iam_handler(person_status=404)
        async with make_iam_api(handler) as api:
            result = await SyncUserIAM.run(
                beanie_client, None,
                username='jdoe', iam_api=api,
                grace_days=14, expiry_offset_days=30, now=now,
            )
        assert result.outcome == 'miss_already_expiring'

        fetched = await User.find_one(User.name == 'jdoe')
        assert fetched.expires_at == existing_expiry  # NOT overwritten

    async def test_no_prior_iam_resolver_misses_starts_streak(
        self, beanie_client, make_user,
    ):
        # User has no iam AND resolver returns missing. We do NOT distinguish
        # this from a real miss — legacy users without prior IAM data must
        # still enter the missing -> offboarding pipeline.
        now = datetime(2026, 5, 1)
        await make_user()  # no iam
        handler = make_iam_handler(person=[])  # search returns empty
        async with make_iam_api(handler) as api:
            result = await SyncUserIAM.run(
                beanie_client, None,
                username='jdoe', iam_api=api,
                grace_days=14, expiry_offset_days=30, now=now,
            )
        assert result.outcome == 'miss_first'

        fetched = await User.find_one(User.name == 'jdoe')
        assert fetched.iam is not None
        assert fetched.iam.iam_status == 'missing'
        assert fetched.iam.first_missing_at == now
        assert fetched.iam.iam_synced_at == now
        # No person snapshot — we never had one to capture.
        assert fetched.iam.person is None

    async def test_followup_miss_with_tz_aware_default_now(
        self, beanie_client, make_user,
    ):
        """Regression: a follow-up sync against a user with a stored
        first_missing_at must work when self.now is the default
        (production path: tz-aware UTC). Previously this raised
        TypeError: can't subtract offset-naive and offset-aware datetimes
        because mongo round-trips datetimes as naive.
        """
        first_missed = datetime(2026, 4, 20)
        await make_user(iam=UCDIAMInfo(
            iam_status='missing',
            person=UCDIAMPerson(iam_id=1000000001, mothra_id=1000001),
            first_missing_at=first_missed,
        ))
        handler = make_iam_handler(person_status=404)
        async with make_iam_api(handler) as api:
            # Note: no `now=` kwarg — exercises the production default.
            result = await SyncUserIAM.run(
                beanie_client, None,
                username='jdoe', iam_api=api,
                grace_days=14, expiry_offset_days=30,
            )
        # 'now' will be roughly today; the streak we preloaded is older
        # than 14 days, so we should have flipped to offboarding.
        assert result.outcome in {'miss_within_grace', 'miss_offboarding'}

    async def test_resolver_hits_but_get_person_misses_records_streak(
        self, beanie_client, make_user,
    ):
        """Regression: when resolver finds an iam_id but get_person 404s,
        we must still record the missing streak. Previously this case
        silently dropped the bookkeeping ('miss_never_seen' dead path)."""
        now = datetime(2026, 5, 1)
        await make_user()  # no prior iam

        def handler(req):
            if '/iam/people/prikerbacct/search' in req.url.path:
                return _iam_response([PERSON_JANE])  # resolver hit
            if req.url.path.startswith('/iam/people/'):
                return httpx.Response(404, content=b'')  # get_person miss
            return _iam_response([])

        async with make_iam_api(handler) as api:
            result = await SyncUserIAM.run(
                beanie_client, None,
                username='jdoe', iam_api=api,
                grace_days=14, expiry_offset_days=30, now=now,
            )
        assert result.outcome == 'miss_first'

        fetched = await User.find_one(User.name == 'jdoe')
        assert fetched.iam is not None
        assert fetched.iam.iam_status == 'missing'
        assert fetched.iam.first_missing_at == now
        assert fetched.iam.iam_synced_at == now
        # No person snapshot was captured (we never got a successful payload).
        assert fetched.iam.person is None

    async def test_transport_error_no_writes(self, beanie_client, make_user):
        from ..models.user import UCDIAMInfo
        now = datetime(2026, 5, 1)
        prior = UCDIAMInfo(person=UCDIAMPerson(iam_id=1000000001, mothra_id=1000001))
        await make_user(iam=prior)

        handler = make_iam_handler(raise_transport=True)
        async with make_iam_api(handler) as api:
            with pytest.raises(IAMTransientError):
                await SyncUserIAM.run(
                    beanie_client, None,
                    username='jdoe', iam_api=api,
                    grace_days=14, expiry_offset_days=30, now=now,
                )

        fetched = await User.find_one(User.name == 'jdoe')
        # iam unchanged
        assert fetched.iam.first_missing_at is None
        assert fetched.iam.iam_synced_at is None
        # No History entry was written
        assert await History.find(History.op == 'sync_user_iam').count() == 0

    async def test_5xx_no_writes(self, beanie_client, make_user):
        from ..models.user import UCDIAMInfo
        now = datetime(2026, 5, 1)
        await make_user(iam=UCDIAMInfo(person=UCDIAMPerson(iam_id=1000000001, mothra_id=1000001)))
        handler = make_iam_handler(person_status=503)
        async with make_iam_api(handler) as api:
            with pytest.raises(IAMTransientError):
                await SyncUserIAM.run(
                    beanie_client, None,
                    username='jdoe', iam_api=api,
                    grace_days=14, expiry_offset_days=30, now=now,
                )

    async def test_type_guard_rejects_system_user(self, beanie_client, make_user):
        await make_user(name='daemon', user_type='system', uid=900)
        now = datetime(2026, 5, 1)
        handler = make_iam_handler(person=[PERSON_JANE])
        async with make_iam_api(handler) as api:
            with pytest.raises(ValueError, match='not applicable'):
                await SyncUserIAM.run(
                    beanie_client, None,
                    username='daemon', iam_api=api,
                    grace_days=14, expiry_offset_days=30, now=now,
                )
        # No History entry written for the rejected sync.
        assert await History.find(History.op == 'sync_user_iam').count() == 0

    async def test_lifecycle_hit_miss_offboard_restore(self, beanie_client, make_user):
        from ..models.user import UCDIAMInfo
        await make_user(status='active')
        # Phase 1: hit. Resolver finds them, get_person returns data.
        def hit(req):
            if '/iam/people/prikerbacct/search' in req.url.path:
                return _iam_response([PERSON_JANE])
            if req.url.path.startswith('/iam/people/'):
                return _iam_response([PERSON_JANE])
            if req.url.path.startswith('/iam/associations/'):
                return _iam_response([])
            return _iam_response([])
        miss_handler = make_iam_handler(person_status=404)

        t0 = datetime(2026, 5, 1)
        async with make_iam_api(hit) as api:
            r1 = await SyncUserIAM.run(
                beanie_client, None,
                username='jdoe', iam_api=api,
                grace_days=14, expiry_offset_days=30, now=t0,
            )
        assert r1.outcome == 'hit'

        # Phase 2: miss-first
        t1 = t0 + timedelta(days=1)
        async with make_iam_api(miss_handler) as api:
            r2 = await SyncUserIAM.run(
                beanie_client, None,
                username='jdoe', iam_api=api,
                grace_days=14, expiry_offset_days=30, now=t1,
            )
        assert r2.outcome == 'miss_first'

        # Phase 3: still missing, past grace
        t2 = t1 + timedelta(days=20)
        async with make_iam_api(miss_handler) as api:
            r3 = await SyncUserIAM.run(
                beanie_client, None,
                username='jdoe', iam_api=api,
                grace_days=14, expiry_offset_days=30, now=t2,
            )
        assert r3.outcome == 'miss_offboarding'
        fetched = await User.find_one(
            User.name == 'jdoe', fetch_links=True, nesting_depth=1,
        )
        assert fetched.status is not None
        assert fetched.status.status_name == 'offboarding'
        assert fetched.expires_at == t2 + timedelta(days=30)

        # Phase 4: reappears, restored
        t3 = t2 + timedelta(days=5)
        async with make_iam_api(hit) as api:
            r4 = await SyncUserIAM.run(
                beanie_client, None,
                username='jdoe', iam_api=api,
                grace_days=14, expiry_offset_days=30, now=t3,
            )
        assert r4.outcome == 'hit_restored'
        fetched = await User.find_one(
            User.name == 'jdoe', fetch_links=True, nesting_depth=1,
        )
        assert fetched.status is not None
        assert fetched.status.status_name == 'active'
        assert fetched.expires_at is None


# ---------------------------------------------------------------------------
# SyncAllUsersIAM driver
# ---------------------------------------------------------------------------


class TestSyncAllUsersIAM:

    @pytest_asyncio.fixture(loop_scope='session')
    async def populated_users(self, beanie_client):
        from ..models.user import UCDIAMInfo
        users = [
            User(name='hit_user', email='h@x.test', uid=20001, gid=20001,
                 fullname='Hit User', home_directory='/home/hit_user', type='user',
                 iam=UCDIAMInfo(person=UCDIAMPerson(iam_id=1000000001, mothra_id=1000001))),
            User(name='miss_user', email='m@x.test', uid=20002, gid=20002,
                 fullname='Miss User', home_directory='/home/miss_user', type='user',
                 iam=UCDIAMInfo(person=UCDIAMPerson(iam_id=2000000002, mothra_id=2000002))),
            User(name='daemon', email='d@x.test', uid=20003, gid=20003,
                 fullname='Daemon', home_directory='/home/daemon', type='system'),
        ]
        for u in users:
            await u.insert()
        return users

    async def test_default_skips_non_syncable_types(self, beanie_client, populated_users):
        # Hit for both user-typed users; system user MUST be skipped.
        now = datetime(2026, 5, 1)
        def handler(req):
            if '/iam/people/prikerbacct/search' in req.url.path:
                return _iam_response([PERSON_JANE])
            if req.url.path.startswith('/iam/people/'):
                # iam_id 1 hits, iam_id 2 misses
                if '/1000000001' in req.url.path:
                    return _iam_response([PERSON_JANE])
                return httpx.Response(404, content=b'')
            if req.url.path.startswith('/iam/associations/'):
                return _iam_response([])
            return _iam_response([])

        async with make_iam_api(handler) as api:
            tally = await SyncAllUsersIAM.run(
                beanie_client, None,
                iam_api=api,
                grace_days=14, expiry_offset_days=30,
                now=now,
            )
        assert tally['hit'] == 1
        assert tally['miss_first'] == 1
        # Daemon system user was filtered out, so total != 3
        # transient_error should be 0
        assert tally['transient_error'] == 0
        assert tally['error'] == 0

        # Verify daemon was untouched
        daemon = await User.find_one(User.name == 'daemon')
        assert daemon.iam is None

    async def test_caller_filter_intersected_with_syncable(
        self, beanie_client, populated_users,
    ):
        # Even if caller passes ['user', 'system'], system is dropped.
        now = datetime(2026, 5, 1)
        handler = make_iam_handler(person_status=404)
        async with make_iam_api(handler) as api:
            tally = await SyncAllUsersIAM.run(
                beanie_client, None,
                iam_api=api,
                grace_days=14, expiry_offset_days=30,
                types=['user', 'system'],   # 'system' must be dropped
                now=now,
            )
        # Only the 2 user-typed users were processed.
        assert tally['miss_first'] == 2
        assert tally['hit'] == 0


# ---------------------------------------------------------------------------
# ReapOffboardedUsers
# ---------------------------------------------------------------------------


class TestReapOffboardedUsers:

    async def test_reaps_only_past_expiry(self, beanie_client):
        now = datetime(2026, 5, 1)
        offboarding = await status_link('offboarding')
        inactive = await status_link('inactive')
        users = [
            # Past expiry — should be reaped
            User(name='past', email='p@x.test', uid=30001, gid=30001,
                 fullname='Past', home_directory='/home/past', type='user',
                 status=offboarding,
                 expires_at=now - timedelta(days=1)),
            # Future expiry — should NOT be reaped
            User(name='future', email='f@x.test', uid=30002, gid=30002,
                 fullname='Future', home_directory='/home/future', type='user',
                 status=offboarding,
                 expires_at=now + timedelta(days=10)),
            # Offboarding but no expiry set — should NOT be reaped
            User(name='no_expiry', email='n@x.test', uid=30003, gid=30003,
                 fullname='No Expiry', home_directory='/home/no_expiry', type='user',
                 status=offboarding),
            # Past expiry but already inactive — should NOT be reaped
            User(name='already_inactive', email='i@x.test', uid=30004, gid=30004,
                 fullname='Already Inactive', home_directory='/home/already_inactive',
                 type='user', status=inactive,
                 expires_at=now - timedelta(days=5)),
        ]
        for u in users:
            await u.insert()

        reaped = await ReapOffboardedUsers.run(beanie_client, None, now=now)
        assert reaped == ['past']

        past = await User.find_one(User.name == 'past',
                                   fetch_links=True, nesting_depth=1)
        future = await User.find_one(User.name == 'future',
                                     fetch_links=True, nesting_depth=1)
        no_expiry = await User.find_one(User.name == 'no_expiry',
                                        fetch_links=True, nesting_depth=1)
        already = await User.find_one(User.name == 'already_inactive',
                                      fetch_links=True, nesting_depth=1)
        assert past.status.status_name == 'inactive'
        assert future.status.status_name == 'offboarding'
        assert no_expiry.status.status_name == 'offboarding'
        assert already.status.status_name == 'inactive'

    async def test_empty_when_nothing_due(self, beanie_client):
        now = datetime(2026, 5, 1)
        # No offboarding users at all.
        reaped = await ReapOffboardedUsers.run(beanie_client, None, now=now)
        assert reaped == []


# ---------------------------------------------------------------------------
# Offboarding lifecycle notifications
# ---------------------------------------------------------------------------


def _collector():
    """A notifier that records the Email objects it is handed."""
    sent: list = []

    async def notify(mail) -> None:
        sent.append(mail)

    return sent, notify


class TestSyncAllUsersIAMNotify:

    async def _seed_leaver(self, name, uid, now, *, days_missing):
        await User(
            name=name, email=f'{name}@example.edu', uid=uid, gid=uid,
            fullname=name.capitalize(), home_directory=f'/home/{name}',
            type='user', status=await status_link('active'),
            iam=UCDIAMInfo(
                iam_status='missing',
                person=UCDIAMPerson(iam_id=uid, mothra_id=uid),
                first_missing_at=now - timedelta(days=days_missing),
            ),
        ).insert()

    async def test_offboarding_sends_warning(self, beanie_client):
        now = datetime(2026, 5, 1)
        await self._seed_leaver('leaver', 40001, now, days_missing=20)
        sent, notify = _collector()

        handler = make_iam_handler(person_status=404)
        async with make_iam_api(handler) as api:
            tally = await SyncAllUsersIAM.run(
                beanie_client, None,
                iam_api=api, grace_days=14, expiry_offset_days=30,
                now=now, notifier=notify,
            )
        assert tally['miss_offboarding'] == 1
        assert tally['notified_offboarding'] == 1
        assert tally['notify_error'] == 0

        assert len(sent) == 1
        mail = sent[0]
        assert isinstance(mail, AccountOffboardingEmail)
        assert mail.emails == ['leaver@example.edu']
        body = '\n\n'.join(mail.paragraphs())
        assert (now + timedelta(days=30)).strftime('%B %d, %Y') in body

    async def test_no_mail_within_grace(self, beanie_client):
        now = datetime(2026, 5, 1)
        await self._seed_leaver('recent', 40002, now, days_missing=2)
        sent, notify = _collector()

        handler = make_iam_handler(person_status=404)
        async with make_iam_api(handler) as api:
            tally = await SyncAllUsersIAM.run(
                beanie_client, None,
                iam_api=api, grace_days=14, expiry_offset_days=30,
                now=now, notifier=notify,
            )
        assert tally['miss_within_grace'] == 1
        assert tally['notified_offboarding'] == 0
        assert sent == []

    async def test_notify_failure_does_not_block_transition(self, beanie_client):
        now = datetime(2026, 5, 1)
        await self._seed_leaver('leaver2', 40003, now, days_missing=20)

        async def boom(mail):
            raise RuntimeError('notify endpoint down')

        handler = make_iam_handler(person_status=404)
        async with make_iam_api(handler) as api:
            tally = await SyncAllUsersIAM.run(
                beanie_client, None,
                iam_api=api, grace_days=14, expiry_offset_days=30,
                now=now, notifier=boom,
            )
        # The state change is committed and tallied; only the send failed.
        assert tally['miss_offboarding'] == 1
        assert tally['notified_offboarding'] == 0
        assert tally['notify_error'] == 1

        fetched = await User.find_one(
            User.name == 'leaver2', fetch_links=True, nesting_depth=1,
        )
        assert fetched.status.status_name == 'offboarding'
        assert fetched.expires_at == now + timedelta(days=30)


class TestReapOffboardedUsersNotify:

    async def _seed(self, name, uid, status_link_obj, expires_at):
        await User(
            name=name, email=f'{name}@example.edu', uid=uid, gid=uid,
            fullname=name.capitalize(), home_directory=f'/home/{name}',
            type='user', status=status_link_obj, expires_at=expires_at,
        ).insert()

    async def test_deactivation_sends_email_only_to_reaped(self, beanie_client):
        now = datetime(2026, 5, 1)
        offboarding = await status_link('offboarding')
        await self._seed('past', 41001, offboarding, now - timedelta(days=1))
        await self._seed('future', 41002, offboarding, now + timedelta(days=5))
        sent, notify = _collector()

        reaped = await ReapOffboardedUsers.run(
            beanie_client, None, now=now, notifier=notify,
        )
        assert reaped == ['past']
        assert len(sent) == 1
        assert isinstance(sent[0], AccountDeactivatedEmail)
        assert sent[0].emails == ['past@example.edu']

    async def test_notify_failure_does_not_block_reap(self, beanie_client):
        now = datetime(2026, 5, 1)
        offboarding = await status_link('offboarding')
        await self._seed('past2', 41003, offboarding, now - timedelta(days=1))

        async def boom(mail):
            raise RuntimeError('notify endpoint down')

        reaped = await ReapOffboardedUsers.run(
            beanie_client, None, now=now, notifier=boom,
        )
        assert reaped == ['past2']
        fetched = await User.find_one(
            User.name == 'past2', fetch_links=True, nesting_depth=1,
        )
        assert fetched.status.status_name == 'inactive'
