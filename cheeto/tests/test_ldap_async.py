"""Integration tests for cheeto/ldap_async.py against an ephemeral slapd.

The `start_slapd` session fixture in conftest.py boots an OpenLDAP slapd
on a tmp directory bound to localhost:SLAPD_PORT with admin creds; tests
talk to it via bonsai through `AsyncLDAPManager`.

Skips automatically when `slapd` isn't available on the host.
"""

from __future__ import annotations

import pytest
import pytest_asyncio

from ..ldap_async import (
    AsyncLDAPManager,
    LDAPGroupRecord,
    LDAPNotFound,
    LDAPUserRecord,
)


SITENAME = 'test-cluster'


@pytest_asyncio.fixture(loop_scope='session')
async def manager(slapd_ldap_config):
    """Live AsyncLDAPManager against the ephemeral slapd. Each test gets
    its own context (so the pool is fresh). The session-scoped slapd is
    re-used across tests; site OUs are created lazily by tests that need
    them via ensure_site_tree()."""
    async with AsyncLDAPManager(
        slapd_ldap_config, sitename=SITENAME,
    ) as mgr:
        yield mgr


@pytest_asyncio.fixture(loop_scope='session', autouse=True)
async def _wipe_site(slapd_ldap_config):
    """Per-test cleanup: delete any per-site subtree so tests start fresh.
    Runs before each test in this module."""
    # We use a one-shot manager just for cleanup; the test's `manager`
    # fixture re-opens its own pool.
    async with AsyncLDAPManager(
        slapd_ldap_config, sitename=SITENAME,
    ) as mgr:
        # Recursively delete the site subtree if it exists. We probe the
        # site-OU dn directly and walk down on hit. Simpler than handling
        # NoSuchObject from each individual delete.
        site_dn = mgr.site_ou_dn()
        if not await mgr.dn_exists(site_dn):
            yield
            return
        # Walk the tree and delete bottom-up. Using ldapsearch via the
        # pool's spawn() is overkill here; pull the subtree via list_*
        # helpers and delete.
        # Easy path: blow away the users we know about + groups + automounts.
        for groupname in [g.groupname for g in await mgr.list_groups()]:
            await mgr.delete_group(groupname)
        for mountname in await mgr.list_automounts('auto.home'):
            await mgr.delete_automount(mountname, 'auto.home')
        for mountname in await mgr.list_automounts('auto.group'):
            await mgr.delete_automount(mountname, 'auto.group')
        # Also wipe ou=users entries created by tests.
        for u in await mgr.list_users():
            await mgr.delete_user(u.username)
    yield


class TestAsyncLDAPManagerSmoke:

    async def test_manager_opens_and_pool_works(self, manager):
        """Confirms the slapd fixture boots and bonsai can talk to it."""
        # ou=users was seeded by start_slapd; the user_base dn should exist.
        assert await manager.dn_exists(manager.config.user_base) is True
        # Random other DN should not exist.
        assert await manager.dn_exists('uid=nobody,' + manager.config.user_base) is False


class TestEnsureSiteTree:

    async def test_creates_full_tree(self, manager):
        result = await manager.ensure_site_tree()
        # All 6 DNs created plus the 2 automount keys = 8.
        assert all(v == 'created' for v in result.values()), result
        # Verify each is now present.
        for dn in result.keys():
            assert await manager.dn_exists(dn)

    async def test_idempotent(self, manager):
        await manager.ensure_site_tree()
        result = await manager.ensure_site_tree()
        assert all(v == 'already_exists' for v in result.values()), result


class TestUserCRUD:

    @pytest_asyncio.fixture(loop_scope='session')
    async def site(self, manager):
        await manager.ensure_site_tree()

    async def test_add_get_delete_user(self, manager, site):
        record = LDAPUserRecord(
            username='alice', email='alice@example.test',
            uid=10001, gid=10001, fullname='Alice',
            home_directory='/home/alice', shell='/usr/bin/bash',
        )
        await manager.add_user(record)
        assert await manager.user_exists('alice')

        fetched = await manager.get_user('alice')
        assert fetched is not None
        assert fetched.username == 'alice'
        assert fetched.uid == 10001

        await manager.delete_user('alice')
        assert not await manager.user_exists('alice')

    async def test_delete_user_idempotent(self, manager, site):
        # Deleting a non-existent user does not raise.
        await manager.delete_user('nobody')

    async def test_update_user(self, manager, site):
        record = LDAPUserRecord(
            username='bob', email='bob@example.test',
            uid=10002, gid=10002, fullname='Bob',
            home_directory='/home/bob', shell='/usr/bin/bash',
        )
        await manager.add_user(record)
        record.email = 'bob@updated.test'
        await manager.update_user(record)
        fetched = await manager.get_user('bob')
        assert fetched.email == 'bob@updated.test'

    async def test_update_nonexistent_user_raises(self, manager, site):
        ghost = LDAPUserRecord(
            username='ghost', email='nope@x',
            uid=99999, gid=99999, fullname='Ghost',
            home_directory='/home/ghost', shell='/usr/bin/bash',
        )
        with pytest.raises(LDAPNotFound):
            await manager.update_user(ghost)


class TestGroupCRUD:

    @pytest_asyncio.fixture(loop_scope='session')
    async def site(self, manager):
        await manager.ensure_site_tree()

    async def test_add_get_delete_group(self, manager, site):
        record = LDAPGroupRecord(groupname='staff', gid=20001)
        await manager.add_group(record)
        assert await manager.group_exists('staff')

        fetched = await manager.get_group('staff')
        assert fetched is not None
        assert fetched.groupname == 'staff'
        assert fetched.gid == 20001

        await manager.delete_group('staff')
        assert not await manager.group_exists('staff')

    async def test_set_group_members_diff_and_patch(self, manager, site):
        # Seed two users + a group.
        for n, uid in [('carol', 10003), ('dave', 10004), ('eve', 10005)]:
            await manager.add_user(LDAPUserRecord(
                username=n, email=f'{n}@x.test',
                uid=uid, gid=uid, fullname=n.title(),
                home_directory=f'/home/{n}', shell='/usr/bin/bash',
            ))
        await manager.add_group(LDAPGroupRecord(
            groupname='eng', gid=20002, members={'carol', 'dave'},
        ))

        # New target: drop dave, add eve.
        await manager.set_group_members('eng', {'carol', 'eve'})
        fetched = await manager.get_group('eng')
        assert fetched.members == {'carol', 'eve'}

    async def test_list_user_memberships(self, manager, site):
        await manager.add_user(LDAPUserRecord(
            username='frank', email='frank@x.test',
            uid=10006, gid=10006, fullname='Frank',
            home_directory='/home/frank', shell='/usr/bin/bash',
        ))
        await manager.add_group(LDAPGroupRecord(
            groupname='ops', gid=20003, members={'frank'},
        ))
        await manager.add_group(LDAPGroupRecord(
            groupname='sec', gid=20004, members={'frank'},
        ))
        memberships = await manager.list_user_memberships('frank')
        assert memberships == {'ops', 'sec'}
