"""Integration tests for cheeto/ldap_async.py against an ephemeral slapd.

The `start_slapd` session fixture in conftest.py boots an OpenLDAP slapd
on a tmp directory bound to localhost:SLAPD_PORT with admin creds; tests
talk to it via bonsai through `AsyncLDAPManager`.

Skips automatically when `slapd` isn't available on the host.
"""

from __future__ import annotations

import bonsai
import pytest
import pytest_asyncio

from ..ldap_async import (
    AsyncLDAPManager,
    LDAPGroupRecord,
    LDAPNotFound,
    LDAPUserRecord,
)


SITENAME = 'test-cluster'


async def _raw_attr(config, dn: str, attr: str) -> list[str] | None:
    """Fetch one attribute of `dn` with a fresh admin bind, bypassing the
    manager's record mapping — the raw server truth, independent of what
    entry_to_user projects. Returns None when the attribute (or entry) is
    absent."""
    client = bonsai.LDAPClient(config.servers[0])
    client.set_credentials(
        'SIMPLE', user=config.login_dn, password=config.password,
    )
    async with client.connect(is_async=True) as conn:
        results = await conn.search(
            dn, bonsai.LDAPSearchScope.BASE, '(objectClass=*)',
            attrlist=[attr],
        )
    if not results:
        return None
    values = results[0].get(attr)
    if not values:
        return None
    return [
        v.decode() if isinstance(v, (bytes, bytearray)) else str(v)
        for v in values
    ]


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
        # ou=users is seeded by the slapd fixture itself, so it reports
        # already_exists; every per-site entry must be freshly created.
        assert result[manager.user_ou_dn()] == 'already_exists'
        assert all(
            v == 'created' for dn, v in result.items()
            if dn != manager.user_ou_dn()
        ), result
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
            uid=10001, gid=10001, fullname='Alice', surname='Alice',
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
            uid=10002, gid=10002, fullname='Bob', surname='Bob',
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
            uid=99999, gid=99999, fullname='Ghost', surname='Ghost',
            home_directory='/home/ghost', shell='/usr/bin/bash',
        )
        with pytest.raises(LDAPNotFound):
            await manager.update_user(ghost)

    async def test_user_password_written_updated_cleared(
        self, manager, site, slapd_ldap_config,
    ):
        record = LDAPUserRecord(
            username='pwrt', email='pwrt@x.test',
            uid=10010, gid=10010, fullname='PW RT', surname='RT',
            home_directory='/home/pwrt', shell='/usr/bin/bash',
            password='$y$j9T$firstsalt$firsthash',
        )
        await manager.add_user(record)
        dn = manager.user_dn('pwrt')
        assert await _raw_attr(slapd_ldap_config, dn, 'userPassword') == [
            '{CRYPT}$y$j9T$firstsalt$firsthash',
        ]
        # get_user round-trips the wire form for presence reporting.
        fetched = await manager.get_user('pwrt')
        assert fetched.password == '{CRYPT}$y$j9T$firstsalt$firsthash'

        record.password = '$y$j9T$secondsalt$secondhash'
        await manager.update_user(record)
        assert await _raw_attr(slapd_ldap_config, dn, 'userPassword') == [
            '{CRYPT}$y$j9T$secondsalt$secondhash',
        ]

        # Clearing the password in beanie must remove the stale hash.
        record.password = None
        await manager.update_user(record)
        assert await _raw_attr(slapd_ldap_config, dn, 'userPassword') is None
        assert (await manager.get_user('pwrt')).password is None

    async def test_user_password_bind_end_to_end(
        self, manager, site, slapd_ldap_config,
    ):
        # Full regression pin for the {CRYPT} prefix: hash a plaintext with
        # the real production hasher, sync it, then simple-bind as the user.
        # Requires the host's crypt(3)/libxcrypt to support yescrypt
        # (standard on Ubuntu 22.04+).
        from ..operations.user import _hash_password

        plaintext = 'correct-horse-battery-staple'
        record = LDAPUserRecord(
            username='pwbind', email='pwbind@x.test',
            uid=10011, gid=10011, fullname='PW Bind', surname='Bind',
            home_directory='/home/pwbind', shell='/usr/bin/bash',
            password=_hash_password(plaintext),
        )
        await manager.add_user(record)
        user_dn = manager.user_dn('pwbind')

        good = bonsai.LDAPClient(slapd_ldap_config.servers[0])
        good.set_credentials('SIMPLE', user=user_dn, password=plaintext)
        async with good.connect(is_async=True) as conn:
            assert 'pwbind' in await conn.whoami()

        bad = bonsai.LDAPClient(slapd_ldap_config.servers[0])
        bad.set_credentials('SIMPLE', user=user_dn, password='wrong-password')
        with pytest.raises(bonsai.AuthenticationError):
            async with bad.connect(is_async=True):
                pass

    async def test_update_clears_stale_ssh_keys(
        self, manager, site, slapd_ldap_config,
    ):
        record = LDAPUserRecord(
            username='keyclr', email='keyclr@x.test',
            uid=10012, gid=10012, fullname='Key Clear', surname='Clear',
            home_directory='/home/keyclr', shell='/usr/bin/bash',
            ssh_keys=['ssh-ed25519 AAA keyclr@x'],
        )
        await manager.add_user(record)
        dn = manager.user_dn('keyclr')
        assert await _raw_attr(slapd_ldap_config, dn, 'sshPublicKey') == [
            'ssh-ed25519 AAA keyclr@x',
        ]

        record.ssh_keys = []
        await manager.update_user(record)
        assert await _raw_attr(slapd_ldap_config, dn, 'sshPublicKey') is None


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
                uid=uid, gid=uid, fullname=n.title(), surname=n.title(),
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
            uid=10006, gid=10006, fullname='Frank', surname='Frank',
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
