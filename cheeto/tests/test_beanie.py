"""Tests for beanie models and operations.

Requires the session-scoped start_mongodb fixture from conftest.py
which starts an ephemeral mongod with replica set support.
"""

import pytest
import pytest_asyncio
from pymongo import AsyncMongoClient
from beanie import init_beanie

# All async tests and fixtures share a single event loop for the session,
# so the AsyncMongoClient created in beanie_client remains valid across tests.

from ..models import ALL_MODELS
from ..models.history import History
from ..models.site import Site
from ..models.user import User, SshKey
from ..models.group import Group
from ..models.slurm import (
    SlurmAccount,
    SlurmAccountLimits,
    SlurmAllocation,
    SlurmAssociation,
    SlurmPartition,
    SlurmQOS,
    SlurmTRES,
)
from ..models.storage import (
    AutomountMap,
    MountOverrides,
    NFSExportConfig,
    Storage,
    StorageAllocation,
)
from ..models.hippo import HippoEvent
from ..models.user_site_info import UserSiteInfo
from ..operations import (
    AddGroupMember,
    AddGroupSlurmer,
    AddGroupSponsor,
    AddGroupSudoer,
    AddQOSAllocation,
    AddSiteUser,
    EditSlurmAllocation,
    AddUserAccess,
    AddUserComment,
    CreateGroup,
    CreateGroupFromSponsor,
    CreateHomeStorage,
    CreateSite,
    CreateSlurmAssociation,
    CreateSlurmPartition,
    CreateSlurmQOS,
    CreateSystemGroup,
    CreateSystemUser,
    CreateUser,
    RemoveGroupMember,
    RemoveSiteUser,
    RemoveUserAccess,
    SetUserPassword,
    SetUserShell,
    SetUserStatus,
    SetUserType,
)

from .conftest import MONGODB_PORT


BEANIE_TEST_DB = 'cheeto_beanie_test'


@pytest_asyncio.fixture(scope='session', loop_scope="session")
async def beanie_client(start_mongodb):
    client = AsyncMongoClient(f'127.0.0.1:{MONGODB_PORT}')
    await init_beanie(
        database=client[BEANIE_TEST_DB],
        document_models=ALL_MODELS,
    )
    yield client
    await client.close()


@pytest_asyncio.fixture(autouse=True, loop_scope="session")
async def clean_db(beanie_client):
    """Delete all documents before each test (preserves indexes)."""
    for model in ALL_MODELS:
        await model.find_all().delete()
    await History.find_all().delete()


# ---------------------------------------------------------------------------
# Model validation tests
# ---------------------------------------------------------------------------


class TestUserModel:

    @pytest_asyncio.fixture(loop_scope="session")
    async def user(self):
        u = User(
            name='testuser', email='test@ucdavis.edu',
            uid=10000, gid=10000, fullname='Test User',
            home_directory='/home/testuser',
        )
        await u.insert()
        return u

    async def test_create_user(self, beanie_client, user):
        fetched = await User.find_one(User.name == 'testuser')
        assert fetched is not None
        assert fetched.uid == 10000
        assert fetched.status == 'active'
        assert fetched.shell == '/usr/bin/bash'
        assert fetched.access == ['login-ssh']
        assert fetched.created_at is not None
        assert fetched.updated_at is not None

    async def test_create_user_duplicate_rejected_by_operation(self, beanie_client, user):
        with pytest.raises(ValueError, match='already exists'):
            await CreateUser.run(
                beanie_client, None,
                name='testuser', email='other@test.com', uid=20000,
                fullname='Other',
            )

    async def test_user_ssh_keys(self, beanie_client):
        u = User(
            name='keyuser', email='key@test.com',
            uid=10001, gid=10001, fullname='Key User',
            home_directory='/home/keyuser',
            ssh_keys=[SshKey(key='ssh-ed25519 AAAA test@host')],
        )
        await u.insert()
        fetched = await User.find_one(User.name == 'keyuser')
        assert len(fetched.ssh_keys) == 1
        assert fetched.ssh_keys[0].key.startswith('ssh-ed25519')
        assert fetched.ssh_keys[0].registered_at is not None

    async def test_user_validation_bad_shell(self):
        with pytest.raises(Exception):
            User(
                name='x', email='x@test.com', uid=1, gid=1,
                fullname='X', home_directory='/x', shell='/bad',
            )

    async def test_user_validation_bad_type(self):
        with pytest.raises(Exception):
            User(
                name='x', email='x@test.com', uid=1, gid=1,
                fullname='X', home_directory='/x', type='invalid',
            )

    async def test_user_validation_bad_access(self):
        with pytest.raises(Exception):
            User(
                name='x', email='x@test.com', uid=1, gid=1,
                fullname='X', home_directory='/x', access=['bogus'],
            )


class TestSiteModel:

    async def test_create_site(self, beanie_client):
        site = Site(name='farm', fqdn='farm.hpc.ucdavis.edu')
        await site.insert()
        fetched = await Site.find_one(Site.name == 'farm')
        assert fetched is not None
        assert fetched.fqdn == 'farm.hpc.ucdavis.edu'
        assert fetched.created_at is not None

    async def test_site_unique_name(self, beanie_client):
        from pymongo.errors import DuplicateKeyError
        await Site(name='uniq', fqdn='a.com').insert()
        with pytest.raises(DuplicateKeyError):
            await Site(name='uniq', fqdn='b.com').insert()


class TestGroupModel:

    async def test_create_group_with_members(self, beanie_client):
        user = User(
            name='gmember', email='gm@test.com',
            uid=20000, gid=20000, fullname='G Member',
            home_directory='/home/gmember',
        )
        await user.insert()

        group = Group(
            name='testgroup', gid=30000,
            members=[user], sponsors=[user],
        )
        await group.insert()

        fetched = await Group.find_one(
            Group.name == 'testgroup', fetch_links=True,
        )
        assert fetched is not None
        assert len(fetched.members) == 1
        assert fetched.members[0].name == 'gmember'
        assert len(fetched.sponsors) == 1

    async def test_user_groups_backlink(self, beanie_client):
        user = User(
            name='bluser', email='bl@test.com',
            uid=20001, gid=20001, fullname='BL User',
            home_directory='/home/bluser',
        )
        await user.insert()

        g1 = Group(name='grp1', gid=30001, members=[user])
        g2 = Group(name='grp2', gid=30002, members=[user])
        await g1.insert()
        await g2.insert()

        fetched = await User.find_one(
            User.name == 'bluser', fetch_links=True,
        )
        assert len(fetched.groups) == 2
        group_names = {g.name for g in fetched.groups}
        assert group_names == {'grp1', 'grp2'}


class TestUserSiteInfoModel:

    async def test_create_user_site_info(self, beanie_client):
        user = User(
            name='usiuser', email='usi@test.com',
            uid=20010, gid=20010, fullname='USI User',
            home_directory='/home/usiuser',
        )
        await user.insert()
        site = Site(name='usisite', fqdn='usi.hpc.test')
        await site.insert()

        usi = UserSiteInfo(user=user, site=site, access=['login-ssh', 'slurm'])
        await usi.insert()

        fetched = await UserSiteInfo.find_one(
            UserSiteInfo.user.id == user.id, fetch_links=True,
        )
        assert fetched is not None
        assert fetched.status == 'active'
        assert 'slurm' in fetched.access


class TestStorageModel:

    async def test_storage_quota_sums_allocations(self, beanie_client):
        user = User(
            name='storuser', email='stor@test.com',
            uid=20020, gid=20020, fullname='Stor User',
            home_directory='/home/storuser',
        )
        await user.insert()
        group = Group(name='storuser', gid=20020, members=[user])
        await group.insert()
        site = Site(name='storsite', fqdn='stor.hpc.test')
        await site.insert()

        storage = Storage(
            name='storuser', site=site, type='zfs', category='home',
            owner=user, group=group, host='nas01',
            allocations=[
                StorageAllocation(quota='100G', comment='initial'),
                StorageAllocation(quota='50G', comment='expansion'),
            ],
        )
        await storage.insert()

        fetched = await Storage.find_one(Storage.name == 'storuser')
        assert fetched.quota == '150G'

    async def test_storage_no_allocations(self, beanie_client):
        user = User(
            name='noalloc', email='na@test.com',
            uid=20021, gid=20021, fullname='No Alloc',
            home_directory='/home/noalloc',
        )
        await user.insert()
        group = Group(name='noalloc', gid=20021, members=[user])
        await group.insert()
        site = Site(name='nasite', fqdn='na.hpc.test')
        await site.insert()

        storage = Storage(
            name='noalloc', site=site, type='quobyte', category='home',
            owner=user, group=group, host='qb01',
        )
        await storage.insert()

        fetched = await Storage.find_one(Storage.name == 'noalloc')
        assert fetched.quota is None

    async def test_storage_without_automount(self, beanie_client):
        user = User(
            name='qbuser', email='qb@test.com',
            uid=20022, gid=20022, fullname='QB User',
            home_directory='/home/qbuser',
        )
        await user.insert()
        group = Group(name='qbuser', gid=20022, members=[user])
        await group.insert()
        site = Site(name='qbsite', fqdn='qb.hpc.test')
        await site.insert()

        storage = Storage(
            name='qbuser', site=site, type='quobyte', category='home',
            owner=user, group=group, host='qb01',
        )
        await storage.insert()

        fetched = await Storage.find_one(Storage.name == 'qbuser')
        assert fetched.automount_map is None
        assert fetched.mount_path == ''
        assert fetched.mount_options == []


class TestSlurmModels:

    async def test_slurm_tres_validation(self):
        t = SlurmTRES(cpus=4, gpus=2, mem='10G')
        assert t.cpus == 4
        assert t.mem == '10G'

        with pytest.raises(Exception):
            SlurmTRES(mem='bad')

    async def test_slurm_allocation(self):
        a = SlurmAllocation(
            tres=SlurmTRES(cpus=8), comment='test',
        )
        assert a.tres.cpus == 8
        assert a.comment == 'test'
        # SlurmAllocation is now a Document — it has BaseDocument timestamps
        assert a.created_at is not None
        assert a.updated_at is not None

    async def test_storage_allocation_validation(self):
        a = StorageAllocation(quota='100G', comment='ok')
        assert a.quota == '100G'

        with pytest.raises(Exception):
            StorageAllocation(quota='bad')


# ---------------------------------------------------------------------------
# Operation tests
# ---------------------------------------------------------------------------


class TestCreateSiteOp:

    async def test_create_site(self, beanie_client):
        site = await CreateSite.run(
            beanie_client, None,
            name='opsite', fqdn='opsite.hpc.test',
        )
        assert site.name == 'opsite'

        fetched = await Site.find_one(Site.name == 'opsite')
        assert fetched is not None

        # History entry created
        hist = await History.find_one(History.op == 'create_site')
        assert hist is not None
        assert hist.changes['name'] == 'opsite'
        assert hist.author is None  # bootstrap, no author

    async def test_create_site_duplicate(self, beanie_client):
        await CreateSite.run(
            beanie_client, None, name='dup', fqdn='dup.test',
        )
        with pytest.raises(ValueError, match='already exists'):
            await CreateSite.run(
                beanie_client, None, name='dup', fqdn='dup2.test',
            )


class TestCreateUserOp:

    async def test_create_user(self, beanie_client):
        user, group = await CreateUser.run(
            beanie_client, None,
            name='opuser', email='op@test.com', uid=40000,
            fullname='Op User',
        )
        assert user.name == 'opuser'
        assert user.uid == 40000
        assert user.gid == 40000
        assert user.home_directory == '/home/opuser'
        assert group.name == 'opuser'
        assert group.gid == 40000
        assert group.type == 'user'

        hist = await History.find_one(History.op == 'create_user')
        assert hist is not None
        assert hist.changes['username'] == 'opuser'

    async def test_create_user_custom_gid(self, beanie_client):
        user, group = await CreateUser.run(
            beanie_client, None,
            name='giduser', email='gid@test.com', uid=40001,
            fullname='GID User', gid=50001,
        )
        assert user.gid == 50001
        assert group.gid == 50001

    async def test_create_user_duplicate(self, beanie_client):
        await CreateUser.run(
            beanie_client, None,
            name='dupuser', email='dup@test.com', uid=40002,
            fullname='Dup User',
        )
        with pytest.raises(ValueError, match='already exists'):
            await CreateUser.run(
                beanie_client, None,
                name='dupuser', email='dup2@test.com', uid=40003,
                fullname='Dup User 2',
            )

    async def test_create_system_user(self, beanie_client):
        user, group = await CreateSystemUser.run(
            beanie_client, None,
            name='sysuser', email='sys@test.com', fullname='Sys User',
        )
        assert user.type == 'system'
        assert user.uid >= 4_000_000_000
        assert user.shell == '/usr/sbin/nologin'

    async def test_create_user_with_password_hashes(self, beanie_client):
        plaintext = 'initial-password'
        user, _ = await CreateUser.run(
            beanie_client, None,
            name='pwuser', email='pw@test.com', uid=41500,
            fullname='PW User', password=plaintext,
        )
        assert user.password is not None
        assert user.password != plaintext
        assert user.password.startswith('$y$')

    async def test_create_system_user_with_password_hashes(self, beanie_client):
        plaintext = 'sys-init-password'
        user, _ = await CreateSystemUser.run(
            beanie_client, None,
            name='syspwuser', email='sys@test.com', fullname='Sys PW',
            password=plaintext,
        )
        assert user.password is not None
        assert user.password != plaintext
        assert user.password.startswith('$y$')


class TestUserMutationOps:

    @pytest_asyncio.fixture(loop_scope="session")
    async def user(self, beanie_client):
        user, _ = await CreateUser.run(
            beanie_client, None,
            name='mutuser', email='mut@test.com', uid=41000,
            fullname='Mut User',
        )
        return user

    async def test_set_user_status(self, beanie_client, user):
        await SetUserStatus.run(
            beanie_client, None,
            name='mutuser', status='inactive', reason='testing',
        )
        fetched = await User.find_one(User.name == 'mutuser')
        assert fetched.status == 'inactive'
        assert any('status=inactive' in c for c in fetched.comments)

    async def test_set_user_type(self, beanie_client, user):
        await SetUserType.run(
            beanie_client, None, name='mutuser', type='admin',
        )
        fetched = await User.find_one(User.name == 'mutuser')
        assert fetched.type == 'admin'

    async def test_set_user_shell(self, beanie_client, user):
        await SetUserShell.run(
            beanie_client, None, name='mutuser', shell='/bin/zsh',
        )
        fetched = await User.find_one(User.name == 'mutuser')
        assert fetched.shell == '/bin/zsh'

    async def test_add_remove_access(self, beanie_client, user):
        await AddUserAccess.run(
            beanie_client, None,
            name='mutuser', access='slurm',
        )
        fetched = await User.find_one(User.name == 'mutuser')
        assert 'slurm' in fetched.access

        await RemoveUserAccess.run(
            beanie_client, None,
            name='mutuser', access='slurm',
        )
        fetched = await User.find_one(User.name == 'mutuser')
        assert 'slurm' not in fetched.access

    async def test_add_comment(self, beanie_client, user):
        await AddUserComment.run(
            beanie_client, None,
            name='mutuser', comment='test comment',
        )
        fetched = await User.find_one(User.name == 'mutuser')
        assert 'test comment' in fetched.comments

    async def test_set_user_password_hashes(self, beanie_client, user):
        plaintext = 'correct-horse-battery-staple'
        await SetUserPassword.run(
            beanie_client, None,
            name='mutuser', password=plaintext,
        )
        fetched = await User.find_one(User.name == 'mutuser')
        assert fetched.password is not None
        assert fetched.password != plaintext
        assert fetched.password.startswith('$y$')

    async def test_set_user_password_differs_per_call(self, beanie_client, user):
        plaintext = 'same-password'
        await SetUserPassword.run(
            beanie_client, None, name='mutuser', password=plaintext,
        )
        first = (await User.find_one(User.name == 'mutuser')).password
        await SetUserPassword.run(
            beanie_client, None, name='mutuser', password=plaintext,
        )
        second = (await User.find_one(User.name == 'mutuser')).password
        # Same plaintext + random salt => different hashes
        assert first != second


class TestSiteUserOps:

    async def test_add_and_remove_site_user(self, beanie_client):
        user, _ = await CreateUser.run(
            beanie_client, None,
            name='siteuser', email='su@test.com', uid=42000,
            fullname='Site User',
        )
        await CreateSite.run(
            beanie_client, None, name='susite', fqdn='su.test',
        )

        usi = await AddSiteUser.run(
            beanie_client, None,
            user_name='siteuser', site_name='susite',
        )
        assert usi is not None

        fetched = await UserSiteInfo.find_one(
            UserSiteInfo.user.id == user.id,
        )
        assert fetched is not None
        assert fetched.status == 'active'

        await RemoveSiteUser.run(
            beanie_client, None,
            user_name='siteuser', site_name='susite',
        )
        fetched = await UserSiteInfo.find_one(
            UserSiteInfo.user.id == user.id,
        )
        assert fetched is None

    async def test_add_site_user_duplicate(self, beanie_client):
        user, _ = await CreateUser.run(
            beanie_client, None,
            name='dupsite', email='ds@test.com', uid=42001,
            fullname='Dup Site',
        )
        await CreateSite.run(
            beanie_client, None, name='dssite', fqdn='ds.test',
        )
        await AddSiteUser.run(
            beanie_client, None,
            user_name='dupsite', site_name='dssite',
        )
        with pytest.raises(ValueError, match='already on site'):
            await AddSiteUser.run(
                beanie_client, None,
                user_name='dupsite', site_name='dssite',
            )


class TestGroupOps:

    async def test_create_group(self, beanie_client):
        group = await CreateGroup.run(
            beanie_client, None,
            name='opgroup', gid=60000,
        )
        assert group.name == 'opgroup'
        assert group.gid == 60000
        assert group.type == 'group'

    async def test_create_system_group(self, beanie_client):
        group = await CreateSystemGroup.run(
            beanie_client, None, name='sysgroup',
        )
        assert group.type == 'system'
        assert group.gid >= 4_000_000_000

    async def test_create_group_from_sponsor(self, beanie_client):
        user, _ = await CreateUser.run(
            beanie_client, None,
            name='sponsor', email='sp@test.com', uid=43000,
            fullname='Sponsor User',
        )
        group = await CreateGroupFromSponsor.run(
            beanie_client, None, sponsor_name='sponsor',
        )
        assert group.name == 'sponsorgrp'
        assert group.type == 'group'

        fetched = await Group.find_one(
            Group.name == 'sponsorgrp', fetch_links=True,
        )
        assert any(m.name == 'sponsor' for m in fetched.members)
        assert any(s.name == 'sponsor' for s in fetched.sponsors)

        # Original personal group is unchanged
        personal = await Group.find_one(Group.name == 'sponsor')
        assert personal.type == 'user'


class TestGroupMembershipOps:

    @pytest_asyncio.fixture(loop_scope="session")
    async def setup(self, beanie_client):
        user, _ = await CreateUser.run(
            beanie_client, None,
            name='memuser', email='mem@test.com', uid=44000,
            fullname='Mem User',
        )
        group = await CreateGroup.run(
            beanie_client, None, name='memgroup', gid=61000,
        )
        return user, group

    async def test_add_and_remove_member(self, beanie_client, setup):
        user, group = setup

        await AddGroupMember.run(
            beanie_client, None,
            group_name='memgroup', user_name='memuser',
        )
        fetched = await Group.find_one(
            Group.name == 'memgroup', fetch_links=True,
        )
        assert any(m.name == 'memuser' for m in fetched.members)

        await RemoveGroupMember.run(
            beanie_client, None,
            group_name='memgroup', user_name='memuser',
        )
        fetched = await Group.find_one(
            Group.name == 'memgroup', fetch_links=True,
        )
        assert not any(m.name == 'memuser' for m in fetched.members)

    async def test_add_sponsor(self, beanie_client, setup):
        user, group = setup
        await AddGroupSponsor.run(
            beanie_client, None,
            group_name='memgroup', user_name='memuser',
        )
        fetched = await Group.find_one(
            Group.name == 'memgroup', fetch_links=True,
        )
        assert any(s.name == 'memuser' for s in fetched.sponsors)

    async def test_add_sudoer(self, beanie_client, setup):
        user, group = setup
        await AddGroupSudoer.run(
            beanie_client, None,
            group_name='memgroup', user_name='memuser',
        )
        fetched = await Group.find_one(
            Group.name == 'memgroup', fetch_links=True,
        )
        assert any(s.name == 'memuser' for s in fetched.sudoers)

    async def test_add_slurmer(self, beanie_client, setup):
        user, group = setup
        await AddGroupSlurmer.run(
            beanie_client, None,
            group_name='memgroup', user_name='memuser',
        )
        fetched = await Group.find_one(
            Group.name == 'memgroup', fetch_links=True,
        )
        assert any(s.name == 'memuser' for s in fetched.slurmers)


class TestSlurmOps:

    @pytest_asyncio.fixture(loop_scope="session")
    async def site(self, beanie_client):
        return await CreateSite.run(
            beanie_client, None, name='slurmsite', fqdn='slurm.test',
        )

    async def test_create_partition(self, beanie_client, site):
        part = await CreateSlurmPartition.run(
            beanie_client, None,
            name='high', site_name='slurmsite',
        )
        assert part.name == 'high'

        fetched = await SlurmPartition.find_one(
            SlurmPartition.name == 'high',
        )
        assert fetched is not None

    async def test_create_qos(self, beanie_client, site):
        qos = await CreateSlurmQOS.run(
            beanie_client, None,
            name='normal', site_name='slurmsite',
            priority=100,
        )
        assert qos.name == 'normal'
        assert qos.priority == 100
        assert qos.flags == ['DenyOnLimit']

    async def test_create_qos_with_allocations(self, beanie_client, site):
        allocs = [
            SlurmAllocation(tres=SlurmTRES(cpus=128), comment='initial'),
            SlurmAllocation(tres=SlurmTRES(cpus=256), comment='expansion'),
        ]
        qos = await CreateSlurmQOS.run(
            beanie_client, None,
            name='qalloc', site_name='slurmsite',
            group_limits=allocs,
        )
        # Allocations should have been inserted and received ids
        for a in allocs:
            assert a.id is not None

        fetched = await SlurmQOS.find_one(
            SlurmQOS.name == 'qalloc', fetch_links=True,
        )
        assert len(fetched.group_limits) == 2
        cpu_counts = sorted(l.tres.cpus for l in fetched.group_limits)
        assert cpu_counts == [128, 256]

    async def test_add_qos_allocation(self, beanie_client, site):
        await CreateSlurmQOS.run(
            beanie_client, None,
            name='qadd', site_name='slurmsite',
        )
        alloc = await AddQOSAllocation.run(
            beanie_client, None,
            qos_name='qadd', site_name='slurmsite',
            field='user_limits',
            tres=SlurmTRES(cpus=32, mem='64G'),
            comment='user cap',
        )
        assert alloc.id is not None

        fetched = await SlurmQOS.find_one(
            SlurmQOS.name == 'qadd', fetch_links=True,
        )
        assert len(fetched.user_limits) == 1
        assert fetched.user_limits[0].tres.cpus == 32
        assert fetched.user_limits[0].tres.mem == '64G'
        assert fetched.user_limits[0].comment == 'user cap'

    async def test_edit_slurm_allocation(self, beanie_client, site):
        await CreateSlurmQOS.run(
            beanie_client, None,
            name='qedit', site_name='slurmsite',
        )
        alloc = await AddQOSAllocation.run(
            beanie_client, None,
            qos_name='qedit', site_name='slurmsite',
            field='group_limits',
            tres=SlurmTRES(cpus=64),
            comment='original',
        )
        updated = await EditSlurmAllocation.run(
            beanie_client, None,
            allocation_id=str(alloc.id),
            tres=SlurmTRES(cpus=128, mem='1T'),
            comment='doubled',
        )
        assert updated.tres.cpus == 128
        assert updated.tres.mem == '1T'
        assert updated.comment == 'doubled'
        # created_at is preserved, updated_at is bumped
        assert updated.updated_at >= updated.created_at

    async def test_total_tres_sums_allocations(self, beanie_client, site):
        from cheeto.queries.slurm import total_tres

        # Empty list -> unlimited defaults
        empty = total_tres([])
        assert empty.cpus == -1
        assert empty.gpus == -1
        assert empty.mem is None

        # Two allocations with overlapping fields
        a1 = SlurmAllocation(tres=SlurmTRES(cpus=128, gpus=8, mem='1T'))
        a2 = SlurmAllocation(tres=SlurmTRES(cpus=64, gpus=4, mem='512G'))
        summed = total_tres([a1, a2])
        assert summed.cpus == 192
        assert summed.gpus == 12
        # 1T + 512G = 1.5T
        assert summed.mem == '1.5T'

        # Partial default -> unlimited fields don't contribute
        a3 = SlurmAllocation(tres=SlurmTRES(cpus=32))
        partial = total_tres([a1, a3])
        assert partial.cpus == 160
        assert partial.gpus == 8           # only from a1
        assert partial.mem == '1T'         # only from a1


class TestHistoryTracking:

    async def test_operations_create_history(self, beanie_client):
        await CreateSite.run(
            beanie_client, None, name='histsite', fqdn='hist.test',
        )
        await CreateUser.run(
            beanie_client, None,
            name='histuser', email='hist@test.com', uid=49000,
            fullname='Hist User',
        )
        await CreateGroup.run(
            beanie_client, None, name='histgroup', gid=69000,
        )

        entries = await History.find_all().to_list()
        ops = {e.op for e in entries}
        assert 'create_site' in ops
        assert 'create_user' in ops
        assert 'create_group' in ops

    async def test_history_with_author(self, beanie_client):
        author, _ = await CreateUser.run(
            beanie_client, None,
            name='author', email='auth@test.com', uid=49001,
            fullname='Author User',
        )

        await CreateSite.run(
            beanie_client, author,
            name='authsite', fqdn='auth.test',
        )

        hist = await History.find_one(
            History.op == 'create_site',
            History.changes.name == 'authsite',
            fetch_links=True,
        )
        assert hist is not None
        assert hist.author is not None
        assert hist.author.name == 'author'

    async def test_failed_operation_no_history(self, beanie_client):
        await CreateSite.run(
            beanie_client, None, name='failsite', fqdn='fail.test',
        )
        count_before = await History.find(
            History.op == 'create_site',
        ).count()

        with pytest.raises(ValueError):
            await CreateSite.run(
                beanie_client, None, name='failsite', fqdn='fail2.test',
            )

        count_after = await History.find(
            History.op == 'create_site',
        ).count()
        assert count_after == count_before


class TestHippoEventModel:

    async def test_create_hippo_event(self, beanie_client):
        user, _ = await CreateUser.run(
            beanie_client, None,
            name='hippouser', email='hp@test.com', uid=45000,
            fullname='Hippo User',
        )
        site = await CreateSite.run(
            beanie_client, None, name='hsite', fqdn='hsite.test',
        )

        event = HippoEvent(
            hippo_id=42,
            hippo_endpoint='https://hippo.test',
            action='CreateAccount',
            cluster='caesfarm',
            site=site,
            target_username='hippouser',
            target_user=user,
            target_groupnames=['grp-a', 'grp-b'],
            raw={'foo': 'bar'},
        )
        await event.insert()

        fetched = await HippoEvent.find_one(
            HippoEvent.hippo_id == 42,
            HippoEvent.hippo_endpoint == 'https://hippo.test',
            fetch_links=True,
        )
        assert fetched is not None
        assert fetched.action == 'CreateAccount'
        assert fetched.status == 'Pending'
        assert fetched.n_tries == 0
        assert fetched.site.name == 'hsite'
        assert fetched.target_user.name == 'hippouser'
        assert fetched.target_groupnames == ['grp-a', 'grp-b']
        assert fetched.raw == {'foo': 'bar'}
        assert fetched.first_seen_at is not None

    async def test_invalid_action_rejected(self, beanie_client):
        with pytest.raises(Exception):
            HippoEvent(
                hippo_id=1,
                hippo_endpoint='x',
                action='NotARealAction',
            )

    async def test_invalid_status_rejected(self, beanie_client):
        with pytest.raises(Exception):
            HippoEvent(
                hippo_id=1,
                hippo_endpoint='x',
                action='CreateAccount',
                status='Bogus',
            )

    async def test_query_by_status(self, beanie_client):
        await HippoEvent(
            hippo_id=100, hippo_endpoint='e', action='CreateAccount',
        ).insert()
        await HippoEvent(
            hippo_id=101, hippo_endpoint='e', action='UpdateSshKey',
            status='Complete',
        ).insert()

        pending = await HippoEvent.find(
            HippoEvent.status == 'Pending',
        ).to_list()
        assert len(pending) == 1
        assert pending[0].hippo_id == 100
