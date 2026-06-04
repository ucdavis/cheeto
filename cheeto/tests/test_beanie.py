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
from ..models.group_membership import GroupMembership
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
    EditSlurmQOS,
    MigrateAccessStatusGroups,
    MigrateUser,
    ProvisionSlurmAllocation,
    RemoveSlurmAssociation,
    RemoveSlurmPartition,
    RemoveSlurmQOS,
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
    SyncUserToLDAP,
)

from .conftest import MONGODB_PORT, access_links, seed_access_status_groups, status_link


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
    """Delete all documents before each test (preserves indexes), then
    re-seed AccessGroup / StatusGroup records so operations that resolve
    Links via access_name/status_name find them."""
    for model in ALL_MODELS:
        await model.find_all().delete()
    await History.find_all().delete()
    await seed_access_status_groups()


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
        # Default User has no status/access Links assigned at construction.
        # Operations that care (CreateUser, SyncUserIAM) resolve them from
        # status_name / access_name strings.
        assert fetched.status is None
        assert fetched.shell == '/usr/bin/bash'
        assert fetched.access == []
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
        )
        await u.insert()
        await SshKey(key='ssh-ed25519 AAAA test@host', user=u).insert()
        await SshKey(key='ssh-rsa BBBB other@host', user=u).insert()

        keys = await SshKey.find(SshKey.user.id == u.id).to_list()
        assert len(keys) == 2
        assert {k.key for k in keys} == {
            'ssh-ed25519 AAAA test@host',
            'ssh-rsa BBBB other@host',
        }
        # SshKey is a Document — gets created_at from BaseDocument and the
        # optional expires_at/provisioned_at from the Expirable mixin.
        assert keys[0].created_at is not None
        assert keys[0].expires_at is None
        assert keys[0].provisioned_at is None

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


class TestExpirable:
    """Round-trip the Expirable mixin fields on documents that adopt it."""

    async def test_user_expirable_defaults(self, beanie_client):
        u = User(
            name='exp_u', email='e@test.com',
            uid=30000, gid=30000, fullname='Exp User',
            home_directory='/home/exp_u',
        )
        await u.insert()
        fetched = await User.find_one(User.name == 'exp_u')
        assert fetched is not None
        assert fetched.expires_at is None
        assert fetched.provisioned_at is None

    async def test_user_expirable_round_trip(self, beanie_client):
        from datetime import datetime, timedelta, timezone
        # Use naive UTC: the test mongo client is tz_aware=False, so reads come
        # back naive — match that on the input side to keep comparisons simple.
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        provisioned = now - timedelta(days=10)
        expires = now + timedelta(days=30)
        u = User(
            name='exp_u2', email='e2@test.com',
            uid=30001, gid=30001, fullname='Exp User 2',
            home_directory='/home/exp_u2',
            provisioned_at=provisioned,
            expires_at=expires,
        )
        await u.insert()
        fetched = await User.find_one(User.name == 'exp_u2')
        assert fetched.provisioned_at is not None
        assert fetched.expires_at is not None
        # Mongo strips sub-millisecond precision; compare with tolerance.
        assert abs((fetched.provisioned_at - provisioned).total_seconds()) < 1
        assert abs((fetched.expires_at - expires).total_seconds()) < 1

    async def test_user_site_info_expirable(self, beanie_client):
        from datetime import datetime, timedelta, timezone
        u = User(
            name='exp_usi', email='usi@test.com',
            uid=30002, gid=30002, fullname='USI Exp',
            home_directory='/home/exp_usi',
        )
        await u.insert()
        site = Site(name='exp_site', fqdn='exp.example.com')
        await site.insert()

        expires = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(days=14)
        usi = UserSiteInfo(user=u, site=site, expires_at=expires)
        await usi.insert()

        fetched = await UserSiteInfo.find_one(
            UserSiteInfo.user.id == u.id,
            UserSiteInfo.site.id == site.id,
        )
        assert fetched.expires_at is not None
        assert abs((fetched.expires_at - expires).total_seconds()) < 1
        assert fetched.provisioned_at is None

    async def test_slurm_allocation_expirable(self, beanie_client):
        from datetime import datetime, timezone
        provisioned = datetime.now(timezone.utc).replace(tzinfo=None)
        alloc = SlurmAllocation(
            tres=SlurmTRES(cpus=8),
            comment='exp test',
            provisioned_at=provisioned,
        )
        await alloc.insert()
        fetched = await SlurmAllocation.get(alloc.id)
        assert fetched.provisioned_at is not None
        assert abs((fetched.provisioned_at - provisioned).total_seconds()) < 1


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

    async def test_group_membership_edge_roundtrip(self, beanie_client):
        from cheeto.models.group_membership import GroupMembership

        user = User(
            name='gmember', email='gm@test.com',
            uid=20000, gid=20000, fullname='G Member',
            home_directory='/home/gmember',
        )
        await user.insert()
        site = Site(name='gmsite', fqdn='gm.test')
        await site.insert()

        group = Group(name='testgroup', gid=30000)
        await group.insert()

        edge = GroupMembership(
            user=user, group=group, site=site,
            roles=['member', 'sponsor'],
        )
        await edge.insert()

        fetched = await GroupMembership.find_one(
            GroupMembership.group.id == group.id,
            GroupMembership.site.id == site.id,
            fetch_links=True,
        )
        assert fetched is not None
        assert fetched.user.name == 'gmember'
        assert fetched.roles == ['member', 'sponsor']

    async def test_membership_roles_deduped_and_validated(self, beanie_client):
        from cheeto.models.group_membership import GroupMembership

        user = User(
            name='rolesuser', email='ru@test.com',
            uid=20009, gid=20009, fullname='Roles User',
            home_directory='/home/rolesuser',
        )
        await user.insert()
        site = Site(name='rolessite', fqdn='roles.test')
        await site.insert()
        group = Group(name='rolesgroup', gid=30009)
        await group.insert()

        # Duplicates collapse and roles sort into canonical order.
        edge = GroupMembership(
            user=user, group=group, site=site,
            roles=['slurmer', 'member', 'member'],
        )
        assert edge.roles == ['member', 'slurmer']

        # Unknown roles are rejected by the MembershipRole Literal itself.
        with pytest.raises(ValueError, match='member.*sponsor.*sudoer.*slurmer'):
            GroupMembership(
                user=user, group=group, site=site, roles=['bogus'],
            )

    async def test_user_memberships_backlink(self, beanie_client):
        from cheeto.models.group_membership import GroupMembership

        user = User(
            name='bluser', email='bl@test.com',
            uid=20001, gid=20001, fullname='BL User',
            home_directory='/home/bluser',
        )
        await user.insert()
        site = Site(name='blsite', fqdn='bl.test')
        await site.insert()

        g1 = Group(name='grp1', gid=30001)
        g2 = Group(name='grp2', gid=30002)
        await g1.insert()
        await g2.insert()
        await GroupMembership(user=user, group=g1, site=site, roles=['member']).insert()
        await GroupMembership(user=user, group=g2, site=site, roles=['member']).insert()

        fetched = await User.find_one(
            User.name == 'bluser', fetch_links=True,
        )
        assert len(fetched.memberships) == 2


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

        active = await status_link('active')
        usi = UserSiteInfo(
            user=user, site=site, status=active,
            access=await access_links(['login-ssh', 'slurm']),
        )
        await usi.insert()

        fetched = await UserSiteInfo.find_one(
            UserSiteInfo.user.id == user.id, fetch_links=True,
        )
        assert fetched is not None
        assert fetched.status is not None
        assert fetched.status.status_name == 'active'
        assert {a.access_name for a in fetched.access} == {'login-ssh', 'slurm'}


class TestStorageModel:

    async def test_storage_quota_sums_allocations(self, beanie_client):
        user = User(
            name='storuser', email='stor@test.com',
            uid=20020, gid=20020, fullname='Stor User',
            home_directory='/home/storuser',
        )
        await user.insert()
        group = Group(name='storuser', gid=20020)
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
        group = Group(name='noalloc', gid=20021)
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
        group = Group(name='qbuser', gid=20022)
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

    async def test_slurm_tres_unlimited_defaults_to_none(self):
        # cpus/gpus default to None (= unlimited)
        t = SlurmTRES()
        assert t.cpus is None
        assert t.gpus is None
        assert t.mem is None
        # slurm_* properties translate None back to -1 for outbound rendering
        assert t.slurm_cpus == -1
        assert t.slurm_gpus == -1
        # to_slurm() emits the sacctmgr-friendly TRES string
        assert t.to_slurm() == 'cpu=-1,mem=-1,gres/gpu=-1'

    async def test_slurm_tres_normalizes_minus_one_to_none(self):
        # -1 (and the string '-1') on input is normalized to None for storage.
        t1 = SlurmTRES(cpus=-1, gpus=-1)
        assert t1.cpus is None
        assert t1.gpus is None
        t2 = SlurmTRES(cpus='-1', gpus='-1')
        assert t2.cpus is None
        assert t2.gpus is None
        # Concrete values are preserved.
        t3 = SlurmTRES(cpus=128, gpus=0)
        assert t3.cpus == 128
        assert t3.slurm_cpus == 128
        assert t3.gpus == 0
        assert t3.slurm_gpus == 0  # zero is a real limit, not unlimited

    async def test_slurm_tres_to_slurm_with_values(self):
        t = SlurmTRES(cpus=128, gpus=8, mem='1T')
        # 1T = 1024*1024 MB = 1048576
        assert t.to_slurm() == 'cpu=128,mem=1048576,gres/gpu=8'

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
        fetched = await User.find_one(
            User.name == 'mutuser', fetch_links=True, nesting_depth=1,
        )
        assert fetched.status is not None
        assert fetched.status.status_name == 'inactive'
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
        fetched = await User.find_one(
            User.name == 'mutuser', fetch_links=True, nesting_depth=1,
        )
        assert 'slurm' in {ag.access_name for ag in fetched.access}

        await RemoveUserAccess.run(
            beanie_client, None,
            name='mutuser', access='slurm',
        )
        fetched = await User.find_one(
            User.name == 'mutuser', fetch_links=True, nesting_depth=1,
        )
        assert 'slurm' not in {ag.access_name for ag in fetched.access}

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
            fetch_links=True, nesting_depth=1,
        )
        assert fetched is not None
        assert fetched.status is not None
        assert fetched.status.status_name == 'active'

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
        from cheeto.queries import group_members_at_site

        user, _ = await CreateUser.run(
            beanie_client, None,
            name='sponsor', email='sp@test.com', uid=43000,
            fullname='Sponsor User',
        )
        site = Site(name='sponsorsite', fqdn='sponsor.test')
        await site.insert()
        group = await CreateGroupFromSponsor.run(
            beanie_client, None, sponsor_name='sponsor', site_name='sponsorsite',
        )
        assert group.name == 'sponsorgrp'
        assert group.type == 'group'

        roster = await group_members_at_site(group, site)
        assert roster['members'] == ['sponsor']
        assert roster['sponsors'] == ['sponsor']

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
        site = Site(name='memsite', fqdn='mem.test')
        await site.insert()
        return user, group, site

    @staticmethod
    async def _roster(group, site):
        from cheeto.queries import group_members_at_site
        return await group_members_at_site(group, site)

    async def test_add_and_remove_member(self, beanie_client, setup):
        user, group, site = setup

        await AddGroupMember.run(
            beanie_client, None,
            group_name='memgroup', user_name='memuser', site_name='memsite',
        )
        assert 'memuser' in (await self._roster(group, site))['members']

        await RemoveGroupMember.run(
            beanie_client, None,
            group_name='memgroup', user_name='memuser', site_name='memsite',
        )
        assert 'memuser' not in (await self._roster(group, site))['members']

    async def test_remove_last_role_deletes_edge(self, beanie_client, setup):
        from cheeto.models.group_membership import GroupMembership
        user, group, site = setup

        await AddGroupMember.run(
            beanie_client, None,
            group_name='memgroup', user_name='memuser', site_name='memsite',
        )
        await RemoveGroupMember.run(
            beanie_client, None,
            group_name='memgroup', user_name='memuser', site_name='memsite',
        )
        edge = await GroupMembership.find_one(
            GroupMembership.user.id == user.id,
            GroupMembership.group.id == group.id,
            GroupMembership.site.id == site.id,
        )
        assert edge is None

    async def test_multiple_roles_share_one_edge(self, beanie_client, setup):
        from cheeto.models.group_membership import GroupMembership
        user, group, site = setup

        await AddGroupMember.run(
            beanie_client, None,
            group_name='memgroup', user_name='memuser', site_name='memsite',
        )
        await AddGroupSponsor.run(
            beanie_client, None,
            group_name='memgroup', user_name='memuser', site_name='memsite',
        )
        edges = await GroupMembership.find(
            GroupMembership.user.id == user.id,
            GroupMembership.group.id == group.id,
            GroupMembership.site.id == site.id,
        ).to_list()
        assert len(edges) == 1
        assert edges[0].roles == ['member', 'sponsor']

    async def test_add_sponsor(self, beanie_client, setup):
        user, group, site = setup
        await AddGroupSponsor.run(
            beanie_client, None,
            group_name='memgroup', user_name='memuser', site_name='memsite',
        )
        assert 'memuser' in (await self._roster(group, site))['sponsors']

    async def test_add_sudoer(self, beanie_client, setup):
        user, group, site = setup
        await AddGroupSudoer.run(
            beanie_client, None,
            group_name='memgroup', user_name='memuser', site_name='memsite',
        )
        assert 'memuser' in (await self._roster(group, site))['sudoers']

    async def test_add_slurmer(self, beanie_client, setup):
        user, group, site = setup
        await AddGroupSlurmer.run(
            beanie_client, None,
            group_name='memgroup', user_name='memuser', site_name='memsite',
        )
        assert 'memuser' in (await self._roster(group, site))['slurmers']


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

    async def test_edit_slurm_allocation_expirable(self, beanie_client, site):
        from datetime import datetime, timedelta, timezone
        await CreateSlurmQOS.run(
            beanie_client, None,
            name='qexp', site_name='slurmsite',
        )
        alloc = await AddQOSAllocation.run(
            beanie_client, None,
            qos_name='qexp', site_name='slurmsite',
            field='group_limits',
            tres=SlurmTRES(cpus=8),
        )
        # Match the test client's tz_aware=False so reads round-trip cleanly.
        provisioned = datetime.now(timezone.utc).replace(tzinfo=None)
        expires = provisioned + timedelta(days=30)

        # Set both timestamps.
        await EditSlurmAllocation.run(
            beanie_client, None,
            allocation_id=str(alloc.id),
            provisioned_at=provisioned,
            expires_at=expires,
        )
        fetched = await SlurmAllocation.get(alloc.id)
        assert fetched.provisioned_at is not None
        assert fetched.expires_at is not None
        assert abs((fetched.provisioned_at - provisioned).total_seconds()) < 1
        assert abs((fetched.expires_at - expires).total_seconds()) < 1
        # Other fields untouched.
        assert fetched.tres.cpus == 8

        # Editing only one field should leave the other intact (UNSET sentinel).
        await EditSlurmAllocation.run(
            beanie_client, None,
            allocation_id=str(alloc.id),
            comment='still has dates',
        )
        fetched = await SlurmAllocation.get(alloc.id)
        assert fetched.comment == 'still has dates'
        assert fetched.expires_at is not None  # unchanged
        assert fetched.provisioned_at is not None

        # Passing None clears the field.
        await EditSlurmAllocation.run(
            beanie_client, None,
            allocation_id=str(alloc.id),
            expires_at=None,
        )
        fetched = await SlurmAllocation.get(alloc.id)
        assert fetched.expires_at is None
        assert fetched.provisioned_at is not None  # still untouched

    async def test_total_tres_sums_allocations(self, beanie_client, site):
        from cheeto.queries.slurm import total_tres

        # Empty list -> unlimited defaults (None for cpus/gpus, None for mem)
        empty = total_tres([])
        assert empty.cpus is None
        assert empty.gpus is None
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

    async def test_edit_slurm_qos_updates_priority_and_flags(self, beanie_client, site):
        await CreateSlurmQOS.run(
            beanie_client, None,
            name='qedit-top', site_name='slurmsite',
            priority=50, flags=['DenyOnLimit'],
        )
        updated = await EditSlurmQOS.run(
            beanie_client, None,
            name='qedit-top', site_name='slurmsite',
            priority=200, flags=['NoDecay', 'DenyOnLimit'],
        )
        assert updated.priority == 200
        assert set(updated.flags) == {'DenyOnLimit', 'NoDecay'}

    async def test_edit_slurm_qos_partial_update(self, beanie_client, site):
        await CreateSlurmQOS.run(
            beanie_client, None,
            name='qedit-part', site_name='slurmsite',
            priority=10, flags=['DenyOnLimit'],
        )
        # Only priority, leave flags alone
        updated = await EditSlurmQOS.run(
            beanie_client, None,
            name='qedit-part', site_name='slurmsite',
            priority=99,
        )
        assert updated.priority == 99
        assert updated.flags == ['DenyOnLimit']

    async def test_remove_slurm_partition_refuses_without_force(
        self, beanie_client, site,
    ):
        import pytest as _pytest
        await CreateSlurmPartition.run(
            beanie_client, None, name='rmpart', site_name='slurmsite',
        )
        # Set up an association so remove is gated
        group = await CreateGroup.run(
            beanie_client, None, name='rmpart-grp', gid=64000,
        )
        from cheeto.models.slurm import SlurmAccount, SlurmAccountLimits
        acc = SlurmAccount(group=group, site=site, limits=SlurmAccountLimits())
        await acc.insert()
        await CreateSlurmQOS.run(
            beanie_client, None, name='rmpart-qos', site_name='slurmsite',
        )
        await CreateSlurmAssociation.run(
            beanie_client, None,
            site_name='slurmsite',
            account_group_name='rmpart-grp',
            partition_name='rmpart', qos_name='rmpart-qos',
        )
        with _pytest.raises(ValueError, match='association'):
            await RemoveSlurmPartition.run(
                beanie_client, None,
                name='rmpart', site_name='slurmsite',
            )

    async def test_remove_slurm_partition_force_cascades(self, beanie_client, site):
        await CreateSlurmPartition.run(
            beanie_client, None, name='rmpartf', site_name='slurmsite',
        )
        group = await CreateGroup.run(
            beanie_client, None, name='rmpartf-grp', gid=64001,
        )
        from cheeto.models.slurm import SlurmAccount, SlurmAccountLimits
        acc = SlurmAccount(group=group, site=site, limits=SlurmAccountLimits())
        await acc.insert()
        await CreateSlurmQOS.run(
            beanie_client, None, name='rmpartf-qos', site_name='slurmsite',
        )
        await CreateSlurmAssociation.run(
            beanie_client, None,
            site_name='slurmsite',
            account_group_name='rmpartf-grp',
            partition_name='rmpartf', qos_name='rmpartf-qos',
        )
        await RemoveSlurmPartition.run(
            beanie_client, None,
            name='rmpartf', site_name='slurmsite', force=True,
        )
        # Partition and its associations are gone
        remaining = await SlurmPartition.find_one(
            SlurmPartition.name == 'rmpartf',
        )
        assert remaining is None

    async def test_remove_slurm_qos_cleans_allocations(self, beanie_client, site):
        qos = await CreateSlurmQOS.run(
            beanie_client, None,
            name='rmqos', site_name='slurmsite',
            group_limits=[
                SlurmAllocation(tres=SlurmTRES(cpus=128), comment='a'),
                SlurmAllocation(tres=SlurmTRES(cpus=64), comment='b'),
            ],
        )
        alloc_ids = [a.id for a in qos.group_limits]

        await RemoveSlurmQOS.run(
            beanie_client, None, name='rmqos', site_name='slurmsite',
        )
        from cheeto.models.slurm import SlurmQOS as _SlurmQOS
        assert (await _SlurmQOS.find_one(_SlurmQOS.name == 'rmqos')) is None
        # Orphaned allocations should also be gone
        for aid in alloc_ids:
            assert (await SlurmAllocation.get(aid)) is None

    async def test_remove_slurm_association_specific(self, beanie_client, site):
        group = await CreateGroup.run(
            beanie_client, None, name='rmassoc-grp', gid=64100,
        )
        from cheeto.models.slurm import SlurmAccount, SlurmAccountLimits
        acc = SlurmAccount(group=group, site=site, limits=SlurmAccountLimits())
        await acc.insert()
        await CreateSlurmPartition.run(
            beanie_client, None, name='rmassoc-part', site_name='slurmsite',
        )
        await CreateSlurmQOS.run(
            beanie_client, None, name='rmassoc-qos', site_name='slurmsite',
        )
        await CreateSlurmAssociation.run(
            beanie_client, None,
            site_name='slurmsite',
            account_group_name='rmassoc-grp',
            partition_name='rmassoc-part', qos_name='rmassoc-qos',
        )
        count = await RemoveSlurmAssociation.run(
            beanie_client, None,
            site_name='slurmsite',
            account_group_name='rmassoc-grp',
            partition_name='rmassoc-part', qos_name='rmassoc-qos',
        )
        assert count == 1

    async def test_remove_slurm_association_all_for_group(
        self, beanie_client, site,
    ):
        # Seed a group with two partitions and two associations on different
        # (partition, qos) combinations.
        group = await CreateGroup.run(
            beanie_client, None, name='rmbulk-grp', gid=64150,
        )
        from cheeto.models.slurm import SlurmAccount, SlurmAccountLimits
        acc = SlurmAccount(group=group, site=site, limits=SlurmAccountLimits())
        await acc.insert()
        for part in ('rmbulk-part-a', 'rmbulk-part-b'):
            await CreateSlurmPartition.run(
                beanie_client, None, name=part, site_name='slurmsite',
            )
        for qos in ('rmbulk-qos-a', 'rmbulk-qos-b'):
            await CreateSlurmQOS.run(
                beanie_client, None, name=qos, site_name='slurmsite',
            )
        for part in ('rmbulk-part-a', 'rmbulk-part-b'):
            for qos in ('rmbulk-qos-a', 'rmbulk-qos-b'):
                await CreateSlurmAssociation.run(
                    beanie_client, None,
                    site_name='slurmsite',
                    account_group_name='rmbulk-grp',
                    partition_name=part, qos_name=qos,
                )

        # site + group only → removes all four
        count = await RemoveSlurmAssociation.run(
            beanie_client, None,
            site_name='slurmsite',
            account_group_name='rmbulk-grp',
        )
        assert count == 4

    async def test_remove_slurm_association_filter_by_partition(
        self, beanie_client, site,
    ):
        group = await CreateGroup.run(
            beanie_client, None, name='rmfilt-grp', gid=64160,
        )
        from cheeto.models.slurm import SlurmAccount, SlurmAccountLimits
        acc = SlurmAccount(group=group, site=site, limits=SlurmAccountLimits())
        await acc.insert()
        for part in ('rmfilt-high', 'rmfilt-low'):
            await CreateSlurmPartition.run(
                beanie_client, None, name=part, site_name='slurmsite',
            )
        await CreateSlurmQOS.run(
            beanie_client, None, name='rmfilt-qos', site_name='slurmsite',
        )
        for part in ('rmfilt-high', 'rmfilt-low'):
            await CreateSlurmAssociation.run(
                beanie_client, None,
                site_name='slurmsite',
                account_group_name='rmfilt-grp',
                partition_name=part, qos_name='rmfilt-qos',
            )

        # Narrow to one partition — only the `rmfilt-high` one goes
        count = await RemoveSlurmAssociation.run(
            beanie_client, None,
            site_name='slurmsite',
            account_group_name='rmfilt-grp',
            partition_name='rmfilt-high',
        )
        assert count == 1

        # The `rmfilt-low` one is still there
        from cheeto.models.slurm import SlurmAssociation as _Assoc
        remaining = await _Assoc.find(
            _Assoc.site.id == site.id,
            _Assoc.account.id == acc.id,
        ).to_list()
        assert len(remaining) == 1

    async def test_remove_slurm_association_no_match_raises(
        self, beanie_client, site,
    ):
        import pytest as _pytest
        group = await CreateGroup.run(
            beanie_client, None, name='rmnomatch-grp', gid=64170,
        )
        from cheeto.models.slurm import SlurmAccount, SlurmAccountLimits
        acc = SlurmAccount(group=group, site=site, limits=SlurmAccountLimits())
        await acc.insert()
        with _pytest.raises(ValueError, match='No matching'):
            await RemoveSlurmAssociation.run(
                beanie_client, None,
                site_name='slurmsite',
                account_group_name='rmnomatch-grp',
            )

    async def test_provision_creates_qos_and_association(self, beanie_client, site):
        group = await CreateGroup.run(
            beanie_client, None, name='prov-grp', gid=64200,
        )
        from cheeto.models.slurm import SlurmAccount, SlurmAccountLimits
        acc = SlurmAccount(group=group, site=site, limits=SlurmAccountLimits())
        await acc.insert()
        await CreateSlurmPartition.run(
            beanie_client, None, name='prov-part', site_name='slurmsite',
        )

        assoc = await ProvisionSlurmAllocation.run(
            beanie_client, None,
            site_name='slurmsite',
            account_group_name='prov-grp',
            partition_name='prov-part',
            group_limits_tres=SlurmTRES(cpus=128, mem='1T'),
            comment='initial',
            priority=100,
        )
        assert assoc.id is not None
        # Auto-generated QOS name
        from cheeto.models.slurm import SlurmQOS as _SlurmQOS
        qos = await _SlurmQOS.find_one(
            _SlurmQOS.name == 'prov-grp-prov-part-qos', fetch_links=True,
        )
        assert qos is not None
        assert qos.priority == 100
        assert len(qos.group_limits) == 1

    async def test_provision_idempotent_when_qos_exists(self, beanie_client, site):
        group = await CreateGroup.run(
            beanie_client, None, name='prov2-grp', gid=64300,
        )
        from cheeto.models.slurm import SlurmAccount, SlurmAccountLimits
        acc = SlurmAccount(group=group, site=site, limits=SlurmAccountLimits())
        await acc.insert()
        await CreateSlurmPartition.run(
            beanie_client, None, name='prov2-part', site_name='slurmsite',
        )
        # Run twice; second call must not create a duplicate association
        assoc1 = await ProvisionSlurmAllocation.run(
            beanie_client, None,
            site_name='slurmsite',
            account_group_name='prov2-grp',
            partition_name='prov2-part',
            qos_name='prov2-qos',
        )
        assoc2 = await ProvisionSlurmAllocation.run(
            beanie_client, None,
            site_name='slurmsite',
            account_group_name='prov2-grp',
            partition_name='prov2-part',
            qos_name='prov2-qos',
        )
        assert assoc1.id == assoc2.id

    async def test_list_qos_and_associations_at_site(self, beanie_client, site):
        from cheeto.queries.slurm import (
            list_qos_at_site, list_associations_at_site, qos_at_site,
        )
        # Two QOSes on this site
        await CreateSlurmQOS.run(
            beanie_client, None, name='q-alpha', site_name='slurmsite',
            priority=10,
        )
        await CreateSlurmQOS.run(
            beanie_client, None, name='q-beta', site_name='slurmsite',
            priority=20,
        )
        qoses = await list_qos_at_site(site)
        names = {q.name for q in qoses}
        assert {'q-alpha', 'q-beta'}.issubset(names)

        one = await qos_at_site(site, 'q-alpha')
        assert one is not None and one.priority == 10
        missing = await qos_at_site(site, 'does-not-exist')
        assert missing is None

        # Set up a group+account+assoc so list_associations returns something
        group = await CreateGroup.run(
            beanie_client, None, name='lsa-grp', gid=64400,
        )
        from cheeto.models.slurm import SlurmAccount, SlurmAccountLimits
        acc = SlurmAccount(group=group, site=site, limits=SlurmAccountLimits())
        await acc.insert()
        await CreateSlurmPartition.run(
            beanie_client, None, name='lsa-part', site_name='slurmsite',
        )
        await CreateSlurmAssociation.run(
            beanie_client, None,
            site_name='slurmsite',
            account_group_name='lsa-grp',
            partition_name='lsa-part', qos_name='q-alpha',
        )
        assocs = await list_associations_at_site(site)
        assert any(
            a.partition.name == 'lsa-part' and a.qos.name == 'q-alpha'
            for a in assocs
        )
        # Filtered by group
        filtered = await list_associations_at_site(site, group=group)
        assert len(filtered) == 1

    async def test_partition_query_helpers(self, beanie_client, site):
        from cheeto.queries.slurm import (
            partition_at_site, list_partitions_at_site,
        )
        for name in ('pq-a', 'pq-b', 'pq-c'):
            await CreateSlurmPartition.run(
                beanie_client, None, name=name, site_name='slurmsite',
            )

        one = await partition_at_site(site, 'pq-a')
        assert one is not None and one.name == 'pq-a'

        missing = await partition_at_site(site, 'pq-nope')
        assert missing is None

        all_parts = await list_partitions_at_site(site)
        names = {p.name for p in all_parts}
        assert {'pq-a', 'pq-b', 'pq-c'}.issubset(names)

    async def test_list_partitions_filtered_by_group(self, beanie_client, site):
        from cheeto.queries.slurm import list_partitions_at_site
        # Two partitions exist; group has an association on only one of them
        await CreateSlurmPartition.run(
            beanie_client, None, name='pgf-yes', site_name='slurmsite',
        )
        await CreateSlurmPartition.run(
            beanie_client, None, name='pgf-no', site_name='slurmsite',
        )
        await CreateSlurmQOS.run(
            beanie_client, None, name='pgf-qos', site_name='slurmsite',
        )
        group = await CreateGroup.run(
            beanie_client, None, name='pgf-grp', gid=64500,
        )
        from cheeto.models.slurm import SlurmAccount, SlurmAccountLimits
        acc = SlurmAccount(group=group, site=site, limits=SlurmAccountLimits())
        await acc.insert()
        await CreateSlurmAssociation.run(
            beanie_client, None,
            site_name='slurmsite',
            account_group_name='pgf-grp',
            partition_name='pgf-yes', qos_name='pgf-qos',
        )

        scoped = await list_partitions_at_site(site, group=group)
        assert {p.name for p in scoped} == {'pgf-yes'}

    async def test_allocation_query_helpers(self, beanie_client, site):
        from cheeto.queries.slurm import (
            list_allocations_at_site, explode_qos_allocations,
        )
        # QOS with two group_limits and one user_limits allocation
        qos = await CreateSlurmQOS.run(
            beanie_client, None,
            name='aq-qos', site_name='slurmsite',
            group_limits=[
                SlurmAllocation(tres=SlurmTRES(cpus=128), comment='primary'),
                SlurmAllocation(tres=SlurmTRES(cpus=64), comment='extension'),
            ],
            user_limits=[
                SlurmAllocation(tres=SlurmTRES(cpus=16), comment='cap'),
            ],
        )
        # Re-fetch with allocations resolved before exploding
        from cheeto.queries.slurm import qos_at_site
        full = await qos_at_site(site, 'aq-qos')
        assert full is not None
        flat = explode_qos_allocations(full)
        assert len(flat) == 3
        assert {qa.field for qa in flat} == {'group_limits', 'user_limits'}

        narrowed = explode_qos_allocations(full, field='user_limits')
        assert len(narrowed) == 1 and narrowed[0].field == 'user_limits'

        # site-wide listing should include them
        all_at_site = await list_allocations_at_site(site)
        ids = {qa.allocation.id for qa in all_at_site if qa.qos.name == 'aq-qos'}
        assert len(ids) == 3

        # Narrow by qos
        only_aq = await list_allocations_at_site(site, qos=full)
        assert len(only_aq) == 3

    async def test_list_allocations_filtered_by_group(self, beanie_client, site):
        from cheeto.queries.slurm import list_allocations_at_site
        # Two QOSes — only one is reachable via the group's associations
        await CreateSlurmQOS.run(
            beanie_client, None,
            name='afg-yes', site_name='slurmsite',
            group_limits=[
                SlurmAllocation(tres=SlurmTRES(cpus=8), comment='a'),
            ],
        )
        await CreateSlurmQOS.run(
            beanie_client, None,
            name='afg-no', site_name='slurmsite',
            group_limits=[
                SlurmAllocation(tres=SlurmTRES(cpus=8), comment='b'),
            ],
        )
        await CreateSlurmPartition.run(
            beanie_client, None, name='afg-part', site_name='slurmsite',
        )
        group = await CreateGroup.run(
            beanie_client, None, name='afg-grp', gid=64600,
        )
        from cheeto.models.slurm import SlurmAccount, SlurmAccountLimits
        acc = SlurmAccount(group=group, site=site, limits=SlurmAccountLimits())
        await acc.insert()
        await CreateSlurmAssociation.run(
            beanie_client, None,
            site_name='slurmsite',
            account_group_name='afg-grp',
            partition_name='afg-part', qos_name='afg-yes',
        )

        scoped = await list_allocations_at_site(site, group=group)
        # Only the yes-QOS contributes
        assert {qa.qos.name for qa in scoped} == {'afg-yes'}
        assert len(scoped) == 1


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


class TestMigrateAccessStatusGroups:
    """Tests for the v1 -> v2 AccessGroup/StatusGroup migration step.

    Connects mongoengine to the same MongoDB the test suite already uses
    (via the existing connect_mongoengine path), drops the v1 group
    collection, then runs the op. The autouse clean_db fixture pre-seeds
    AccessGroup/StatusGroup records for every test, so these tests start
    by deleting those to test the actual migration path.
    """

    @pytest_asyncio.fixture(loop_scope='session')
    async def clean_polymorphic_groups(self, beanie_client):
        """Wipe all groups (Group + AccessGroup + StatusGroup share the
        polymorphic collection) before each test."""
        from cheeto.models.group import Group
        await Group.find_all(with_children=True).delete()

    async def test_no_v1_data_falls_back_to_defaults(
        self, beanie_client, clean_polymorphic_groups,
    ):
        from cheeto.config import MongoConfig
        from cheeto.constants import MIN_SPECIAL_GID
        from cheeto.database import connect_mongoengine
        from cheeto.models.group import AccessGroup, StatusGroup
        from cheeto.operations.group import (
            DEFAULT_ACCESS_GROUPS, DEFAULT_STATUS_GROUPS,
        )

        # Connect mongoengine to the same MongoDB so OldGlobalGroup.objects()
        # works. v1 GlobalGroup collection is empty in this fresh DB.
        mongo_cfg = MongoConfig(
            uri='127.0.0.1', port=MONGODB_PORT, user='', tls=False,
            password='', database='cheeto_migrate_test',
        )
        connect_mongoengine(mongo_cfg, quiet=True)

        # No mapping passed → falls back to the defaults from operations.group
        result = await MigrateAccessStatusGroups.run(
            beanie_client, None,
        )
        assert all(v == 'created' for v in result['access'].values()), result
        assert all(v == 'created' for v in result['status'].values()), result

        # All defaults landed.
        ag_names = {ag.access_name for ag in await AccessGroup.find_all().to_list()}
        sg_names = {sg.status_name for sg in await StatusGroup.find_all().to_list()}
        assert ag_names == {an for an, _ in DEFAULT_ACCESS_GROUPS}
        assert sg_names == {sn for sn, _ in DEFAULT_STATUS_GROUPS}

        # Names match the LDAP groupnames from defaults.
        ag_records = await AccessGroup.find_all().to_list()
        ag_by_access = {ag.access_name: ag for ag in ag_records}
        for access_name, ldap_name in DEFAULT_ACCESS_GROUPS:
            assert ag_by_access[access_name].name == ldap_name

        # All allocated gids land in the special-gid band.
        for ag in ag_records:
            assert ag.gid >= MIN_SPECIAL_GID, ag
        for sg in await StatusGroup.find_all().to_list():
            assert sg.gid >= MIN_SPECIAL_GID, sg

    async def test_idempotent(self, beanie_client, clean_polymorphic_groups):
        from cheeto.config import MongoConfig
        from cheeto.database import connect_mongoengine
        from cheeto.models.group import AccessGroup

        mongo_cfg = MongoConfig(
            uri='127.0.0.1', port=MONGODB_PORT, user='', tls=False,
            password='', database='cheeto_migrate_test',
        )
        connect_mongoengine(mongo_cfg, quiet=True)

        await MigrateAccessStatusGroups.run(beanie_client, None)
        first_count = await AccessGroup.find_all().count()

        # Re-running reports already_exists for everything.
        result = await MigrateAccessStatusGroups.run(beanie_client, None)
        assert all(
            v == 'already_exists' for v in result['access'].values()
        )
        assert all(
            v == 'already_exists' for v in result['status'].values()
        )
        assert await AccessGroup.find_all().count() == first_count

    async def test_legacy_doc_upgraded_and_gid_rebanded(
        self, beanie_client, clean_polymorphic_groups,
    ):
        """A pre-existing legacy doc (no _class_id, gid in SYSTEM_UID band)
        gets stamped with the discriminator, marked with access_name, and
        its gid pulled into the MIN_SPECIAL_GID band — file ownerships
        don't apply to special groups so renumbering is safe."""
        from cheeto.config import MongoConfig
        from cheeto.constants import MIN_SPECIAL_GID, MIN_SYSTEM_UID
        from cheeto.database import connect_mongoengine
        from cheeto.models.group import AccessGroup, Group

        mongo_cfg = MongoConfig(
            uri='127.0.0.1', port=MONGODB_PORT, user='', tls=False,
            password='', database='cheeto_migrate_test',
        )
        connect_mongoengine(mongo_cfg, quiet=True)

        # Seed a legacy-style raw doc directly via pymongo so _class_id
        # stays unset (mirrors data migrated before is_root=True landed).
        coll = Group.get_pymongo_collection()
        legacy_gid = MIN_SYSTEM_UID + 1001  # 4_000_001_001 — classic v1
        await coll.insert_one({
            'name': 'login-ssh-users',
            'gid': legacy_gid,
            'type': 'access',
            '_class_id': None,
            'members': [], 'sponsors': [], 'sudoers': [], 'slurmers': [],
        })

        result = await MigrateAccessStatusGroups.run(beanie_client, None)

        # login-ssh was upgraded (not freshly created).
        assert result['access']['login-ssh'] == 'upgraded'

        ag = await AccessGroup.find_one(
            AccessGroup.access_name == 'login-ssh',
        )
        assert ag is not None
        assert ag.name == 'login-ssh-users'
        assert ag.gid != legacy_gid
        assert ag.gid >= MIN_SPECIAL_GID, (
            f'gid {ag.gid} not pulled out of SYSTEM_UID range'
        )

    async def test_custom_mapping_extends_defaults(
        self, beanie_client, clean_polymorphic_groups,
    ):
        from cheeto.config import MongoConfig
        from cheeto.database import connect_mongoengine
        from cheeto.models.group import AccessGroup, StatusGroup
        from cheeto.operations.group import (
            DEFAULT_ACCESS_GROUPS, DEFAULT_STATUS_GROUPS,
        )

        mongo_cfg = MongoConfig(
            uri='127.0.0.1', port=MONGODB_PORT, user='', tls=False,
            password='', database='cheeto_migrate_test',
        )
        connect_mongoengine(mongo_cfg, quiet=True)

        # Operator-provided non-default mapping (mimics a v1 config with
        # extra shorthand-to-LDAP-name pairs). The defaults are always
        # seeded; the kwargs add to them, they don't replace.
        result = await MigrateAccessStatusGroups.run(
            beanie_client, None,
            access_groups={'gpu-access': 'gpu-users'},
            status_groups={'paused': 'paused-users'},
        )
        # Defaults + the extra got created.
        expected_access = {an for an, _ in DEFAULT_ACCESS_GROUPS} | {'gpu-access'}
        expected_status = {sn for sn, _ in DEFAULT_STATUS_GROUPS} | {'paused'}
        assert set(result['access']) == expected_access
        assert set(result['status']) == expected_status
        assert all(v == 'created' for v in result['access'].values())
        assert all(v == 'created' for v in result['status'].values())

        ag = await AccessGroup.find_one(
            AccessGroup.access_name == 'gpu-access',
        )
        assert ag is not None
        assert ag.name == 'gpu-users'

        sg = await StatusGroup.find_one(
            StatusGroup.status_name == 'paused',
        )
        assert sg is not None
        assert sg.name == 'paused-users'


class TestAccessOverrideSemantics:
    """Override (not union) semantics for User.access vs UserSiteInfo.access.

    Contract: when `usi.access` is non-empty, it replaces `user.access`
    entirely for that site. An empty `usi.access` falls through to the
    global `user.access`.
    """

    async def test_effective_access_links_empty_usi_uses_global(
        self, beanie_client,
    ):
        from cheeto.queries import effective_access_links

        user = User(
            name='effa1', email='effa1@test.com', uid=900001, gid=900001,
            fullname='Effa One', home_directory='/home/effa1',
            access=await access_links(['login-ssh', 'sudo']),
        )
        await user.insert()
        site = Site(name='effa_site', fqdn='effa.test')
        await site.insert()
        usi = UserSiteInfo(
            user=user, site=site,
            status=await status_link('active'),
            access=[],
        )
        await usi.insert()

        result = effective_access_links(user, usi)
        assert {a.access_name for a in result} == {'login-ssh', 'sudo'}

    async def test_effective_access_links_nonempty_usi_overrides(
        self, beanie_client,
    ):
        from cheeto.queries import effective_access_links

        user = User(
            name='effa2', email='effa2@test.com', uid=900002, gid=900002,
            fullname='Effa Two', home_directory='/home/effa2',
            access=await access_links(['login-ssh', 'sudo']),
        )
        await user.insert()
        site = Site(name='effa2_site', fqdn='effa2.test')
        await site.insert()
        # usi overrides global completely: only compute-ssh applies here.
        usi = UserSiteInfo(
            user=user, site=site,
            status=await status_link('active'),
            access=await access_links(['compute-ssh']),
        )
        await usi.insert()

        result = effective_access_links(user, usi)
        assert {a.access_name for a in result} == {'compute-ssh'}
        # sudo is in global but NOT in the override, so it's dropped:
        assert 'sudo' not in {a.access_name for a in result}

    async def test_effective_access_links_none_usi_uses_global(
        self, beanie_client,
    ):
        from cheeto.queries import effective_access_links

        user = User(
            name='effa3', email='effa3@test.com', uid=900003, gid=900003,
            fullname='Effa Three', home_directory='/home/effa3',
            access=await access_links(['login-ssh']),
        )
        await user.insert()

        result = effective_access_links(user, None)
        assert {a.access_name for a in result} == {'login-ssh'}


class TestFindUsersAccessSiteOverride:
    """`find_users(access=..., site=...)` should honor the override
    semantics rather than the older union semantics."""

    async def test_site_scoped_access_filter_with_override(
        self, beanie_client,
    ):
        from cheeto.queries import find_users

        site = Site(name='ovrsite', fqdn='ovr.test')
        await site.insert()

        # User A: global has sudo, no override → effective at site = sudo
        a = User(
            name='ovr_a', email='a@ovr.test', uid=901001, gid=901001,
            fullname='A', home_directory='/home/ovr_a',
            access=await access_links(['sudo']),
        )
        await a.insert()
        await UserSiteInfo(
            user=a, site=site,
            status=await status_link('active'),
            access=[],
        ).insert()

        # User B: global has sudo, but override is [login-ssh] →
        # effective at site = login-ssh (override drops sudo)
        b = User(
            name='ovr_b', email='b@ovr.test', uid=901002, gid=901002,
            fullname='B', home_directory='/home/ovr_b',
            access=await access_links(['sudo']),
        )
        await b.insert()
        await UserSiteInfo(
            user=b, site=site,
            status=await status_link('active'),
            access=await access_links(['login-ssh']),
        ).insert()

        # User C: global has login-ssh only, but override grants sudo →
        # effective at site = sudo
        c = User(
            name='ovr_c', email='c@ovr.test', uid=901003, gid=901003,
            fullname='C', home_directory='/home/ovr_c',
            access=await access_links(['login-ssh']),
        )
        await c.insert()
        await UserSiteInfo(
            user=c, site=site,
            status=await status_link('active'),
            access=await access_links(['sudo']),
        ).insert()

        # User D: has global sudo but NO usi at this site → not at site,
        # should be excluded from a site-scoped filter regardless.
        d = User(
            name='ovr_d', email='d@ovr.test', uid=901004, gid=901004,
            fullname='D', home_directory='/home/ovr_d',
            access=await access_links(['sudo']),
        )
        await d.insert()

        users = await find_users(access='sudo', site='ovrsite')
        names = {u.name for u in users}
        assert names == {'ovr_a', 'ovr_c'}, names

    async def test_global_access_filter_unaffected_by_overrides(
        self, beanie_client,
    ):
        """Without a site filter, only global User.access is consulted —
        per-site overrides don't show up."""
        from cheeto.queries import find_users

        site = Site(name='gblsite', fqdn='gbl.test')
        await site.insert()

        # Global doesn't have slurm; per-site override grants it. Without
        # --site, the user should NOT match `--access slurm`.
        u = User(
            name='gbl_x', email='x@gbl.test', uid=902001, gid=902001,
            fullname='X', home_directory='/home/gbl_x',
            access=await access_links(['login-ssh']),
        )
        await u.insert()
        await UserSiteInfo(
            user=u, site=site,
            status=await status_link('active'),
            access=await access_links(['slurm']),
        ).insert()

        users = await find_users(access='slurm')
        assert all(usr.name != 'gbl_x' for usr in users)


class TestMigrateUserAccessFolding:
    """MigrateUser should fold v1 GlobalUser.access + every SiteUser._access
    into the v2 user.access (the global), then leave UserSiteInfo.access
    empty so the override-not-union semantics applies cleanly."""

    @pytest_asyncio.fixture(loop_scope='session')
    async def clean_polymorphic_groups(self, beanie_client):
        await Group.find_all(with_children=True).delete()
        await User.find_all().delete()
        await UserSiteInfo.find_all().delete()
        await Site.find_all().delete()
        await seed_access_status_groups()

    async def test_global_and_site_accesses_fold_into_global(
        self, beanie_client, clean_polymorphic_groups,
    ):
        from mongoengine import disconnect

        from cheeto.config import MongoConfig
        from cheeto.database import connect_mongoengine
        from cheeto.database.site import Site as OldSite
        from cheeto.database.user import GlobalUser as OldGlobalUser
        from cheeto.database.user import SiteUser as OldSiteUser

        # Drop any prior mongoengine alias so we can rebind to a fresh
        # database name without `A different connection ... was already
        # registered` from earlier tests in the session.
        disconnect()

        mongo_cfg = MongoConfig(
            uri='127.0.0.1', port=MONGODB_PORT, user='', tls=False,
            password='', database='cheeto_migrate_user_test',
        )
        connection = connect_mongoengine(mongo_cfg, quiet=True)
        # Start clean so a prior test run doesn't leave stale rows behind.
        connection.drop_database(mongo_cfg.database)

        # Sites (v1 mongoengine) and v2 beanie equivalents.
        OldSite(sitename='foldfarm', fqdn='foldfarm.test').save()
        OldSite(sitename='foldhive', fqdn='foldhive.test').save()
        await Site(name='foldfarm', fqdn='foldfarm.test').insert()
        await Site(name='foldhive', fqdn='foldhive.test').insert()

        old_user = OldGlobalUser(
            username='folduser', email='fold@test.com',
            uid=910001, gid=910001, fullname='Fold User',
            shell='/bin/bash', home_directory='/home/folduser',
            type='user', status='active',
            access=['login-ssh'],
        )
        old_user.save()

        OldSiteUser(
            username='folduser', sitename='foldfarm', parent=old_user,
            _status='active', _access=['sudo', 'slurm'],
        ).save()
        OldSiteUser(
            username='folduser', sitename='foldhive', parent=old_user,
            _status='active', _access=['root-ssh'],
        ).save()

        await MigrateUser.run(
            beanie_client, None, username='folduser',
        )

        new_user = await User.find_one(User.name == 'folduser')
        assert new_user is not None
        new_user_access = await access_links_to_names(new_user.access)
        # Union of v1 global + every v1 site override.
        assert set(new_user_access) == {
            'login-ssh', 'sudo', 'slurm', 'root-ssh',
        }

        # And every UserSiteInfo carries an empty access list — overrides
        # are intentionally cleared so the global drives effective access.
        usis = await UserSiteInfo.find(
            UserSiteInfo.user.id == new_user.id,
        ).to_list()
        assert len(usis) == 2
        for usi in usis:
            assert usi.access == []

        connection.drop_database(mongo_cfg.database)


class TestSiteStickyValidation:
    """`SiteSlurmSettings.default_account` must appear in `sticky`."""

    async def _seed(self):
        site = Site(name='vsf', fqdn='vsf.test')
        await site.insert()
        group = Group(name='gvalsticky', gid=42424)
        await group.insert()
        account = SlurmAccount(group=group, site=site)
        await account.insert()
        return site, group, account

    async def test_default_must_be_in_sticky(self, beanie_client):
        from cheeto.models.site import SiteSlurmSettings
        _, _, account = await self._seed()
        with pytest.raises(ValueError, match='default_account'):
            SiteSlurmSettings(sticky=[], default_account=account)

    async def test_default_in_sticky_validates(self, beanie_client):
        from cheeto.models.site import SiteSlurmSettings
        _, _, account = await self._seed()
        settings = SiteSlurmSettings(sticky=[account], default_account=account)
        assert settings.default_account is not None

    async def test_in_place_mutation_caught_at_save(self, beanie_client):
        from cheeto.models.site import SiteSlurmSettings
        site, _, account = await self._seed()
        site.slurm = SiteSlurmSettings(
            sticky=[account], default_account=account,
        )
        await site.save()

        # Mutate in place: drop sticky out from under default_account.
        site.slurm.sticky.clear()
        with pytest.raises(ValueError, match='default_account'):
            await site.save()


class TestEffectiveGroupMembers:
    """`effective_group_members(group, site)` unions in users at the site
    when the group is in `site.group.sticky`."""

    async def test_non_sticky_returns_only_direct_members(self, beanie_client):
        from cheeto.queries.group import effective_group_members
        site = Site(name='egmsiteA', fqdn='a.test')
        await site.insert()

        u_member = User(
            name='egm_direct', email='d@x.test',
            uid=51001, gid=51001, fullname='Direct',
            home_directory='/home/d',
            status=await status_link('active'),
            access=await access_links(['login-ssh']),
        )
        u_at_site = User(
            name='egm_at_site', email='s@x.test',
            uid=51002, gid=51002, fullname='At Site',
            home_directory='/home/s',
            status=await status_link('active'),
            access=await access_links(['login-ssh']),
        )
        await u_member.insert()
        await u_at_site.insert()
        await UserSiteInfo(
            user=u_at_site, site=site,
            status=await status_link('active'),
        ).insert()

        group = Group(name='egm_grp', gid=52000)
        await group.insert()
        await GroupMembership(
            user=u_member, group=group, site=site, roles=['member'],
        ).insert()

        ids = await effective_group_members(group, site)
        assert ids == {u_member.id}

    async def test_sticky_unions_in_users_at_site(self, beanie_client):
        from cheeto.queries.group import effective_group_members
        site = Site(name='egmsiteB', fqdn='b.test')
        await site.insert()

        u_direct = User(
            name='egmB_direct', email='d@x.test',
            uid=51003, gid=51003, fullname='Direct',
            home_directory='/home/d',
            status=await status_link('active'),
            access=await access_links(['login-ssh']),
        )
        u_at_site = User(
            name='egmB_at_site', email='s@x.test',
            uid=51004, gid=51004, fullname='At Site',
            home_directory='/home/s',
            status=await status_link('active'),
            access=await access_links(['login-ssh']),
        )
        await u_direct.insert()
        await u_at_site.insert()
        await UserSiteInfo(
            user=u_at_site, site=site,
            status=await status_link('active'),
        ).insert()
        # u_direct has no USI at this site; only via direct membership.

        group = Group(name='egmB_grp', gid=52001)
        await group.insert()
        await GroupMembership(
            user=u_direct, group=group, site=site, roles=['member'],
        ).insert()

        # Make group sticky on the site.
        site.group.sticky = [group]
        await site.save()
        # Re-fetch to ensure sticky list reads back as Link references.
        site = await Site.find_one(Site.name == 'egmsiteB')

        ids = await effective_group_members(group, site)
        assert ids == {u_direct.id, u_at_site.id}


class TestUserSlurmAtSiteWithSticky:
    """A user at a site with no group memberships still gets accounts via
    `site.slurm.sticky` (role='sticky')."""

    async def test_sticky_account_appears_for_user_at_site(self, beanie_client):
        from cheeto.queries.slurm import user_slurm_at_site
        site = Site(name='usswsite', fqdn='uss.test')
        await site.insert()

        user = User(
            name='uss_user', email='u@x.test',
            uid=53001, gid=53001, fullname='U',
            home_directory='/home/u',
            status=await status_link('active'),
            access=await access_links(['login-ssh', 'slurm']),
        )
        await user.insert()
        await UserSiteInfo(
            user=user, site=site,
            status=await status_link('active'),
        ).insert()

        sticky_group = Group(name='uss_sticky_grp', gid=54000)
        await sticky_group.insert()
        account = SlurmAccount(group=sticky_group, site=site)
        await account.insert()

        site.slurm.sticky = [account]
        await site.save()
        site = await Site.find_one(Site.name == 'usswsite')

        results = await user_slurm_at_site(user, site)
        assert len(results) == 1
        assert results[0].role == 'sticky'
        assert results[0].group.name == 'uss_sticky_grp'

    async def test_no_usi_means_no_sticky_rows(self, beanie_client):
        """User not at the site doesn't pick up sticky accounts even when
        they exist."""
        from cheeto.queries.slurm import user_slurm_at_site
        site = Site(name='usswsite2', fqdn='uss2.test')
        await site.insert()

        user = User(
            name='uss_outsider', email='o@x.test',
            uid=53002, gid=53002, fullname='Out',
            home_directory='/home/o',
            status=await status_link('active'),
            access=await access_links(['login-ssh']),
        )
        await user.insert()
        # NO UserSiteInfo at this site.

        sticky_group = Group(name='uss_sticky_grp2', gid=54001)
        await sticky_group.insert()
        account = SlurmAccount(group=sticky_group, site=site)
        await account.insert()
        site.slurm.sticky = [account]
        await site.save()

        results = await user_slurm_at_site(user, site)
        assert results == []

    async def test_sticky_group_account_surfaces_for_user_at_site(
        self, beanie_client,
    ):
        """A user at a site picks up the SlurmAccount of any group in
        `site.group.sticky` with role `sticky-member`."""
        from cheeto.queries.slurm import user_slurm_at_site
        site = Site(name='usswsite3', fqdn='uss3.test')
        await site.insert()

        user = User(
            name='uss_grp_user', email='g@x.test',
            uid=53003, gid=53003, fullname='Grp',
            home_directory='/home/g',
            status=await status_link('active'),
            access=await access_links(['login-ssh', 'slurm']),
        )
        await user.insert()
        await UserSiteInfo(
            user=user, site=site,
            status=await status_link('active'),
        ).insert()

        sticky_group = Group(name='uss_sticky_grp3', gid=54002)
        await sticky_group.insert()
        account = SlurmAccount(group=sticky_group, site=site)
        await account.insert()

        site.group.sticky = [sticky_group]
        await site.save()
        site = await Site.find_one(Site.name == 'usswsite3')

        results = await user_slurm_at_site(user, site)
        assert len(results) == 1
        assert results[0].role == 'sticky-member'
        assert results[0].group.name == 'uss_sticky_grp3'
        assert results[0].slurm.account.id == account.id


async def access_links_to_names(links) -> list[str]:
    """Helper for tests: resolve a list of Link[AccessGroup] (or fetched
    AccessGroup) into access_name strings."""
    from cheeto.queries.access_status import resolve_access_names
    return await resolve_access_names(links)


class TestSiteToPuppetLegacy:
    """`site_to_puppet_legacy(site)` reproduces v1's site_to_puppet output
    shape from v2 data. Storage/share blocks are intentionally empty."""

    async def test_empty_site_yields_empty_maps(self, beanie_client):
        from cheeto.queries import site_to_puppet_legacy
        from cheeto.puppet import PuppetAccountMap

        site = Site(name='puppet_empty', fqdn='empty.test')
        await site.insert()

        result = await site_to_puppet_legacy(site)
        assert result.user == {}
        assert result.group == {}
        assert result.share == {}
        # Marshmallow serialization is exercised in test_puppet.py;
        # here just verify the dump path doesn't choke on the empty map.
        assert PuppetAccountMap.Schema().dumps(result) == '{}\n'

    async def test_user_and_group_basic_fields(self, beanie_client):
        from cheeto.queries import site_to_puppet_legacy

        site = Site(name='puppet_basic', fqdn='basic.test')
        await site.insert()

        user = User(
            name='puppet_alice', email='alice@x.test',
            uid=60001, gid=60001, fullname='Alice',
            home_directory='/home/alice', shell='/bin/zsh',
            status=await status_link('active'),
            access=await access_links(['login-ssh', 'sudo']),
        )
        await user.insert()
        await UserSiteInfo(
            user=user, site=site,
            status=await status_link('active'),
        ).insert()
        # Primary group: same name as user, gid == uid → excluded from
        # both the user's `groups` list and the top-level `group:` map,
        # even when an explicit member edge points at it.
        primary = Group(name='puppet_alice', gid=60001)
        await primary.insert()
        await GroupMembership(
            user=user, group=primary, site=site, roles=['member'],
        ).insert()
        # Regular group: should land in user.groups and in the group: map.
        lab = Group(name='puppet_lab', gid=61000)
        await lab.insert()
        await GroupMembership(
            user=user, group=lab, site=site, roles=['member'],
        ).insert()

        result = await site_to_puppet_legacy(site)
        assert set(result.user.keys()) == {'puppet_alice'}
        rec = result.user['puppet_alice']
        assert rec.fullname == 'Alice'
        assert rec.email == 'alice@x.test'
        assert rec.uid == 60001
        assert rec.gid == 60001
        assert rec.shell == '/bin/zsh'
        assert rec.home == '/home/alice'
        assert rec.groups == ['puppet_lab']  # primary excluded
        assert rec.tag == ['sudo-tag']
        assert rec.slurm is None

        assert set(result.group.keys()) == {'puppet_lab'}
        assert result.group['puppet_lab'].gid == 61000

    async def test_sticky_group_surfaces_in_user_groups(self, beanie_client):
        from cheeto.queries import site_to_puppet_legacy

        site = Site(name='puppet_stickyg', fqdn='stickyg.test')
        await site.insert()

        user = User(
            name='pl_bob', email='b@x.test',
            uid=60002, gid=60002, fullname='Bob',
            home_directory='/home/b',
            status=await status_link('active'),
            access=await access_links(['login-ssh']),
        )
        await user.insert()
        sticky_g = Group(name='pl_sticky', gid=61001)
        await sticky_g.insert()  # bob is NOT in members

        await UserSiteInfo(
            user=user, site=site,
            status=await status_link('active'),
        ).insert()

        site.group.sticky = [sticky_g]
        await site.save()
        site = await Site.find_one(Site.name == 'puppet_stickyg')

        result = await site_to_puppet_legacy(site)
        assert result.user['pl_bob'].groups == ['pl_sticky']

    async def test_sticky_slurm_account_in_user_slurm(self, beanie_client):
        from cheeto.queries import site_to_puppet_legacy

        site = Site(name='puppet_stickys', fqdn='stickys.test')
        await site.insert()

        user = User(
            name='pl_carol', email='c@x.test',
            uid=60003, gid=60003, fullname='Carol',
            home_directory='/home/c',
            status=await status_link('active'),
            access=await access_links(['login-ssh', 'slurm']),
        )
        await user.insert()
        slurm_g = Group(name='pl_slurm_grp', gid=61002)
        await slurm_g.insert()
        account = SlurmAccount(group=slurm_g, site=site)
        await account.insert()

        await UserSiteInfo(
            user=user, site=site,
            status=await status_link('active'),
        ).insert()
        site.slurm.sticky = [account]
        await site.save()
        site = await Site.find_one(Site.name == 'puppet_stickys')

        result = await site_to_puppet_legacy(site)
        assert result.user['pl_carol'].slurm is not None
        assert result.user['pl_carol'].slurm.account == ['pl_slurm_grp']

    async def test_inactive_user_keeps_raw_shell(self, beanie_client):
        """v1 has dead-code shell translation; we match v1 verbatim and
        emit the raw shell even for inactive users."""
        from cheeto.queries import site_to_puppet_legacy

        site = Site(name='puppet_inactive', fqdn='inactive.test')
        await site.insert()

        user = User(
            name='pl_dave', email='d@x.test',
            uid=60004, gid=60004, fullname='Dave',
            home_directory='/home/d', shell='/bin/zsh',
            status=await status_link('inactive'),
            access=await access_links(['login-ssh']),
        )
        await user.insert()
        await UserSiteInfo(
            user=user, site=site,
            status=await status_link('inactive'),
        ).insert()

        result = await site_to_puppet_legacy(site)
        assert result.user['pl_dave'].shell == '/bin/zsh'

    async def test_sponsor_not_at_site_still_resolves(self, beanie_client):
        """A group's sponsor doesn't have to have a UserSiteInfo at the
        site; sponsor names are emitted regardless."""
        from cheeto.queries import site_to_puppet_legacy

        site = Site(name='puppet_sponsor', fqdn='sponsor.test')
        await site.insert()

        sponsor = User(
            name='pl_sponsor', email='s@x.test',
            uid=60005, gid=60005, fullname='Sponsor',
            home_directory='/home/s',
            status=await status_link('active'),
            access=await access_links(['login-ssh']),
        )
        await sponsor.insert()  # no USI at this site

        member = User(
            name='pl_member', email='m@x.test',
            uid=60006, gid=60006, fullname='Member',
            home_directory='/home/m',
            status=await status_link('active'),
            access=await access_links(['login-ssh']),
        )
        await member.insert()
        await UserSiteInfo(
            user=member, site=site,
            status=await status_link('active'),
        ).insert()

        lab = Group(name='pl_lab', gid=61003)
        await lab.insert()
        await GroupMembership(
            user=member, group=lab, site=site, roles=['member'],
        ).insert()
        # Sponsor edge at this site even though the sponsor has no USI here.
        await GroupMembership(
            user=sponsor, group=lab, site=site, roles=['sponsor'],
        ).insert()

        result = await site_to_puppet_legacy(site)
        assert 'pl_sponsor' not in result.user  # not at site
        assert result.group['pl_lab'].sponsors == ['pl_sponsor']


class _FakeLDAPManager:
    """Minimal stand-in for AsyncLDAPManager covering only what
    SyncUserToLDAP touches. Records add/remove-group calls and serves a
    seeded current-membership set so we can assert the reconcile decisions
    without a live directory."""

    def __init__(self, current_memberships: set[str]):
        self._current = set(current_memberships)
        self.added: list[str] = []
        self.removed: list[str] = []

    def user_dn(self, username: str) -> str:
        return f'uid={username},ou=people'

    async def delete_user(self, username: str) -> None:
        pass

    async def update_user(self, record) -> None:
        # Pretend the user already exists → 'updated' path.
        return None

    async def add_user(self, record) -> None:
        pass

    async def list_user_memberships(self, username: str) -> set[str]:
        return set(self._current)

    async def add_users_to_group(self, group, usernames, verify_users=True):
        self.added.append(group)

    async def remove_users_from_group(self, group, usernames):
        self.removed.append(group)


class TestSyncUserToLDAPMembership:
    """SyncUserToLDAP must keep the user in their per-site posix groups; a
    single-user sync previously stripped them because only access/status
    groups were in the target set."""

    async def _seed_user_on_site(self):
        user = User(
            name='ldapu', email='l@x.test',
            uid=70001, gid=70001, fullname='LDAP User',
            home_directory='/home/ldapu',
            status=await status_link('active'),
            access=await access_links(['login-ssh']),
        )
        await user.insert()
        site = Site(name='ldapsite', fqdn='ldap.test')
        await site.insert()
        await UserSiteInfo(
            user=user, site=site, status=await status_link('active'),
        ).insert()
        lab = Group(name='labgrp', gid=71000)
        await lab.insert()
        await GroupMembership(
            user=user, group=lab, site=site, roles=['member'],
        ).insert()
        return user, site, lab

    async def test_posix_membership_preserved(self, beanie_client):
        user, site, lab = await self._seed_user_on_site()
        # LDAP already has the user in their lab group and the login-ssh
        # access group; the status group is missing.
        ldap = _FakeLDAPManager({'labgrp', 'login-ssh-users'})

        result = await SyncUserToLDAP.run(
            beanie_client, None,
            username='ldapu', sitename='ldapsite', ldap=ldap,
        )

        # The lab group must NOT be stripped, and the missing status group
        # gets added.
        assert 'labgrp' not in result.extra['removed_groups']
        assert ldap.removed == []
        assert 'active-users' in result.extra['added_groups']

    async def test_stale_group_still_removed(self, beanie_client):
        user, site, lab = await self._seed_user_on_site()
        # LDAP has the user in a group they no longer belong to anywhere.
        ldap = _FakeLDAPManager(
            {'labgrp', 'login-ssh-users', 'active-users', 'oldgrp'},
        )

        result = await SyncUserToLDAP.run(
            beanie_client, None,
            username='ldapu', sitename='ldapsite', ldap=ldap,
        )

        assert result.extra['removed_groups'] == ['oldgrp']
        assert 'labgrp' not in result.extra['removed_groups']
