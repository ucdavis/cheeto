"""Tests for beanie models and operations.

Requires the session-scoped start_mongodb fixture from conftest.py
which starts an ephemeral mongod with replica set support.
"""

import datetime
import os

import pytest
import pytest_asyncio
from pymongo import AsyncMongoClient
from beanie import init_beanie

# All async tests and fixtures share a single event loop for the session,
# so the AsyncMongoClient created in beanie_client remains valid across tests.

from ..models import ALL_MODELS
from ..models.history import History
from ..models.ldap_sync import LDAPInfo
from ..models.site import Site
from ..models.user import UCDIAMInfo, User, SshKey
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
    QuobyteConfig,
    StaticMount,
    Storage,
    StorageAllocation,
    StorageVolume,
    ZFSConfig,
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
    CreateAutomountMap,
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
    SetStorageMount,
    SetVolumeStorageMounts,
    SetSiteDefaultSlurmAccount,
    ClearSiteDefaultSlurmAccount,
    ExportPuppetStorage,
    ExportRootSSHKeys,
    ExportSympaEmails,
    RemoveSite,
    SyncGroupToLDAP,
    SyncSiteAutomounts,
    SyncSiteLDAP,
    SyncSlurm,
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


async def _seed_storage_actors(prefix: str, uid: int):
    """User + personal group + site for storage tests."""
    user = User(
        name=prefix, email=f'{prefix}@test.com',
        uid=uid, gid=uid, fullname=prefix,
        home_directory=f'/home/{prefix}',
    )
    await user.insert()
    group = Group(name=prefix, gid=uid)
    await group.insert()
    site = Site(name=f'{prefix}site', fqdn=f'{prefix}.hpc.test')
    await site.insert()
    return user, group, site


async def _seed_volume(site, name='nas01vol', host='nas01',
                       host_path='/nas01/vol', backend='zfs', **kwargs):
    volume = StorageVolume(
        name=name, site=site, backend=backend,
        host=host, host_path=host_path,
        **({'zfs': ZFSConfig()} if backend == 'zfs'
           else {'quobyte': QuobyteConfig()}),
        **kwargs,
    )
    await volume.insert()
    return volume


class TestStorageModel:

    async def test_volume_quota_sums_allocations(self, beanie_client):
        _, _, site = await _seed_storage_actors('storuser', 20020)
        await _seed_volume(
            site, allocations=[
                StorageAllocation(quota='100G', comment='initial'),
                StorageAllocation(quota='50G', comment='expansion'),
            ],
        )
        fetched = await StorageVolume.find_one(StorageVolume.name == 'nas01vol')
        assert fetched.quota == '150G'

    async def test_volume_quota_fractional_roundtrip(self, beanie_client):
        """Fractional quotas must render exactly: the float/int megs round
        trip turned 45.8T into 45.79999924T."""
        _, _, site = await _seed_storage_actors('fracq', 20026)
        volume = await _seed_volume(
            site, allocations=[StorageAllocation(quota='45.8T')],
        )
        assert volume.quota == '45.8T'

    async def test_root_vs_subpath_storage_quota(self, beanie_client):
        user, group, site = await _seed_storage_actors('subq', 20021)
        volume = await _seed_volume(
            site, allocations=[StorageAllocation(quota='70T')],
        )
        root = Storage(
            name='rootstor', site=site, category='group',
            owner=user, group=group, volume=volume, subpath='',
        )
        sub = Storage(
            name='substor', site=site, category='home',
            owner=user, group=group, volume=volume, subpath='subq',
        )
        await root.insert()
        await sub.insert()

        # `volume` is the fetched document on the in-memory objects.
        assert root.quota == '70T'
        assert sub.quota is None
        assert root.host_path == '/nas01/vol'
        assert sub.host_path == '/nas01/vol/subq'
        assert sub.host == 'nas01'

    async def test_unmounted_quobyte_storage(self, beanie_client):
        user, group, site = await _seed_storage_actors('qbuser', 20022)
        volume = await _seed_volume(
            site, name='qbvol', host='qb01', host_path='/qb/vol',
            backend='quobyte',
        )
        storage = Storage(
            name='qbuser', site=site, category='home',
            owner=user, group=group, volume=volume,
        )
        await storage.insert()
        assert volume.quota is None
        assert storage.mount_path == ''
        assert storage.mount_options == []

    async def test_backend_config_exclusivity(self, beanie_client):
        _, _, site = await _seed_storage_actors('bce', 20023)
        with pytest.raises(ValueError, match='QuobyteConfig'):
            StorageVolume(
                name='bad', site=site, backend='zfs',
                host='h', host_path='/p', quobyte=QuobyteConfig(),
            )
        with pytest.raises(ValueError, match='ZFSConfig'):
            StorageVolume(
                name='bad2', site=site, backend='quobyte',
                host='h', host_path='/p', zfs=ZFSConfig(),
            )
        # in-place mutation caught at save via before_event
        volume = await _seed_volume(site)
        volume.quobyte = QuobyteConfig()
        with pytest.raises(ValueError, match='QuobyteConfig'):
            await volume.save()

    async def test_mount_mechanism_exclusivity(self, beanie_client):
        user, group, site = await _seed_storage_actors('mme', 20024)
        volume = await _seed_volume(site)
        amap = AutomountMap(name='home', site=site, prefix='/home')
        await amap.insert()
        smount = StaticMount(
            name='home', site=site, fstype='nfs4',
            volume=volume, mount_path='/home',
        )
        await smount.insert()

        with pytest.raises(ValueError, match='not both'):
            Storage(
                name='bad', site=site, category='home',
                owner=user, group=group, volume=volume,
                automount_map=amap, static_mount=smount,
            )
        with pytest.raises(ValueError, match='mount_name'):
            Storage(
                name='bad2', site=site, category='home',
                owner=user, group=group, volume=volume,
                static_mount=smount, mount_name='nope',
            )
        # mutate-then-save hole
        storage = Storage(
            name='ok', site=site, category='home',
            owner=user, group=group, volume=volume,
            automount_map=amap, mount_name='ok',
        )
        await storage.insert()
        storage.static_mount = smount
        with pytest.raises(ValueError, match='not both'):
            await storage.save()

    async def test_automount_mount_path_and_options(self, beanie_client):
        user, group, site = await _seed_storage_actors('amnt', 20025)
        volume = await _seed_volume(site)
        amap = AutomountMap(
            name='group', site=site, prefix='/group',
            options=['fstype=nfs', 'vers=4.2'],
        )
        await amap.insert()

        merged = Storage(
            name='merged', site=site, category='group',
            owner=user, group=group, volume=volume,
            automount_map=amap, mount_name='mergedmnt',
            mount_overrides=MountOverrides(
                add_options=['actimeo=60'], remove_options=['fstype=nfs'],
            ),
        )
        replaced = Storage(
            name='replaced', site=site, category='share',
            owner=user, group=group, volume=volume,
            automount_map=amap,
            mount_overrides=MountOverrides(options=['rw', 'nosuid']),
        )
        assert merged.mount_path == '/group/mergedmnt'
        assert merged.mount_options == ['actimeo=60', 'vers=4.2']
        assert replaced.mount_path == '/group/replaced'  # falls back to name
        assert replaced.mount_options == ['rw', 'nosuid']

    async def test_static_mount_path_math(self, beanie_client):
        user, group, site = await _seed_storage_actors('smath', 20026)
        central = await _seed_volume(
            site, name='home', host='192.168.211.74',
            host_path='/flash/export/home',
        )
        child = await _seed_volume(
            site, name='home/smath', host='192.168.211.74',
            host_path='/flash/export/home/smath',
            parent=central,
        )
        smount = StaticMount(
            name='home', site=site, fstype='nfs4',
            volume=central, mount_path='/home',
            options=['defaults', '_netdev', 'vers=4.2'],
        )
        await smount.insert()

        child_storage = Storage(
            name='smath', site=site, category='home',
            owner=user, group=group, volume=child,
            static_mount=smount,
        )
        assert child_storage.mount_path == '/home/smath'
        assert child_storage.mount_options == ['defaults', '_netdev', 'vers=4.2']

        # equal path → the mount path itself
        root_storage = Storage(
            name='homeroot', site=site, category='share',
            owner=user, group=group, volume=central,
            static_mount=smount,
        )
        assert root_storage.mount_path == '/home'

        # not under the mount → error
        elsewhere = await _seed_volume(
            site, name='other', host='192.168.211.74',
            host_path='/flash/export/scratch',
        )
        bad = Storage(
            name='bad', site=site, category='share',
            owner=user, group=group, volume=elsewhere,
            static_mount=smount,
        )
        with pytest.raises(ValueError, match='not under'):
            bad.mount_path

        # spec-only static mount → error
        cvmfs = StaticMount(
            name='cvmfs', site=site, fstype='cvmfs',
            spec='cvmfs-config.cern.ch',
            mount_path='/cvmfs/cvmfs-config.cern.ch',
        )
        await cvmfs.insert()
        specbad = Storage(
            name='specbad', site=site, category='share',
            owner=user, group=group, volume=central,
            static_mount=cvmfs,
        )
        with pytest.raises(ValueError, match='spec-only'):
            specbad.mount_path

    async def test_static_mount_validators(self, beanie_client):
        _, _, site = await _seed_storage_actors('smv', 20027)
        volume = await _seed_volume(site)

        with pytest.raises(ValueError, match='volume or spec'):
            StaticMount(name='neither', site=site, fstype='nfs4',
                        mount_path='/x')
        with pytest.raises(ValueError, match='mutually exclusive'):
            StaticMount(name='both', site=site, fstype='nfs4',
                        volume=volume, spec='spec', mount_path='/x')
        with pytest.raises(ValueError, match='requires volume'):
            StaticMount(name='subnospec', site=site, fstype='cvmfs',
                        spec='repo.cern.ch', subpath='sub', mount_path='/x')
        with pytest.raises(ValueError, match='Invalid mount fstype'):
            StaticMount(name='badfs', site=site, fstype='ext4',
                        volume=volume, mount_path='/x')
        # cvmfs spec mount is fine, and device_spec returns the raw spec
        ok = StaticMount(
            name='cvmfs', site=site, fstype='cvmfs',
            spec='cvmfs-config.cern.ch',
            mount_path='/cvmfs/cvmfs-config.cern.ch',
        )
        assert ok.device_spec == 'cvmfs-config.cern.ch'
        assert ok.host_path == ''
        # volume-backed device_spec
        vm = StaticMount(
            name='vmnt', site=site, fstype='nfs4',
            volume=volume, subpath='share', mount_path='/share',
        )
        assert vm.device_spec == 'nas01:/nas01/vol/share'

    async def test_require_fetched_raises_on_unfetched(self, beanie_client):
        user, group, site = await _seed_storage_actors('unf', 20028)
        volume = await _seed_volume(site)
        storage = Storage(
            name='unf', site=site, category='home',
            owner=user, group=group, volume=volume,
        )
        await storage.insert()
        # plain find (no fetch_links) leaves volume as a Link proxy
        fetched = await Storage.find_one(Storage.name == 'unf')
        with pytest.raises(RuntimeError, match='unfetched Link'):
            fetched.host
        # fetched query works
        resolved = await Storage.find_one(
            Storage.name == 'unf', fetch_links=True, nesting_depth=1,
        )
        assert resolved.host == 'nas01'

    async def test_legacy_farm_round_trip(self, beanie_client):
        """The acid test: one 70T group ZFS volume backing three storages —
        its root, a subdir group mount, and a subdir legacy home."""
        owner, group, site = await _seed_storage_actors('ajfinger', 20029)
        volume = await _seed_volume(
            site, name='ajfingergrp', host='nas-4-1',
            host_path='/nas-4-1/ajfingergrp',
            allocations=[StorageAllocation(quota='70T')],
        )
        group_map = AutomountMap(
            name='group', site=site, prefix='/group',
            options=['fstype=nfs', 'actimeo=60', 'vers=4.2'],
        )
        home_map = AutomountMap(
            name='home', site=site, prefix='/home',
            options=['fstype=nfs', 'vers=4.2', 'actimeo=60'],
        )
        await group_map.insert()
        await home_map.insert()

        root = Storage(
            name='ajfingerroot', site=site, category='group',
            owner=owner, group=group, volume=volume, subpath='',
            automount_map=group_map, mount_name='ajfingerroot',
        )
        subdir = Storage(
            name='ajfingergrp', site=site, category='group',
            owner=owner, group=group, volume=volume, subpath='ajfingergrp',
            automount_map=group_map, mount_name='ajfingergrp',
        )
        home = Storage(
            name='maccamp', site=site, category='home',
            owner=owner, group=group, volume=volume, subpath='maccamp',
            automount_map=home_map, mount_name='maccamp',
        )
        for s in (root, subdir, home):
            await s.insert()

        assert root.quota == '70T'
        assert root.host_path == '/nas-4-1/ajfingergrp'
        assert root.mount_path == '/group/ajfingerroot'

        assert subdir.quota is None
        assert subdir.host_path == '/nas-4-1/ajfingergrp/ajfingergrp'
        assert subdir.mount_path == '/group/ajfingergrp'

        assert home.quota is None
        assert home.host_path == '/nas-4-1/ajfingergrp/maccamp'
        assert home.mount_path == '/home/maccamp'


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

        # Fractional sizes round-trip exactly (float arithmetic rendered
        # 45.8T as 45.79999924T)
        frac = total_tres([SlurmAllocation(tres=SlurmTRES(mem='45.8T'))])
        assert frac.mem == '45.8T'
        frac_sum = total_tres([
            SlurmAllocation(tres=SlurmTRES(mem='45.8T')),
            SlurmAllocation(tres=SlurmTRES(mem='204.8G')),
        ])
        assert frac_sum.mem == '46T'

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


async def _special_group_names() -> set[str]:
    from cheeto.models.group import AccessGroup, StatusGroup
    return (
        {g.name for g in await AccessGroup.find_all().to_list()}
        | {g.name for g in await StatusGroup.find_all().to_list()}
    )


class TestSiteToPuppetLegacy:
    """`site_to_puppet_legacy(site)` reproduces v1's site_to_puppet output
    shape from v2 data. Storage/share blocks are covered separately in
    TestSiteToPuppetLegacyStorage."""

    async def test_empty_site_yields_empty_maps(self, beanie_client):
        from cheeto.queries import site_to_puppet_legacy
        from cheeto.puppet import PuppetAccountMap

        site = Site(name='puppet_empty', fqdn='empty.test')
        await site.insert()

        result = await site_to_puppet_legacy(site)
        assert result.user == {}
        # Even an empty site exports the (global) special groups — v1
        # carried their SiteGroups everywhere.
        assert set(result.group.keys()) == await _special_group_names()
        assert result.share == {}
        # Marshmallow serialization is exercised in test_puppet.py;
        # here just verify the dump path doesn't choke.
        PuppetAccountMap.Schema().dumps(result)

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

        assert set(result.group.keys()) == (
            {'puppet_lab'} | await _special_group_names()
        )
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

    async def test_user_slurm_accounts_from_slurmer_and_sticky_sources(
        self, beanie_client,
    ):
        """A user's slurm.account list = slurmer-role group accounts UNION
        sticky-group accounts (v1's sticky trigger adds every site user to
        global groups as member+slurmer) UNION the site's sticky accounts
        (site.slurm.sticky). A plain `member` and a `sponsor`-only edge do NOT
        contribute a slurm account."""
        from cheeto.queries import site_to_puppet_legacy

        site = Site(name='puppet_slurmedge', fqdn='slurmedge.test')
        await site.insert()

        user = User(
            name='pl_eve', email='e@x.test',
            uid=60007, gid=60007, fullname='Eve',
            home_directory='/home/e',
            status=await status_link('active'),
            access=await access_links(['login-ssh', 'slurm']),
        )
        await user.insert()
        await UserSiteInfo(
            user=user, site=site, status=await status_link('active'),
        ).insert()

        groups = {}
        for name, gid, roles in (
            ('pl_acct_member', 61010, ['member']),    # plain member -> excluded
            ('pl_acct_slurmer', 61011, ['slurmer']),  # slurmer -> included
            ('pl_acct_sponsor', 61012, ['sponsor']),  # sponsor-only -> excluded
            ('pl_acct_stickygrp', 61013, []),         # sticky group -> included
            ('pl_acct_sticky', 61014, []),            # sticky account -> included
        ):
            g = Group(name=name, gid=gid)
            await g.insert()
            account = SlurmAccount(group=g, site=site)
            await account.insert()
            if roles:
                await GroupMembership(
                    user=user, group=g, site=site, roles=roles,
                ).insert()
            groups[name] = (g, account)

        site.group.sticky = [groups['pl_acct_stickygrp'][0]]
        site.slurm.sticky = [groups['pl_acct_sticky'][1]]
        await site.save()
        site = await Site.find_one(Site.name == 'puppet_slurmedge')

        result = await site_to_puppet_legacy(site)
        # slurmer + sticky-group + sticky-account surface; the plain-member and
        # sponsor-only accounts do not.
        assert result.user['pl_eve'].slurm is not None
        assert result.user['pl_eve'].slurm.account == [
            'pl_acct_slurmer', 'pl_acct_sticky', 'pl_acct_stickygrp',
        ]

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

    async def test_slurm_account_group_appears_without_members(
        self, beanie_client,
    ):
        """A group whose only site presence is its slurm account must
        appear in the group map (v1 had a SiteGroup for every sponsor
        group with an account)."""
        from cheeto.queries import site_to_puppet_legacy

        site = Site(name='puppet_slacct', fqdn='slacct.test')
        await site.insert()
        other_site = Site(name='puppet_slacct_b', fqdn='slacctb.test')
        await other_site.insert()

        acct_grp = Group(name='pl_acctgrp', gid=61010)
        await acct_grp.insert()  # no membership edges anywhere
        await SlurmAccount(group=acct_grp, site=site).insert()

        elsewhere_grp = Group(name='pl_elsewhere', gid=61011)
        await elsewhere_grp.insert()
        await SlurmAccount(group=elsewhere_grp, site=other_site).insert()

        result = await site_to_puppet_legacy(site)
        assert 'pl_acctgrp' in result.group
        assert result.group['pl_acctgrp'].gid == 61010
        # Accounts at other sites don't leak in.
        assert 'pl_elsewhere' not in result.group

    async def test_special_groups_exported_with_gids(self, beanie_client):
        from cheeto.models.group import AccessGroup, StatusGroup
        from cheeto.queries import site_to_puppet_legacy

        site = Site(name='puppet_special', fqdn='special.test')
        await site.insert()

        result = await site_to_puppet_legacy(site)
        for g in (
            await AccessGroup.find_all().to_list()
            + await StatusGroup.find_all().to_list()
        ):
            assert g.name in result.group
            assert result.group[g.name].gid == g.gid
            assert result.group[g.name].sponsors is None


class TestSiteToPuppetLegacyStorage:
    """Storage blocks in the legacy export: user home autofs/zfs, group
    storage lists, and the share map (v1 user_to_puppet / group_to_puppet /
    share_to_puppet parity)."""

    async def _seed_base(self):
        site = Site(name='plstor', fqdn='plstor.test')
        await site.insert()
        user = User(
            name='pls_user', email='pls@x.test',
            uid=62001, gid=62001, fullname='Storage User',
            home_directory='/home/pls_user',
            status=await status_link('active'),
            access=await access_links(['login-ssh']),
        )
        await user.insert()
        await UserSiteInfo(
            user=user, site=site, status=await status_link('active'),
        ).insert()
        primary = Group(name='pls_user', gid=62001)
        await primary.insert()
        lab = Group(name='pls_lab', gid=62100)
        await lab.insert()
        await GroupMembership(
            user=user, group=lab, site=site, roles=['member'],
        ).insert()
        return site, user, primary, lab

    @staticmethod
    async def _volume(site, name, host_path, quota=None, managed=True):
        vol = StorageVolume(
            name=name, site=site, backend='zfs',
            zfs=ZFSConfig() if managed else None,
            host='nas-1', host_path=host_path,
            allocations=(
                [StorageAllocation(quota=quota)] if quota else []
            ),
        )
        await vol.insert()
        return vol

    async def test_user_home_storage(self, beanie_client):
        from cheeto.queries import site_to_puppet_legacy

        site, user, primary, lab = await self._seed_base()
        amap = AutomountMap(
            name='home', site=site, prefix='/home',
            options=['fstype=nfs', 'nosuid'],
        )
        await amap.insert()
        vol = await self._volume(
            site, 'home-pls_user', '/export/home/pls_user', quota='1T',
        )
        await Storage(
            name='pls_user', site=site, category='home',
            owner=user, group=primary, volume=vol, automount_map=amap,
        ).insert()

        result = await site_to_puppet_legacy(site)
        storage = result.user['pls_user'].storage
        assert storage is not None
        assert storage.autofs.nas == 'nas-1'
        # v1 emits the PARENT of the storage path for user homes.
        assert storage.autofs.path == '/export/home'
        assert storage.zfs.quota == '1T'

    async def test_user_without_home_storage_omits_block(self, beanie_client):
        from cheeto.queries import site_to_puppet_legacy

        site, *_ = await self._seed_base()
        result = await site_to_puppet_legacy(site)
        assert result.user['pls_user'].storage is None

    async def test_subpath_home_renders_zfs_false(self, beanie_client):
        from cheeto.queries import site_to_puppet_legacy

        site, user, primary, lab = await self._seed_base()
        amap = AutomountMap(name='home', site=site, prefix='/home')
        await amap.insert()
        # Farm legacy shape: home carved out of a collection volume.
        vol = await self._volume(
            site, 'homes', '/export/home', quota='100T',
        )
        await Storage(
            name='pls_user', site=site, category='home',
            owner=user, group=primary, volume=vol, subpath='pls_user',
            automount_map=amap,
        ).insert()

        result = await site_to_puppet_legacy(site)
        storage = result.user['pls_user'].storage
        assert storage.autofs.path == '/export/home'
        assert storage.zfs is False  # subpath storages have no quota

    async def test_group_storage_list(self, beanie_client):
        from cheeto.queries import site_to_puppet_legacy

        site, user, primary, lab = await self._seed_base()
        amap = AutomountMap(
            name='group', site=site, prefix='/group',
            options=['fstype=nfs', 'noatime', 'nosuid'],
        )
        await amap.insert()
        vol = await self._volume(
            site, 'pls_lab', '/export/group/pls_lab', quota='10T',
        )
        await Storage(
            name='pls_lab', site=site, category='group',
            owner=user, group=lab, volume=vol, automount_map=amap,
            globus=True,
        ).insert()

        result = await site_to_puppet_legacy(site)
        rows = result.group['pls_lab'].storage
        assert len(rows) == 1
        row = rows[0]
        assert row.name == 'pls_lab'
        assert row.owner == 'pls_user'
        assert row.group == 'pls_lab'
        assert row.autofs.nas == 'nas-1'
        assert row.autofs.path == '/export/group/pls_lab'  # full path
        # fstype=nfs stripped; remaining options reverse-sorted (v1 parity).
        assert row.autofs.options == 'nosuid,noatime'
        assert row.zfs.quota == '10T'
        assert row.globus is True

    async def test_unmanaged_volume_renders_zfs_false(self, beanie_client):
        from cheeto.queries import site_to_puppet_legacy

        site, user, primary, lab = await self._seed_base()
        amap = AutomountMap(name='group', site=site, prefix='/group')
        await amap.insert()
        vol = await self._volume(
            site, 'pls_lab', '/export/group/pls_lab',
            quota='10T', managed=False,
        )
        await Storage(
            name='pls_lab', site=site, category='group',
            owner=user, group=lab, volume=vol, automount_map=amap,
        ).insert()

        result = await site_to_puppet_legacy(site)
        assert result.group['pls_lab'].storage[0].zfs is False

    async def test_storage_only_group_appears(self, beanie_client):
        """v1 exported every SiteGroup; a group with no membership edges
        but with storage at the site must still appear."""
        from cheeto.queries import site_to_puppet_legacy

        site, user, primary, lab = await self._seed_base()
        loner = Group(name='pls_loner', gid=62200)
        await loner.insert()  # no GroupMembership edges
        amap = AutomountMap(name='group', site=site, prefix='/group')
        await amap.insert()
        vol = await self._volume(
            site, 'pls_loner', '/export/group/pls_loner', quota='5T',
        )
        await Storage(
            name='pls_loner', site=site, category='group',
            owner=user, group=loner, volume=vol, automount_map=amap,
        ).insert()

        result = await site_to_puppet_legacy(site)
        assert 'pls_loner' in result.group
        assert result.group['pls_loner'].gid == 62200
        assert result.group['pls_loner'].storage[0].zfs.quota == '5T'
        # Groups without storage emit an empty list (v1 parity).
        assert result.group['pls_lab'].storage == []

    async def test_share_map(self, beanie_client):
        from cheeto.queries import site_to_puppet_legacy

        site, user, primary, lab = await self._seed_base()
        amap = AutomountMap(name='share', site=site, prefix='/share')
        await amap.insert()
        vol = await self._volume(
            site, 'datashare', '/export/share/datashare', quota='20T',
        )
        await Storage(
            name='datashare', site=site, category='share',
            owner=user, group=lab, volume=vol, automount_map=amap,
        ).insert()

        result = await site_to_puppet_legacy(site)
        assert set(result.share.keys()) == {'datashare'}
        rec = result.share['datashare'].storage
        assert rec.owner == 'pls_user'
        assert rec.group == 'pls_lab'
        assert rec.autofs.nas == 'nas-1'
        assert rec.autofs.path == '/export/share/datashare'
        assert rec.zfs.quota == '20T'


class _FakeLDAPManager:
    """Minimal stand-in for AsyncLDAPManager covering what the per-record
    sync ops touch. Records every write call and serves a seeded
    current-membership set so we can assert reconcile and gate decisions
    without a live directory."""

    def __init__(self, current_memberships: set[str] = frozenset()):
        self._current = set(current_memberships)
        self.added: list[str] = []
        self.removed: list[str] = []
        self.user_writes: list[str] = []
        self.deleted_users: list[str] = []
        self.group_writes: list[str] = []
        self.deleted_groups: list[str] = []
        self.automount_writes: list[str] = []

    def user_dn(self, username: str) -> str:
        return f'uid={username},ou=people'

    async def delete_user(self, username: str) -> None:
        self.deleted_users.append(username)

    async def update_user(self, record) -> None:
        # Pretend the user already exists → 'updated' path.
        self.user_writes.append(record.username)

    async def add_user(self, record) -> None:
        self.user_writes.append(record.username)

    async def list_user_memberships(self, username: str) -> set[str]:
        return set(self._current)

    async def add_users_to_group(self, group, usernames, verify_users=True):
        self.added.append(group)

    async def remove_users_from_group(self, group, usernames):
        self.removed.append(group)

    async def delete_group(self, groupname: str) -> None:
        self.deleted_groups.append(groupname)

    async def set_group_members(self, groupname, members) -> None:
        # Pretend the group already exists → 'membership_diffed' path.
        self.group_writes.append(groupname)

    async def add_group(self, record) -> None:
        self.group_writes.append(record.groupname)

    async def upsert_automount(self, *, mountname, mapname, host, path,
                               options) -> None:
        self.automount_writes.append(f'{mapname}:{mountname}')

    @property
    def write_count(self) -> int:
        return (
            len(self.user_writes) + len(self.deleted_users)
            + len(self.group_writes) + len(self.deleted_groups)
            + len(self.automount_writes)
            + len(self.added) + len(self.removed)
        )


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


# ---------------------------------------------------------------------------
# LDAPSyncable dirty tracking
# ---------------------------------------------------------------------------


class TestLDAPInfo:
    """Unit semantics of the embedded LDAPInfo model."""

    def test_needs_sync_per_site(self):
        info = LDAPInfo()
        watermark = info.modified_at
        assert info.needs_sync('a') and info.needs_sync('b')
        info.synced['a'] = watermark
        assert not info.needs_sync('a')
        assert info.needs_sync('b')

    def test_stale_watermark_stays_dirty(self):
        # A record mutated mid-sync: the op wrote the OLD modified_at as
        # the watermark, so the record must still read dirty.
        info = LDAPInfo()
        info.synced['a'] = info.modified_at
        info.modified_at = info.modified_at + datetime.timedelta(seconds=1)
        assert info.needs_sync('a')

    def test_datetimes_coerced_naive_utc(self):
        # The mongo client is tz-naive; aware inputs must coerce or the
        # needs_sync comparison raises TypeError.
        aware = datetime.datetime(2026, 6, 1, 12, tzinfo=datetime.timezone.utc)
        info = LDAPInfo(modified_at=aware, synced={'a': aware})
        assert info.modified_at.tzinfo is None
        assert info.synced['a'].tzinfo is None
        info.needs_sync('a')  # must not raise


class TestLDAPSyncableFingerprint:

    async def _user(self, name='fpuser', uid=73001):
        u = User(
            name=name, email=f'{name}@x.test',
            uid=uid, gid=uid, fullname='FP User',
            home_directory=f'/home/{name}',
            status=await status_link('active'),
            access=await access_links(['login-ssh']),
        )
        await u.insert()
        return u

    async def _modified_at(self, model, doc_id):
        return (await model.get(doc_id, with_children=True)).ldap.modified_at

    async def test_insert_sets_fingerprint_and_dirty(self, beanie_client):
        u = await self._user()
        fetched = await User.get(u.id)
        assert fetched.ldap.fingerprint is not None
        assert fetched.ldap.synced == {}
        assert fetched.ldap.needs_sync('anysite')

    async def test_bookkeeping_save_does_not_redirty(self, beanie_client):
        # The nightly IAM sync saves every user just to advance
        # iam.iam_synced_at; that must NOT re-dirty LDAP state — this is
        # the regression the fingerprint exists for.
        u = await self._user()
        before = await self._modified_at(User, u.id)
        u.iam = UCDIAMInfo(
            iam_status='present',
            iam_synced_at=datetime.datetime(2026, 6, 1, 2, 0, 0),
        )
        await u.save()
        assert await self._modified_at(User, u.id) == before

    async def test_projected_field_change_redirties(self, beanie_client):
        u = await self._user()
        before = await self._modified_at(User, u.id)
        u.shell = '/usr/bin/zsh'
        await u.save()
        assert await self._modified_at(User, u.id) > before

    async def test_access_change_redirties(self, beanie_client):
        u = await self._user()
        before = await self._modified_at(User, u.id)
        u.access = await access_links(['login-ssh', 'sudo'])
        await u.save()
        assert await self._modified_at(User, u.id) > before

    async def test_fingerprint_link_fetch_invariant(self, beanie_client):
        u = await self._user()
        unfetched = await User.get(u.id)
        fetched = await User.get(u.id, fetch_links=True)
        assert unfetched.ldap_fingerprint() == fetched.ldap_fingerprint()

    async def test_group_gid_change_redirties(self, beanie_client):
        g = Group(name='fpgrp', gid=73100)
        await g.insert()
        before = await self._modified_at(Group, g.id)
        g.gid = 73101
        await g.save()
        assert await self._modified_at(Group, g.id) > before

    async def test_storage_own_field_change_redirties(self, beanie_client):
        owner, group, site = await _seed_storage_actors('fpstor', 73200)
        volume = await _seed_volume(site)
        storage = Storage(
            name='fpstor', site=site, category='group',
            owner=owner, group=group, volume=volume,
        )
        await storage.insert()
        before = await self._modified_at(Storage, storage.id)
        storage.mount_name = 'renamed'
        await storage.save()
        assert await self._modified_at(Storage, storage.id) > before


class TestLDAPSyncablePropagation:
    """Changes on relational documents must mark the documents whose LDAP
    projection they feed."""

    async def _modified_at(self, model, doc_id):
        return (await model.get(doc_id, with_children=True)).ldap.modified_at

    async def _seed(self):
        user = User(
            name='propu', email='p@x.test', uid=73301, gid=73301,
            fullname='Prop User', home_directory='/home/propu',
            status=await status_link('active'),
        )
        await user.insert()
        site = Site(name='propsite', fqdn='prop.test')
        await site.insert()
        return user, site

    async def test_ssh_key_insert_and_delete(self, beanie_client):
        user, _ = await self._seed()
        before = await self._modified_at(User, user.id)
        key = SshKey(key='ssh-ed25519 AAA', user=user)
        await key.insert()
        after_insert = await self._modified_at(User, user.id)
        assert after_insert > before
        await key.delete()
        assert await self._modified_at(User, user.id) > after_insert

    async def test_user_site_info_save(self, beanie_client):
        user, site = await self._seed()
        usi = UserSiteInfo(user=user, site=site)
        await usi.insert()
        before = await self._modified_at(User, user.id)
        usi.status = await status_link('inactive')
        await usi.save()
        assert await self._modified_at(User, user.id) > before

    async def test_group_membership_marks_both_edges(self, beanie_client):
        user, site = await self._seed()
        group = Group(name='propgrp', gid=73400)
        await group.insert()
        u_before = await self._modified_at(User, user.id)
        g_before = await self._modified_at(Group, group.id)
        edge = GroupMembership(
            user=user, group=group, site=site, roles=['member'],
        )
        await edge.insert()
        u_after = await self._modified_at(User, user.id)
        g_after = await self._modified_at(Group, group.id)
        assert u_after > u_before and g_after > g_before
        await edge.delete()
        assert await self._modified_at(User, user.id) > u_after
        assert await self._modified_at(Group, group.id) > g_after

    async def test_volume_marks_only_its_storages(self, beanie_client):
        owner, group, site = await _seed_storage_actors('propstor', 73500)
        vol_a = await _seed_volume(site, name='vola', host_path='/nas/a')
        vol_b = await _seed_volume(site, name='volb', host_path='/nas/b')
        stor_a = Storage(
            name='stora', site=site, category='group',
            owner=owner, group=group, volume=vol_a,
        )
        stor_b = Storage(
            name='storb', site=site, category='group',
            owner=owner, group=group, volume=vol_b,
        )
        await stor_a.insert()
        await stor_b.insert()
        a_before = await self._modified_at(Storage, stor_a.id)
        b_before = await self._modified_at(Storage, stor_b.id)
        vol_a.allocations = [StorageAllocation(quota='10T')]
        await vol_a.save()
        assert await self._modified_at(Storage, stor_a.id) > a_before
        assert await self._modified_at(Storage, stor_b.id) == b_before

    async def test_automount_map_marks_its_storages(self, beanie_client):
        owner, group, site = await _seed_storage_actors('propmnt', 73600)
        volume = await _seed_volume(site)
        amap = AutomountMap(name='group', site=site, prefix='/group')
        await amap.insert()
        storage = Storage(
            name='mntstor', site=site, category='group',
            owner=owner, group=group, volume=volume, automount_map=amap,
        )
        await storage.insert()
        before = await self._modified_at(Storage, storage.id)
        amap.options = ['nosuid']
        await amap.save()
        assert await self._modified_at(Storage, storage.id) > before

    async def test_propagation_fires_once_per_save(self, beanie_client, monkeypatch):
        # Guards the Update-covers-Save assumption: save() routes through
        # self.update(), so subscribing Save too would double-fire the
        # cross-collection write. Count ldap_touch calls during one save.
        from ..models import user as user_module
        user, _ = await self._seed()
        key = SshKey(key='ssh-ed25519 AAA', user=user)
        await key.insert()

        calls = []
        real_touch = user_module.ldap_touch

        def counting_touch():
            calls.append(1)
            return real_touch()

        monkeypatch.setattr(user_module, 'ldap_touch', counting_touch)
        key.key = 'ssh-ed25519 BBB'
        await key.save()
        assert len(calls) == 1

    async def test_op_writing_parent_and_edge_defers_touch(
        self, beanie_client,
    ):
        # SetUserStatus(site=...) saves the USI (whose hook touches the
        # user) AND the user doc in one transaction. The touch must defer
        # past the commit instead of deadlocking against the transaction's
        # own user write (which previously stalled until mongod aborted
        # the txn at transactionLifetimeLimit).
        user, site = await self._seed()
        await UserSiteInfo(
            user=user, site=site, status=await status_link('active'),
        ).insert()
        before = await self._modified_at(User, user.id)

        await SetUserStatus.run(
            beanie_client, None,
            name='propu', status='inactive', reason='testing',
            site='propsite',
        )

        assert await self._modified_at(User, user.id) > before


async def _seed_gate_site(sitename='gatesite', username='gateu', uid=74001):
    user = User(
        name=username, email=f'{username}@x.test',
        uid=uid, gid=uid, fullname='Gate User',
        home_directory=f'/home/{username}',
        status=await status_link('active'),
        access=await access_links(['login-ssh']),
    )
    await user.insert()
    site = Site(name=sitename, fqdn=f'{sitename}.test')
    await site.insert()
    await UserSiteInfo(
        user=user, site=site, status=await status_link('active'),
    ).insert()
    return user, site


class TestSyncUserToLDAPGate:

    async def _sync(self, client, ldap, **kwargs):
        return await SyncUserToLDAP.run(
            client, None,
            username='gateu', sitename='gatesite', ldap=ldap, **kwargs,
        )

    async def test_sync_writes_watermark_then_skips_clean(self, beanie_client):
        user, _ = await _seed_gate_site()
        ldap = _FakeLDAPManager()
        expected_watermark = (await User.get(user.id)).ldap.modified_at

        r1 = await self._sync(beanie_client, ldap)
        assert r1.outcome in ('updated', 'no_op')
        raw = await User.get(user.id)
        assert raw.ldap.synced['gatesite'] == expected_watermark
        # Watermark write must not re-dirty (query update bypasses hooks).
        assert raw.ldap.modified_at == expected_watermark
        assert not raw.ldap.needs_sync('gatesite')

        writes_after_first = ldap.write_count
        r2 = await self._sync(beanie_client, ldap)
        assert r2.outcome == 'skipped_clean'
        assert ldap.write_count == writes_after_first

    async def test_change_redirties_then_syncs(self, beanie_client):
        user, _ = await _seed_gate_site()
        ldap = _FakeLDAPManager()
        await self._sync(beanie_client, ldap)

        user = await User.get(user.id)
        user.shell = '/usr/bin/zsh'
        await user.save()
        r = await self._sync(beanie_client, ldap)
        assert r.outcome in ('updated', 'no_op')
        assert not (await User.get(user.id)).ldap.needs_sync('gatesite')

    async def test_force_bypasses_gate_and_recreates(self, beanie_client):
        await _seed_gate_site()
        ldap = _FakeLDAPManager()
        await self._sync(beanie_client, ldap)
        r = await self._sync(beanie_client, ldap, force=True)
        assert r.outcome == 'recreated'
        assert ldap.deleted_users == ['gateu']

    async def test_full_bypasses_gate_without_recreate(self, beanie_client):
        await _seed_gate_site()
        ldap = _FakeLDAPManager()
        await self._sync(beanie_client, ldap)
        r = await self._sync(beanie_client, ldap, full=True)
        assert r.outcome in ('updated', 'no_op')
        assert ldap.deleted_users == []

    async def test_ignore_beats_force(self, beanie_client):
        user, _ = await _seed_gate_site()
        user.ldap.ignore = True
        await user.save()
        ldap = _FakeLDAPManager()
        r = await self._sync(beanie_client, ldap, force=True)
        assert r.outcome == 'skipped_ignored'
        assert ldap.write_count == 0

    async def test_two_site_watermarks_independent(self, beanie_client):
        user, _ = await _seed_gate_site()
        site_b = Site(name='gatesiteb', fqdn='b.test')
        await site_b.insert()
        await UserSiteInfo(
            user=user, site=site_b, status=await status_link('active'),
        ).insert()

        await self._sync(beanie_client, _FakeLDAPManager())
        raw = await User.get(user.id)
        assert not raw.ldap.needs_sync('gatesite')
        assert raw.ldap.needs_sync('gatesiteb')


class TestSyncGroupToLDAPGate:

    async def _sync(self, client, ldap, groupname='gategrp', **kwargs):
        return await SyncGroupToLDAP.run(
            client, None,
            groupname=groupname, sitename='gatesite', ldap=ldap, **kwargs,
        )

    async def _seed_group_with_member(self):
        user, site = await _seed_gate_site()
        group = Group(name='gategrp', gid=74100)
        await group.insert()
        await GroupMembership(
            user=user, group=group, site=site, roles=['member'],
        ).insert()
        return group

    async def test_sync_writes_watermark_then_skips_clean(self, beanie_client):
        group = await self._seed_group_with_member()
        ldap = _FakeLDAPManager()

        r1 = await self._sync(beanie_client, ldap)
        assert r1.outcome == 'membership_diffed'
        assert not (await Group.get(group.id)).ldap.needs_sync('gatesite')

        r2 = await self._sync(beanie_client, ldap)
        assert r2.outcome == 'skipped_clean'
        assert ldap.group_writes == ['gategrp']

    async def test_membership_change_redirties(self, beanie_client):
        group = await self._seed_group_with_member()
        ldap = _FakeLDAPManager()
        await self._sync(beanie_client, ldap)

        edge = await GroupMembership.find_one(
            GroupMembership.group.id == group.id,
        )
        await edge.delete()
        assert (await Group.get(group.id)).ldap.needs_sync('gatesite')

    async def test_no_members_writes_watermark(self, beanie_client):
        _, site = await _seed_gate_site()
        group = Group(name='gategrp', gid=74100)
        await group.insert()
        ldap = _FakeLDAPManager()

        r1 = await self._sync(beanie_client, ldap)
        assert r1.outcome == 'skipped_no_members_on_site'
        r2 = await self._sync(beanie_client, ldap)
        assert r2.outcome == 'skipped_clean'

    async def test_special_group_no_watermark(self, beanie_client):
        await _seed_gate_site()
        ldap = _FakeLDAPManager()
        r = await self._sync(
            beanie_client, ldap, groupname='login-ssh-users',
        )
        assert r.outcome == 'skipped_special'
        special = await Group.find_one(
            Group.name == 'login-ssh-users', with_children=True,
        )
        assert special.ldap.synced == {}


class TestSyncSiteAutomountsGate:

    async def _seed(self):
        owner, group, site = await _seed_storage_actors('amgate', 74200)
        volume = await _seed_volume(site)
        amap = AutomountMap(name='group', site=site, prefix='/group')
        await amap.insert()
        storage = Storage(
            name='amgstor', site=site, category='group',
            owner=owner, group=group, volume=volume, automount_map=amap,
        )
        await storage.insert()
        return storage, site

    async def test_upsert_then_skip_clean(self, beanie_client):
        storage, site = await self._seed()
        ldap = _FakeLDAPManager()

        r1 = await SyncSiteAutomounts.run(
            beanie_client, None, sitename=site.name, ldap=ldap,
        )
        assert r1['group_automounts'] == 1
        assert len(ldap.automount_writes) == 1

        r2 = await SyncSiteAutomounts.run(
            beanie_client, None, sitename=site.name, ldap=ldap,
        )
        assert r2['group_automounts'] == 0
        assert r2['skipped_clean'] == 1
        assert len(ldap.automount_writes) == 1

        # A volume change re-dirties the row via propagation.
        volume = await StorageVolume.find_one(StorageVolume.name == 'nas01vol')
        volume.host = 'nas02'
        await volume.save()
        r3 = await SyncSiteAutomounts.run(
            beanie_client, None, sitename=site.name, ldap=ldap,
        )
        assert r3['group_automounts'] == 1


class TestSyncSiteLDAPIncremental:

    async def _seed(self):
        user, site = await _seed_gate_site()
        other = User(
            name='gateu2', email='g2@x.test', uid=74002, gid=74002,
            fullname='Gate User Two', home_directory='/home/gateu2',
            status=await status_link('active'),
            access=await access_links(['login-ssh']),
        )
        await other.insert()
        await UserSiteInfo(
            user=other, site=site, status=await status_link('active'),
        ).insert()
        group = Group(name='gategrp', gid=74100)
        await group.insert()
        await GroupMembership(
            user=user, group=group, site=site, roles=['member'],
        ).insert()
        return user, other, group, site

    async def _run(self, client, ldap, **kwargs):
        return await SyncSiteLDAP.run(
            client, None,
            sitename='gatesite', ldap=ldap,
            scope=['users', 'groups'], **kwargs,
        )

    async def test_second_run_skips_everything(self, beanie_client):
        await self._seed()
        ldap = _FakeLDAPManager()

        r1 = await self._run(beanie_client, ldap)
        assert r1['users']['skipped_clean'] == 0
        assert (
            r1['users']['updated'] + r1['users']['no_op']
            + r1['users']['created'] == 2
        )
        writes_after_first = ldap.write_count

        r2 = await self._run(beanie_client, ldap)
        assert r2['users']['skipped_clean'] == 2
        assert r2['groups']['skipped_clean'] == 1
        # No per-record ops generated at all → no LDAP writes.
        assert ldap.write_count == writes_after_first

    async def test_full_resyncs_clean_records(self, beanie_client):
        await self._seed()
        ldap = _FakeLDAPManager()
        await self._run(beanie_client, ldap)

        r = await self._run(beanie_client, ldap, full=True)
        assert r['users']['skipped_clean'] == 0
        assert r['groups']['skipped_clean'] == 0
        assert ldap.deleted_users == []

    async def test_ignored_user_never_synced(self, beanie_client):
        user, *_ = await self._seed()
        user.ldap.ignore = True
        await user.save()
        ldap = _FakeLDAPManager()

        r = await self._run(beanie_client, ldap, force=True)
        assert r['users']['skipped_ignored'] == 1
        assert 'gateu' not in ldap.user_writes

    async def test_only_dirty_user_synced(self, beanie_client):
        user, other, group, site = await self._seed()
        ldap = _FakeLDAPManager()
        await self._run(beanie_client, ldap)

        other = await User.get(other.id)
        other.shell = '/usr/bin/zsh'
        await other.save()

        r = await self._run(beanie_client, ldap)
        assert r['users']['skipped_clean'] == 1
        assert ldap.user_writes.count('gateu2') == 2
        assert ldap.user_writes.count('gateu') == 1


class TestBackfillLDAPInfo:

    async def _raw_insert_legacy_user(self):
        """Insert a user document the way it existed before LDAPSyncable:
        no `ldap` field stored."""
        coll = User.get_pymongo_collection()
        res = await coll.insert_one({
            'name': 'legacyu', 'email': 'legacy@x.test',
            'uid': 74900, 'gid': 74900, 'fullname': 'Legacy User',
            'home_directory': '/home/legacyu',
        })
        return res.inserted_id

    async def test_legacy_doc_mints_fresh_dirty_state_per_read(
        self, beanie_client,
    ):
        # Without a stored ldap subdocument, default_factory mints a new
        # modified_at on every read — the record can never go clean.
        doc_id = await self._raw_insert_legacy_user()
        first = await User.get(doc_id)
        second = await User.get(doc_id)
        assert first.ldap.fingerprint is None
        assert first.ldap.modified_at != second.ldap.modified_at
        assert first.ldap.needs_sync('anysite')

    async def test_backfill_persists_state_and_is_idempotent(
        self, beanie_client,
    ):
        doc_id = await self._raw_insert_legacy_user()
        from ..operations import BackfillLDAPInfo

        result = await BackfillLDAPInfo.run(beanie_client, None)
        # Seeded access/status groups were inserted through beanie, so
        # their ldap state is already stored; only the raw doc is touched.
        assert result == {'users': 1, 'groups': 0, 'storages': 0}

        first = await User.get(doc_id)
        second = await User.get(doc_id)
        assert first.ldap.fingerprint is not None
        assert first.ldap.modified_at == second.ldap.modified_at

        again = await BackfillLDAPInfo.run(beanie_client, None)
        assert again == {'users': 0, 'groups': 0, 'storages': 0}

    async def test_backfilled_doc_goes_clean_after_sync(self, beanie_client):
        await self._raw_insert_legacy_user()
        from ..operations import BackfillLDAPInfo
        await BackfillLDAPInfo.run(beanie_client, None)

        site = Site(name='legacysite', fqdn='legacy.test')
        await site.insert()
        user = await User.find_one(User.name == 'legacyu')
        await UserSiteInfo(
            user=user, site=site, status=await status_link('active'),
        ).insert()

        ldap = _FakeLDAPManager()
        r1 = await SyncUserToLDAP.run(
            beanie_client, None,
            username='legacyu', sitename='legacysite', ldap=ldap,
        )
        assert r1.outcome in ('updated', 'no_op')
        r2 = await SyncUserToLDAP.run(
            beanie_client, None,
            username='legacyu', sitename='legacysite', ldap=ldap,
        )
        assert r2.outcome == 'skipped_clean'


async def _seed_slurm_site(with_default=False, partition_name='hi'):
    """Seed a site with one group, a slurm account, partition, QOS, and an
    association, plus a slurm-eligible member (alice) and an ineligible one
    (bob, no slurm access). When `with_default`, mark the account sticky and
    set it as the site's default account. `partition_name` lets the live
    integration test use a partition the controller actually has (e.g. 'cpu').
    Returns the Site."""
    site = Site(name='slsite', fqdn='sl.test')
    await site.insert()

    group = Group(name='sllab', gid=72000)
    await group.insert()

    alice = User(
        name='sl_alice', email='a@sl.test', uid=72001, gid=72001,
        fullname='Alice', home_directory='/home/a',
        status=await status_link('active'),
        access=await access_links(['login-ssh', 'slurm']),
    )
    bob = User(
        name='sl_bob', email='b@sl.test', uid=72002, gid=72002,
        fullname='Bob', home_directory='/home/b',
        status=await status_link('active'),
        access=await access_links(['login-ssh']),  # no slurm
    )
    await alice.insert()
    await bob.insert()
    for u in (alice, bob):
        await UserSiteInfo(user=u, site=site, status=await status_link('active')).insert()
        await GroupMembership(user=u, group=group, site=site, roles=['member']).insert()

    account = SlurmAccount(group=group, site=site)
    await account.insert()
    partition = SlurmPartition(name=partition_name, site=site)
    await partition.insert()
    alloc = SlurmAllocation(tres=SlurmTRES(cpus=8, mem='16G'))
    await alloc.insert()
    qos = SlurmQOS(
        name='sllab-q', site=site, group_limits=[alloc],
        priority=10, flags=['DenyOnLimit'],
    )
    await qos.insert()
    await SlurmAssociation(
        site=site, account=account, partition=partition, qos=qos,
    ).insert()

    if with_default:
        site.slurm.sticky = [account]
        site.slurm.default_account = account
        await site.save()
        site = await Site.find_one(Site.name == 'slsite')
    return site


class TestBuildDesiredSlurmState:

    async def test_desired_state_excludes_ineligible_users(self, beanie_client):
        from cheeto.queries import build_desired_slurm_state
        from cheeto.slurm_sync import AccountState, TRESLimit

        site = await _seed_slurm_site()
        state = await build_desired_slurm_state(site)

        assert set(state.qos) == {'sllab-q'}
        q = state.qos['sllab-q']
        assert q.group == TRESLimit(cpus=8, mem_megs=16384)
        assert q.priority == 10
        assert q.flags == frozenset({'DenyOnLimit'})

        # only the group with an eligible member's association is kept
        assert state.accounts == {'sllab': AccountState()}
        # alice is slurm-eligible; bob is not
        assert state.associations == {('sl_alice', 'sllab', 'hi'): 'sllab-q'}
        # no site default configured → no default accounts
        assert state.default_accounts == {}

    async def test_default_accounts_for_at_site_users(self, beanie_client):
        from cheeto.queries import build_desired_slurm_state

        site = await _seed_slurm_site(with_default=True)
        state = await build_desired_slurm_state(site)
        # the default account resolves to its owning group name, set for
        # every at-site eligible user (alice; bob has no association)
        assert state.default_accounts == {'sl_alice': 'sllab'}


class _FakeSAcctMgr:
    """Stand-in for AsyncSAcctMgr: serves a fixed current state and records
    dispatched command strings (optionally failing chosen ones)."""

    def __init__(self, current, fail_on=()):
        self._current = current
        self.fail_on = set(fail_on)
        self.dispatched: list[str] = []

    async def read_current_state(self):
        return self._current

    async def dispatch(self, spec):
        import sh
        self.dispatched.append(str(spec))
        if str(spec) in self.fail_on:
            raise sh.ErrorReturnCode_1('sacctmgr', b'', b'boom')
        return None


class TestSyncSlurmOperation:

    async def test_dry_run_plans_without_dispatching(self, beanie_client):
        from cheeto.slurm_sync import SlurmSyncState
        site = await _seed_slurm_site()
        fake = _FakeSAcctMgr(SlurmSyncState())  # empty current → all adds

        result = await SyncSlurm.run(
            beanie_client, None, sitename=site.name, sacctmgr=fake, apply=False,
        )
        assert result['apply'] is False
        assert fake.dispatched == []  # dry-run never dispatches
        assert result['plan']['Add QOS']
        assert result['plan']['Add accounts']
        assert result['plan']['Add user associations']

    async def test_apply_dispatches_in_batch_order(self, beanie_client):
        from cheeto.slurm_sync import SlurmSyncState
        site = await _seed_slurm_site()
        fake = _FakeSAcctMgr(SlurmSyncState())

        result = await SyncSlurm.run(
            beanie_client, None, sitename=site.name, sacctmgr=fake, apply=True,
        )
        # QOS add dispatched before the user-association add (account before user).
        joined = '\n'.join(fake.dispatched)
        assert 'add qos sllab-q' in joined
        assert 'add account sllab' in joined
        assert 'add user user=sl_alice' in joined
        qos_idx = next(i for i, c in enumerate(fake.dispatched) if 'add qos' in c)
        user_idx = next(i for i, c in enumerate(fake.dispatched) if 'add user' in c)
        assert qos_idx < user_idx
        assert result['tally']['Add QOS']['ok'] == 1

    async def test_failed_command_tallied_without_aborting(self, beanie_client):
        from cheeto.slurm_sync import SlurmSyncState
        site = await _seed_slurm_site()
        # Fail the QOS add; the rest should still run.
        fake = _FakeSAcctMgr(
            SlurmSyncState(),
            fail_on={'sacctmgr -iQ add qos sllab-q '
                     'GrpTRES=cpu=8,mem=16384,gres/gpu=-1 '
                     'MaxTRESPerUser=cpu=-1,mem=-1,gres/gpu=-1 '
                     'MaxTRESPerJob=cpu=-1,mem=-1,gres/gpu=-1 '
                     'Flags=DenyOnLimit Priority=10'},
        )
        result = await SyncSlurm.run(
            beanie_client, None, sitename=site.name, sacctmgr=fake, apply=True,
        )
        assert result['tally']['Add QOS'] == {'ok': 0, 'failed': 1, 'total': 1}
        # the account add still happened despite the QOS failure
        assert result['tally']['Add accounts']['ok'] == 1

    async def test_max_deletions_cap_aborts(self, beanie_client):
        from cheeto.slurm_sync import AccountState, QOSState, SlurmSyncState, SlurmSyncAborted
        site = await _seed_slurm_site()
        # Current has extra entities absent from desired → deletions.
        current = SlurmSyncState(
            qos={'stale-q': QOSState()},
            accounts={'stalelab': AccountState()},
            associations={('ghost', 'stalelab', 'hi'): 'stale-q'},
        )
        fake = _FakeSAcctMgr(current)
        with pytest.raises(SlurmSyncAborted):
            await SyncSlurm.run(
                beanie_client, None, sitename=site.name, sacctmgr=fake,
                apply=True, max_deletions=1,
            )
        assert fake.dispatched == []  # aborted before any mutation

    async def test_dry_run_over_cap_does_not_abort(self, beanie_client):
        """The cap only gates apply; a dry-run preview shows the full plan
        including deletions even when it exceeds the cap."""
        from cheeto.slurm_sync import AccountState, QOSState, SlurmSyncState
        site = await _seed_slurm_site()
        current = SlurmSyncState(
            qos={'stale-q': QOSState()},
            accounts={'stalelab': AccountState()},
            associations={('ghost', 'stalelab', 'hi'): 'stale-q'},
        )
        fake = _FakeSAcctMgr(current)
        result = await SyncSlurm.run(
            beanie_client, None, sitename=site.name, sacctmgr=fake,
            apply=False, max_deletions=1,
        )
        assert fake.dispatched == []
        assert result['plan']['Delete QOS'] == ['sacctmgr -iQ remove qos stale-q']

    async def test_offline_dump_idempotent(self, beanie_client):
        """A DumpSAcctMgr whose captured state matches the site's desired
        state yields an empty plan — offline idempotency."""
        from cheeto.slurm_sync import DumpSAcctMgr
        site = await _seed_slurm_site()
        qos_dump = (
            'Name|Priority|GrpTRES|MaxTRES|MaxTRESPU|Flags\n'
            'sllab-q|10|cpu=8,mem=16384,gres/gpu=-1|||DenyOnLimit\n'
        )
        assoc_dump = (
            'Account|User|Partition|QOS|MaxJobs|GrpJobs|MaxSubmit|MaxWall\n'
            'sllab||||-1|-1|-1|-1\n'
            'sllab|sl_alice|hi|sllab-q||||\n'
        )
        mgr = DumpSAcctMgr(qos_text=qos_dump, associations_text=assoc_dump)
        result = await SyncSlurm.run(
            beanie_client, None, sitename=site.name, sacctmgr=mgr, apply=False,
        )
        assert result['plan'] == {}  # already in sync

    async def test_default_account_set_for_new_user_offline(self, beanie_client):
        """With a site default configured and an empty controller, the plan
        re-points the at-site user's default account."""
        from cheeto.slurm_sync import DumpSAcctMgr
        site = await _seed_slurm_site(with_default=True)
        mgr = DumpSAcctMgr()  # empty controller
        result = await SyncSlurm.run(
            beanie_client, None, sitename=site.name, sacctmgr=mgr, apply=False,
        )
        assert result['plan']['Set user default accounts'] == [
            'sacctmgr -iQ modify user set defaultaccount=sllab where user=sl_alice'
        ]

    async def test_default_account_idempotent_offline(self, beanie_client):
        """When the controller dump already has the user defaulting to the
        site default (and qos/assoc/accounts match), the plan is empty."""
        from cheeto.slurm_sync import DumpSAcctMgr
        site = await _seed_slurm_site(with_default=True)
        qos_dump = (
            'Name|Priority|GrpTRES|MaxTRES|MaxTRESPU|Flags\n'
            'sllab-q|10|cpu=8,mem=16384,gres/gpu=-1|||DenyOnLimit\n'
        )
        assoc_dump = (
            'Account|User|Partition|QOS|MaxJobs|GrpJobs|MaxSubmit|MaxWall\n'
            'sllab||||-1|-1|-1|-1\n'
            'sllab|sl_alice|hi|sllab-q||||\n'
        )
        user_dump = 'User|Def Acct\nsl_alice|sllab\n'
        mgr = DumpSAcctMgr(
            qos_text=qos_dump, associations_text=assoc_dump, user_text=user_dump,
        )
        result = await SyncSlurm.run(
            beanie_client, None, sitename=site.name, sacctmgr=mgr, apply=False,
        )
        assert result['plan'] == {}  # already in sync, defaults included




class TestSiteDefaultSlurmAccountOps:

    async def test_set_default_adds_sticky_and_sets_default(self, beanie_client):
        from cheeto.models.base import link_target_id
        # seed creates the SlurmAccount for group 'sllab' but no default/sticky
        site = await _seed_slurm_site()
        assert site.slurm.default_account is None
        assert site.slurm.sticky == []

        await SetSiteDefaultSlurmAccount.run(
            beanie_client, None, sitename='slsite', groupname='sllab',
        )
        site = await Site.find_one(Site.name == 'slsite')
        # default set, and the account auto-added to sticky to satisfy the
        # @before_event validator
        assert link_target_id(site.slurm.default_account) is not None
        assert len(site.slurm.sticky) == 1
        assert link_target_id(site.slurm.default_account) == \
            link_target_id(site.slurm.sticky[0])
        # resolves to the owning group name
        from cheeto.queries import resolve_slurm_account_label
        assert await resolve_slurm_account_label(site.slurm.default_account) == 'sllab'

    async def test_clear_default_leaves_sticky(self, beanie_client):
        from cheeto.models.base import link_target_id
        site = await _seed_slurm_site()
        await SetSiteDefaultSlurmAccount.run(
            beanie_client, None, sitename='slsite', groupname='sllab',
        )
        await ClearSiteDefaultSlurmAccount.run(
            beanie_client, None, sitename='slsite',
        )
        site = await Site.find_one(Site.name == 'slsite')
        assert site.slurm.default_account is None
        assert len(site.slurm.sticky) == 1  # still sticky

    async def test_clear_default_idempotent(self, beanie_client):
        await _seed_slurm_site()
        # no default set → clear is a no-op, no error
        await ClearSiteDefaultSlurmAccount.run(
            beanie_client, None, sitename='slsite',
        )
        site = await Site.find_one(Site.name == 'slsite')
        assert site.slurm.default_account is None

    async def test_set_default_unknown_group_errors(self, beanie_client):
        await _seed_slurm_site()
        with pytest.raises(ValueError):
            await SetSiteDefaultSlurmAccount.run(
                beanie_client, None, sitename='slsite', groupname='nope',
            )


@pytest.mark.skipif(
    os.environ.get('CHEETO_SLURM_LIVE') != '1',
    reason='requires a live slurm-docker-cluster controller; '
           'set CHEETO_SLURM_LIVE=1 (CI slurm-integration job)',
)
class TestSlurmLive:
    """End-to-end against a real slurmctld (giovtorres/slurm-docker-cluster)
    reached via `docker exec slurmctld sacctmgr`. Gated on CHEETO_SLURM_LIVE
    so it only runs in the CI job that brings the cluster up.

    Validates what offline tests can't: that the hand-written parsers accept
    real `sacctmgr show -P` output and that rendered commands are accepted by
    a real controller, end to end through SyncSlurm."""

    async def test_apply_then_converge(self, beanie_client):
        from cheeto.slurm_sync import AsyncSAcctMgr
        # partition 'cpu' is the default partition in slurm-docker-cluster.
        site = await _seed_slurm_site(with_default=True, partition_name='cpu')
        mgr = AsyncSAcctMgr(exec_prefix=['docker', 'exec', 'slurmctld'])

        # A freshly-registered cluster has only root/normal (both protected),
        # so a full apply is adds-only. max_deletions=0 makes the op abort
        # rather than destructively delete an unexpected baseline entity.
        r1 = await SyncSlurm.run(
            beanie_client, None, sitename=site.name, sacctmgr=mgr,
            apply=True, max_deletions=0,
        )
        for label, tally in r1['tally'].items():
            assert tally['failed'] == 0, (label, tally, r1['plan'])

        # Re-running must converge: the controller now matches desired state.
        r2 = await SyncSlurm.run(
            beanie_client, None, sitename=site.name, sacctmgr=mgr, apply=False,
        )
        assert r2['plan'] == {}, r2['plan']


class TestExportRootSSHKeys:
    """`ExportRootSSHKeys` renders root authorized_keys for admins at a site
    with root-ssh access, one `# user <email>` comment per user and each key
    prefixed with environment="REMOTE_SSH_USER=<user>"."""

    async def _admin(self, name, uid, *, access, site, keys):
        user = User(
            name=name, email=f'{name}@x.test', uid=uid, gid=uid,
            fullname=name, home_directory=f'/home/{name}', type='admin',
            status=await status_link('active'),
            access=await access_links(access),
        )
        await user.insert()
        if site is not None:
            await UserSiteInfo(
                user=user, site=site, status=await status_link('active'),
            ).insert()
        for k in keys:
            await SshKey(key=k, user=user).insert()
        return user

    async def test_export_filters_and_format(self, beanie_client):
        site = Site(name='rksite', fqdn='rk.test')
        await site.insert()
        other = Site(name='rkother', fqdn='rko.test')
        await other.insert()

        # included: two admins with root-ssh + keys (bob has two keys)
        await self._admin(
            'rk_alice', 80001, access=['login-ssh', 'root-ssh'], site=site,
            keys=['ssh-ed25519 AAAAaaa alice@laptop'],
        )
        await self._admin(
            'rk_bob', 80002, access=['root-ssh'], site=site,
            keys=['ssh-ed25519 AAAAbbb bob@a', 'ssh-rsa AAAAbbb2 bob@b'],
        )
        # excluded: admin with root-ssh but no keys
        await self._admin(
            'rk_nokeys', 80003, access=['root-ssh'], site=site, keys=[],
        )
        # excluded: admin without root-ssh
        await self._admin(
            'rk_noroot', 80004, access=['login-ssh'], site=site,
            keys=['ssh-ed25519 AAAAnoroot noroot@h'],
        )
        # excluded: non-admin with root-ssh
        u = User(
            name='rk_user', email='u@x.test', uid=80005, gid=80005,
            fullname='U', home_directory='/home/u', type='user',
            status=await status_link('active'),
            access=await access_links(['root-ssh']),
        )
        await u.insert()
        await UserSiteInfo(user=u, site=site, status=await status_link('active')).insert()
        await SshKey(key='ssh-ed25519 AAAAuser user@h', user=u).insert()
        # excluded: admin with root-ssh + key but NOT on this site
        await self._admin(
            'rk_offsite', 80006, access=['root-ssh'], site=other,
            keys=['ssh-ed25519 AAAAoff off@h'],
        )

        text = await ExportRootSSHKeys.run(beanie_client, None, sitename='rksite')

        assert text == (
            '# rk_alice <rk_alice@x.test>\n'
            'environment="REMOTE_SSH_USER=rk_alice" ssh-ed25519 AAAAaaa alice@laptop\n'
            '# rk_bob <rk_bob@x.test>\n'
            'environment="REMOTE_SSH_USER=rk_bob" ssh-ed25519 AAAAbbb bob@a\n'
            'environment="REMOTE_SSH_USER=rk_bob" ssh-rsa AAAAbbb2 bob@b\n'
        )
        # none of the excluded users leak in
        for name in ('rk_nokeys', 'rk_noroot', 'rk_user', 'rk_offsite'):
            assert name not in text

    async def test_export_empty_when_no_qualifying_admins(self, beanie_client):
        site = Site(name='rkempty', fqdn='rke.test')
        await site.insert()
        text = await ExportRootSSHKeys.run(beanie_client, None, sitename='rkempty')
        assert text == ''

    async def test_export_unknown_site_errors(self, beanie_client):
        with pytest.raises(ValueError):
            await ExportRootSSHKeys.run(beanie_client, None, sitename='nope')


class TestExportSympaEmails:
    """`ExportSympaEmails` lists user/admin emails at a site whose effective
    status is not inactive (disabled/offboarding included, per v1), minus an
    ignore set; sorted, one per line."""

    async def _u(self, name, uid, *, type, status, site, usi_status=None):
        user = User(
            name=name, email=f'{name}@x.test', uid=uid, gid=uid,
            fullname=name, home_directory=f'/home/{name}', type=type,
            status=await status_link(status),
            access=await access_links(['login-ssh']),
        )
        await user.insert()
        if site is not None:
            await UserSiteInfo(
                user=user, site=site,
                status=(await status_link(usi_status)) if usi_status else None,
            ).insert()
        return user

    async def test_filters_and_format(self, beanie_client):
        site = Site(name='sysite', fqdn='sy.test')
        await site.insert()
        other = Site(name='syother', fqdn='syo.test')
        await other.insert()

        await self._u('sy_user', 90001, type='user', status='active', site=site)
        await self._u('sy_admin', 90002, type='admin', status='active', site=site)
        # global inactive but per-site override active → included
        await self._u('sy_override', 90003, type='user', status='inactive',
                      site=site, usi_status='active')
        # disabled / offboarding → included (v1 policy)
        await self._u('sy_disabled', 90004, type='user', status='disabled', site=site)
        await self._u('sy_offb', 90005, type='user', status='offboarding', site=site)
        # excluded: inactive
        await self._u('sy_inactive', 90006, type='user', status='inactive', site=site)
        # excluded: non-user/admin types
        await self._u('sy_system', 90007, type='system', status='active', site=site)
        await self._u('sy_class', 90008, type='class', status='active', site=site)
        # excluded: active but not on this site
        await self._u('sy_offsite', 90009, type='user', status='active', site=other)
        # excluded: in the ignore set
        await self._u('hpc-help', 90010, type='admin', status='active', site=site)

        text = await ExportSympaEmails.run(
            beanie_client, None, sitename='sysite',
            ignore=['hpc-help@x.test'],
        )

        assert text == (
            'sy_admin@x.test\n'
            'sy_disabled@x.test\n'
            'sy_offb@x.test\n'
            'sy_override@x.test\n'
            'sy_user@x.test\n'
        )
        for excluded in ('sy_inactive', 'sy_system', 'sy_class',
                         'sy_offsite', 'hpc-help@x.test'):
            assert excluded not in text

    async def test_default_ignore_and_empty(self, beanie_client):
        # default ignore is hpc-help@ucdavis.edu
        site = Site(name='syempty', fqdn='sye.test')
        await site.insert()
        await self._u('sy_help', 90020, type='admin', status='active', site=site)
        # give them the default-ignored address
        u = await User.find_one(User.name == 'sy_help')
        u.email = 'hpc-help@ucdavis.edu'
        await u.save()

        text = await ExportSympaEmails.run(beanie_client, None, sitename='syempty')
        assert text == ''  # only user is the ignored help address

    async def test_unknown_site_errors(self, beanie_client):
        with pytest.raises(ValueError):
            await ExportSympaEmails.run(beanie_client, None, sitename='nope')


class TestExportPuppetStorage:
    """`ExportPuppetStorage` renders v1's `_storage_to_puppet` structure:
    zfs/nfs buckets, each user/group/share (category 'home' -> key 'user'),
    keyed by host. Whole managed datasets (subpath '', ZFSConfig present)
    are zfs entries with quota+permissions; subdirectory exports and
    unmanaged export roots (zfs=None) are options/ranges-only nfs entries;
    quobyte volumes are excluded entirely."""

    EXPORT = NFSExportConfig(
        export_options='rw,no_root_squash,sync',
        export_ranges=['10.19.0.0/16'],
    )

    async def _seed(self):
        user, group, site = await _seed_storage_actors('pups', 21000)
        grpvol = await _seed_volume(
            site, name='pupsgrp', host='nas-1', host_path='/nas-1/pupsgrp',
            allocations=[StorageAllocation(quota='70T')],
            nfs_export=self.EXPORT,
        )
        homevol = await _seed_volume(
            site, name='home/pups', host='nas-2', host_path='/home/pups',
            allocations=[StorageAllocation(quota='20G')],
            nfs_export=self.EXPORT,
        )
        # unmanaged export root (v1 plain-NFS bare volume): zfs=None
        barevol = StorageVolume(
            name='loose', site=site, backend='zfs', zfs=None,
            host='nas-1', host_path='/nas-1/loose',
            nfs_export=self.EXPORT,
        )
        await barevol.insert()
        qbvol = await _seed_volume(
            site, name='qb', host='qb01', host_path='/qb/pups',
            backend='quobyte',
        )

        def _storage(name, category, volume, subpath='', nfs_export=None):
            return Storage(
                name=name, site=site, category=category,
                owner=user, group=group, volume=volume,
                subpath=subpath, nfs_export=nfs_export,
            )

        # whole managed datasets → zfs bucket
        await _storage('pupsgrp', 'group', grpvol).insert()
        await _storage('pups', 'home', homevol).insert()
        # subdirectory export (Farm legacy home) → nfs bucket, own export
        await _storage(
            'pupssub', 'home', grpvol, subpath='pupssub',
            nfs_export=NFSExportConfig(
                export_options='rw,sync', export_ranges=['172.16.0.0/16'],
            ),
        ).insert()
        # whole-volume storage on an UNMANAGED root → nfs bucket
        await _storage('loose', 'share', barevol).insert()
        # quobyte-backed → excluded
        await _storage('qbstor', 'group', qbvol).insert()
        return site

    async def test_structure_and_classification(self, beanie_client):
        await self._seed()
        data = await ExportPuppetStorage.run(
            beanie_client, None, sitename='pupssite',
        )

        assert set(data) == {'zfs', 'nfs'}
        for bucket in ('zfs', 'nfs'):
            assert set(data[bucket]) == {'user', 'group', 'share'}

        assert data['zfs']['group'] == {'nas-1': [{
            'name': 'pupsgrp',
            'owner': 'pups',
            'group': 'pups',
            'path': '/nas-1/pupsgrp',
            'export_options': 'rw,no_root_squash,sync',
            'export_ranges': ['10.19.0.0/16'],
            'quota': '70T',
            'permissions': '2770',
        }]}
        assert data['zfs']['user'] == {'nas-2': [{
            'name': 'pups',
            'owner': 'pups',
            'group': 'pups',
            'path': '/home/pups',
            'export_options': 'rw,no_root_squash,sync',
            'export_ranges': ['10.19.0.0/16'],
            'quota': '20G',
            'permissions': '0770',
        }]}
        # subdir export: storage-level export config wins; no quota/perms
        assert data['nfs']['user'] == {'nas-1': [{
            'name': 'pupssub',
            'owner': 'pups',
            'group': 'pups',
            'path': '/nas-1/pupsgrp/pupssub',
            'export_options': 'rw,sync',
            'export_ranges': ['172.16.0.0/16'],
        }]}
        # unmanaged root: nfs bucket even with subpath ''
        assert data['nfs']['share'] == {'nas-1': [{
            'name': 'loose',
            'owner': 'pups',
            'group': 'pups',
            'path': '/nas-1/loose',
            'export_options': 'rw,no_root_squash,sync',
            'export_ranges': ['10.19.0.0/16'],
        }]}
        # quobyte storage appears nowhere
        assert data['zfs']['share'] == {}
        assert data['nfs']['group'] == {}

    async def test_empty_site(self, beanie_client):
        site = Site(name='pupempty', fqdn='pe.test')
        await site.insert()
        data = await ExportPuppetStorage.run(
            beanie_client, None, sitename='pupempty',
        )
        assert data == {
            'zfs': {'user': {}, 'group': {}, 'share': {}},
            'nfs': {'user': {}, 'group': {}, 'share': {}},
        }

    async def test_unknown_site_errors(self, beanie_client):
        with pytest.raises(ValueError):
            await ExportPuppetStorage.run(beanie_client, None, sitename='nope')


class TestRemoveSite:
    """`RemoveSite` cascade-deletes every per-site record (beanie has no
    reverse cascade) and the site, leaving other sites and global records
    untouched."""

    async def test_count_and_cascade(self, beanie_client):
        from cheeto.queries import count_site_dependents
        from cheeto.queries.site import SITE_LINKED_MODELS
        from cheeto.models.storage import AutomountMap

        # primary site with the full spread of per-site records
        site = await _seed_slurm_site()  # 'slsite': 2 USIs, 2 memberships,
        #   1 account/partition/qos(+1 alloc)/association
        alice = await User.find_one(User.name == 'sl_alice')
        group = await Group.find_one(Group.name == 'sllab')
        st_volume = await _seed_volume(site)
        await Storage(
            name='st1', site=site, category='home',
            owner=alice, group=group, volume=st_volume,
        ).insert()
        await StaticMount(
            name='stmnt', site=site, fstype='nfs4',
            volume=st_volume, mount_path='/stmnt',
        ).insert()
        await AutomountMap(name='home', site=site, prefix='/home').insert()
        await HippoEvent(
            hippo_id=1, hippo_endpoint='https://h.test',
            action='CreateAccount', site=site,
        ).insert()

        # an independent second site that must survive untouched
        other = Site(name='other_site', fqdn='o.test')
        await other.insert()
        ouser = User(
            name='o_user', email='o@x.test', uid=73001, gid=73001,
            fullname='O', home_directory='/home/o',
            status=await status_link('active'),
            access=await access_links(['slurm']),
        )
        await ouser.insert()
        await UserSiteInfo(
            user=ouser, site=other, status=await status_link('active'),
        ).insert()
        oalloc = SlurmAllocation(tres=SlurmTRES(cpus=4))
        await oalloc.insert()
        await SlurmQOS(name='o-qos', site=other, group_limits=[oalloc]).insert()

        counts = await count_site_dependents(site)
        assert counts == {
            'user_site_info': 2,
            'group_membership': 2,
            'slurm_associations': 1,
            'slurm_qos': 1,
            'slurm_partitions': 1,
            'slurm_accounts': 1,
            'storage': 1,
            'static_mounts': 1,
            'storage_volumes': 1,
            'automount_maps': 1,
            'hippo_events': 1,
            'slurm_allocations': 1,
        }

        result = await RemoveSite.run(beanie_client, None, sitename='slsite')
        assert result['site'] == 1
        assert result['user_site_info'] == 2
        assert result['slurm_allocations'] == 1

        # everything for slsite is gone
        assert await Site.find_one(Site.name == 'slsite') is None
        for _label, model in SITE_LINKED_MODELS:
            assert await model.find(model.site.id == site.id).count() == 0
        # the site's allocation is gone; only the other site's remains
        assert await SlurmAllocation.find_all().count() == 1

        # global records untouched
        assert await User.find_one(User.name == 'sl_alice') is not None
        assert await Group.find_one(Group.name == 'sllab') is not None

        # the second site is fully intact (no over-deletion)
        assert await Site.find_one(Site.name == 'other_site') is not None
        assert await UserSiteInfo.find(
            UserSiteInfo.site.id == other.id,
        ).count() == 1
        assert await SlurmQOS.find(SlurmQOS.site.id == other.id).count() == 1

    async def test_count_bare_site_all_zero(self, beanie_client):
        from cheeto.queries import count_site_dependents
        site = Site(name='baresite', fqdn='bare.test')
        await site.insert()
        counts = await count_site_dependents(site)
        assert set(counts.values()) == {0}

    async def test_remove_unknown_site_errors(self, beanie_client):
        with pytest.raises(ValueError):
            await RemoveSite.run(beanie_client, None, sitename='nope')


class TestSiteStorageSettings:

    async def test_mount_mechanisms_mutually_exclusive(self, beanie_client):
        from cheeto.models.site import SiteStorageSettings
        site = Site(name='ssset', fqdn='ssset.test')
        await site.insert()
        volume = await _seed_volume(site)
        amap = AutomountMap(name='home', site=site, prefix='/home')
        await amap.insert()
        smount = StaticMount(
            name='home', site=site, fstype='nfs4',
            volume=volume, mount_path='/home',
        )
        await smount.insert()

        with pytest.raises(ValueError, match='mutually exclusive'):
            SiteStorageSettings(
                home_automount_map=amap, home_static_mount=smount,
            )

        # mutate-then-save hole closed by Site's before_event
        site.storage.home_automount_map = amap
        await site.save()
        site.storage.home_static_mount = smount
        with pytest.raises(ValueError, match='mutually exclusive'):
            await site.save()

    async def test_quota_pattern_enforced(self, beanie_client):
        from cheeto.models.site import SiteStorageSettings
        with pytest.raises(ValueError):
            SiteStorageSettings(default_home_quota='not-a-quota')
        ok = SiteStorageSettings(default_home_quota='20G')
        assert ok.default_home_quota == '20G'

    async def test_resolve_settings_automount(self, beanie_client):
        # The labels `ng site show` renders for an automount-backed site.
        from cheeto.operations import SetSiteStorageDefaults
        from cheeto.queries import resolve_site_storage_settings
        site = Site(name='rss_auto', fqdn='rss-auto.test')
        await site.insert()
        await _seed_volume(site, name='home', host='flash01',
                           host_path='/flash/export/home')
        amap = AutomountMap(name='home', site=site, prefix='/home')
        await amap.insert()
        await SetSiteStorageDefaults.run(
            beanie_client, None, sitename='rss_auto',
            home_volume='home', home_quota='20G', home_automount_map='home',
        )
        site = await Site.find_one(Site.name == 'rss_auto')
        assert await resolve_site_storage_settings(site.storage) == {
            'default_home_volume': 'home',
            'default_home_quota': '20G',
            'home_automount_map': 'home',
            'home_static_mount': None,
        }

    async def test_resolve_settings_static_mount(self, beanie_client):
        from cheeto.operations import SetSiteStorageDefaults
        from cheeto.queries import resolve_site_storage_settings
        site = Site(name='rss_static', fqdn='rss-static.test')
        await site.insert()
        volume = await _seed_volume(site, name='home', host='flash01',
                                    host_path='/flash/export/home')
        smount = StaticMount(name='home', site=site, fstype='nfs4',
                             volume=volume, mount_path='/home')
        await smount.insert()
        await SetSiteStorageDefaults.run(
            beanie_client, None, sitename='rss_static',
            home_volume='home', home_quota='20G', home_static_mount='home',
        )
        site = await Site.find_one(Site.name == 'rss_static')
        resolved = await resolve_site_storage_settings(site.storage)
        assert resolved['default_home_volume'] == 'home'
        assert resolved['default_home_quota'] == '20G'
        assert resolved['home_automount_map'] is None
        assert resolved['home_static_mount'] == 'home'

    async def test_resolve_settings_empty(self, beanie_client):
        # A bare site (no storage defaults set) resolves to all-None.
        from cheeto.queries import resolve_site_storage_settings
        site = Site(name='rss_empty', fqdn='rss-empty.test')
        await site.insert()
        assert await resolve_site_storage_settings(site.storage) == {
            'default_home_volume': None,
            'default_home_quota': None,
            'home_automount_map': None,
            'home_static_mount': None,
        }


class TestCreateHomeStorageOp:

    async def _seed_site_with_defaults(self, beanie_client, *, static=False):
        """Site with a central home volume + defaults + a home mount
        mechanism, plus a user 'hsuser' with personal group."""
        from cheeto.operations import SetSiteStorageDefaults
        site = Site(name='hs_site', fqdn='hs.test')
        await site.insert()
        central = await _seed_volume(
            site, name='home', host='flash01',
            host_path='/flash/export/home',
        )
        if static:
            mount = StaticMount(
                name='home', site=site, fstype='nfs4',
                volume=central, mount_path='/home',
                options=['defaults', 'vers=4.2'],
            )
            await mount.insert()
            await SetSiteStorageDefaults.run(
                beanie_client, None, sitename='hs_site',
                home_volume='home', home_quota='20G',
                home_static_mount='home',
            )
        else:
            amap = AutomountMap(
                name='home', site=site, prefix='/home',
                options=['fstype=nfs', 'vers=4.2'],
            )
            await amap.insert()
            await SetSiteStorageDefaults.run(
                beanie_client, None, sitename='hs_site',
                home_volume='home', home_quota='20G',
                home_automount_map='home',
            )

        user, _ = await CreateUser.run(
            beanie_client, None,
            name='hsuser', email='hs@test.com', uid=74001,
            fullname='HS User',
        )
        return await Site.find_one(Site.name == 'hs_site')

    async def test_site_defaults_path(self, beanie_client):
        await self._seed_site_with_defaults(beanie_client)
        storage = await CreateHomeStorage.run(
            beanie_client, None,
            user_name='hsuser', site_name='hs_site',
        )
        volume = await StorageVolume.find_one(
            StorageVolume.name == 'home/hsuser', fetch_links=True,
            nesting_depth=1,
        )
        assert volume is not None
        assert volume.host == 'flash01'
        assert volume.host_path == '/flash/export/home/hsuser'
        assert volume.quota == '20G'
        assert volume.parent.name == 'home'

        fetched = await Storage.find_one(
            Storage.name == 'hsuser', fetch_links=True, nesting_depth=2,
        )
        assert fetched.category == 'home'
        assert fetched.subpath == ''
        assert fetched.quota == '20G'
        assert fetched.mount_path == '/home/hsuser'

    async def test_static_mount_site(self, beanie_client):
        await self._seed_site_with_defaults(beanie_client, static=True)
        await CreateHomeStorage.run(
            beanie_client, None,
            user_name='hsuser', site_name='hs_site',
        )
        fetched = await Storage.find_one(
            Storage.name == 'hsuser', fetch_links=True, nesting_depth=2,
        )
        assert fetched.automount_map is None
        assert fetched.static_mount is not None
        assert fetched.mount_path == '/home/hsuser'
        assert fetched.mount_options == ['defaults', 'vers=4.2']

    async def test_host_escape_hatch(self, beanie_client):
        site = Site(name='hs_site', fqdn='hs.test')
        await site.insert()
        await CreateUser.run(
            beanie_client, None,
            name='hsuser', email='hs@test.com', uid=74001,
            fullname='HS User',
        )
        await CreateHomeStorage.run(
            beanie_client, None,
            user_name='hsuser', site_name='hs_site',
            host='nas99', quota='50G',
        )
        volume = await StorageVolume.find_one(
            StorageVolume.name == 'home/hsuser',
        )
        assert volume.host == 'nas99'
        assert volume.host_path == '/home/hsuser'
        assert volume.quota == '50G'
        assert volume.parent is None

    async def test_no_defaults_no_args_errors(self, beanie_client):
        site = Site(name='hs_site', fqdn='hs.test')
        await site.insert()
        await CreateUser.run(
            beanie_client, None,
            name='hsuser', email='hs@test.com', uid=74001,
            fullname='HS User',
        )
        with pytest.raises(ValueError, match='no default home volume'):
            await CreateHomeStorage.run(
                beanie_client, None,
                user_name='hsuser', site_name='hs_site',
            )

    async def test_duplicate_home_errors(self, beanie_client):
        await self._seed_site_with_defaults(beanie_client)
        await CreateHomeStorage.run(
            beanie_client, None,
            user_name='hsuser', site_name='hs_site',
        )
        with pytest.raises(ValueError, match='already exists'):
            await CreateHomeStorage.run(
                beanie_client, None,
                user_name='hsuser', site_name='hs_site',
            )


class TestMigrateStorage:
    """v1 storage (collections + NFS/ZFS sources + automounts) → v2
    (StorageVolume + Storage + AutomountMap), incl. the Farm legacy acid
    test: subdir NFS exports of group ZFS volumes become subpath storages
    on a single covering volume."""

    def _connect_v1(self, dbname: str):
        from mongoengine import disconnect

        from cheeto.config import MongoConfig
        from cheeto.database import connect_mongoengine

        disconnect()
        mongo_cfg = MongoConfig(
            uri='127.0.0.1', port=MONGODB_PORT, user='', tls=False,
            password='', database=dbname,
        )
        connection = connect_mongoengine(mongo_cfg, quiet=True)
        connection.drop_database(dbname)
        return connection, mongo_cfg

    @staticmethod
    def _v1_actors(sitename: str):
        """v1 site + GlobalUsers/GlobalGroups needed by source refs."""
        from cheeto.database.site import Site as OldSite
        from cheeto.database.user import GlobalUser as OldGlobalUser
        from cheeto.database.group import GlobalGroup as OldGlobalGroup

        OldSite(sitename=sitename, fqdn=f'{sitename}.test').save()
        actors = {}
        for i, name in enumerate(('ajfinger', 'maccamp', 'maccamp2')):
            u = OldGlobalUser(
                username=name, email=f'{name}@test.com',
                uid=920000 + i, gid=920000 + i, fullname=name,
                shell='/bin/bash', home_directory=f'/home/{name}',
                type='user', status='active', access=['login-ssh'],
            )
            u.save()
            actors[name] = u
        groups = {}
        for i, name in enumerate(('ajfingergrp', 'maccamp', 'maccamp2')):
            g = OldGlobalGroup(groupname=name, gid=930000 + i, type='group')
            g.save()
            groups[name] = g
        return actors, groups

    async def _v2_actors(self, sitename: str):
        site = Site(name=sitename, fqdn=f'{sitename}.test')
        await site.insert()
        for i, name in enumerate(('ajfinger', 'maccamp', 'maccamp2')):
            await User(
                name=name, email=f'{name}@test.com',
                uid=920000 + i, gid=920000 + i, fullname=name,
                home_directory=f'/home/{name}',
            ).insert()
        for i, name in enumerate(('ajfingergrp', 'maccamp', 'maccamp2')):
            await Group(name=name, gid=930000 + i).insert()
        return site

    def _seed_farm_v1(self, sitename: str):
        """The full Farm scenario: home collection w/ defaults, legacy 70T
        group ZFS volume, subdir NFS exports (group + legacy home), a
        new-style collection-resolved home, and the automounts/storages."""
        from cheeto.database.storage import (
            Automount as OldAutomount,
            AutomountMap as OldAutomountMap,
            NFSMountSource as OldNFSMountSource,
            Storage as OldStorage,
            ZFSMountSource as OldZFSMountSource,
            ZFSSourceCollection,
        )

        actors, groups = self._v1_actors(sitename)

        home_col = ZFSSourceCollection(
            sitename=sitename, name='home',
            _host='nas-12', prefix='/export/home', _quota='20G',
        )
        home_col.save()
        group_col = ZFSSourceCollection(sitename=sitename, name='group')
        group_col.save()

        home_map = OldAutomountMap(
            sitename=sitename, tablename='home', prefix='/home',
            _options=['fstype=nfs', 'vers=4.2', 'actimeo=60'],
        )
        home_map.save()
        group_map = OldAutomountMap(
            sitename=sitename, tablename='group', prefix='/group',
            _options=['fstype=nfs', 'actimeo=60', 'vers=4.2'],
        )
        group_map.save()

        # Legacy group ZFS volume (name is a path, as load_group_storages
        # wrote them).
        zfs_grp = OldZFSMountSource(
            name='/nas-4-1/ajfingergrp', sitename=sitename,
            _host='nas-4-1', _host_path='/nas-4-1/ajfingergrp',
            _quota='70T', owner=actors['ajfinger'],
            group=groups['ajfingergrp'], collection=group_col,
        )
        zfs_grp.save()
        # Subdir NFS exports carved out of it.
        nfs_grp = OldNFSMountSource(
            name='ajfingergrp', sitename=sitename,
            _host='nas-4-1', _host_path='/nas-4-1/ajfingergrp/ajfingergrp',
            owner=actors['ajfinger'], group=groups['ajfingergrp'],
            collection=group_col,
        )
        nfs_grp.save()
        nfs_home = OldNFSMountSource(
            name='maccamp', sitename=sitename,
            _host='nas-4-1', _host_path='/nas-4-1/ajfingergrp/maccamp',
            owner=actors['maccamp'], group=groups['maccamp'],
            collection=home_col,
        )
        nfs_home.save()
        # New-style home: everything resolves through the collection.
        zfs_home2 = OldZFSMountSource(
            name='maccamp2', sitename=sitename,
            owner=actors['maccamp2'], group=groups['maccamp2'],
            collection=home_col,
        )
        zfs_home2.save()

        mounts = {}
        for name, mmap, opts in (
            ('ajfingerroot', group_map, ['fstype=nfs', 'actimeo=60', 'vers=4.2']),
            ('ajfingergrp', group_map, None),
            ('maccamp', home_map, None),
            ('maccamp2', home_map, None),
        ):
            m = OldAutomount(
                sitename=sitename, name=name, map=mmap,
                _options=opts or [],
            )
            m.save()
            mounts[name] = m

        OldStorage(name='ajfingerroot', source=zfs_grp,
                   mount=mounts['ajfingerroot']).save()
        OldStorage(name='ajfingergrp', source=nfs_grp,
                   mount=mounts['ajfingergrp']).save()
        OldStorage(name='maccamp', source=nfs_home,
                   mount=mounts['maccamp']).save()
        OldStorage(name='maccamp2', source=zfs_home2,
                   mount=mounts['maccamp2'], globus=True).save()

    async def test_farm_acid(self, beanie_client):
        from cheeto.operations import (
            MigrateAutomountMaps,
            MigrateStorageVolumes,
            MigrateStorages,
        )
        connection, cfg = self._connect_v1('cheeto_migrate_storage_test')
        try:
            self._seed_farm_v1('migfarm')
            site = await self._v2_actors('migfarm')

            await MigrateAutomountMaps.run(beanie_client, None)
            await MigrateStorageVolumes.run(beanie_client, None)
            await MigrateStorages.run(beanie_client, None)

            # --- maps ---
            home_map = await AutomountMap.find_one(
                AutomountMap.name == 'home', AutomountMap.site.id == site.id,
            )
            assert home_map.prefix == '/home'
            assert home_map.options == ['fstype=nfs', 'vers=4.2', 'actimeo=60']

            # --- volumes: home root, ajfingergrp (70T), home/maccamp2 ---
            vols = await StorageVolume.find(
                StorageVolume.site.id == site.id,
                fetch_links=True, nesting_depth=1,
            ).to_list()
            by_name = {v.name: v for v in vols}
            assert set(by_name) == {'home', 'ajfingergrp', 'home/maccamp2'}

            home_root = by_name['home']
            assert home_root.host == 'nas-12'
            assert home_root.host_path == '/export/home'
            assert home_root.quota is None

            grp_vol = by_name['ajfingergrp']
            assert grp_vol.host == 'nas-4-1'
            assert grp_vol.host_path == '/nas-4-1/ajfingergrp'
            assert grp_vol.quota == '70T'
            assert grp_vol.parent is None

            home2 = by_name['home/maccamp2']
            assert home2.host == 'nas-12'
            assert home2.host_path == '/export/home/maccamp2'
            assert home2.quota == '20G'   # from collection fallback
            assert home2.parent.name == 'home'

            # --- site storage defaults seeded from the home collection ---
            site = await Site.find_one(Site.name == 'migfarm')
            from cheeto.models.base import link_target_id
            assert link_target_id(site.storage.default_home_volume) == home_root.id
            assert site.storage.default_home_quota == '20G'

            # --- storages ---
            async def _stor(name, category):
                s = await Storage.find_one(
                    Storage.name == name, Storage.site.id == site.id,
                    Storage.category == category,
                    fetch_links=True, nesting_depth=2,
                )
                assert s is not None, (name, category)
                return s

            root = await _stor('ajfingerroot', 'group')
            assert root.volume.name == 'ajfingergrp'
            assert root.subpath == ''
            assert root.quota == '70T'
            assert root.mount_path == '/group/ajfingerroot'
            # explicit v1 _options land as replace-overrides
            assert root.mount_overrides.options == [
                'fstype=nfs', 'actimeo=60', 'vers=4.2',
            ]

            subdir = await _stor('ajfingergrp', 'group')
            assert subdir.volume.name == 'ajfingergrp'
            assert subdir.subpath == 'ajfingergrp'
            assert subdir.quota is None
            assert subdir.mount_path == '/group/ajfingergrp'

            home_stor = await _stor('maccamp', 'home')
            assert home_stor.volume.name == 'ajfingergrp'
            assert home_stor.subpath == 'maccamp'
            assert home_stor.quota is None
            assert home_stor.mount_path == '/home/maccamp'

            home2_stor = await _stor('maccamp2', 'home')
            assert home2_stor.volume.name == 'home/maccamp2'
            assert home2_stor.subpath == ''
            assert home2_stor.quota == '20G'
            assert home2_stor.mount_path == '/home/maccamp2'
            assert home2_stor.globus is True

            # --- idempotent re-run: nothing new, settings untouched ---
            n_vols = await StorageVolume.find_all().count()
            n_stor = await Storage.find_all().count()
            n_maps = await AutomountMap.find_all().count()
            await MigrateAutomountMaps.run(beanie_client, None)
            await MigrateStorageVolumes.run(beanie_client, None)
            await MigrateStorages.run(beanie_client, None)
            assert await StorageVolume.find_all().count() == n_vols
            assert await Storage.find_all().count() == n_stor
            assert await AutomountMap.find_all().count() == n_maps
            site = await Site.find_one(Site.name == 'migfarm')
            assert link_target_id(site.storage.default_home_volume) == home_root.id
        finally:
            connection.drop_database(cfg.database)

    async def test_unmatched_and_equal_path_nfs(self, beanie_client):
        """An NFS source with no covering ZFS volume becomes a bare
        unquota'd volume; one whose path equals a ZFS dataset root matches
        it (subpath '')."""
        from cheeto.database.storage import (
            NFSMountSource as OldNFSMountSource,
            ZFSMountSource as OldZFSMountSource,
        )
        from cheeto.operations import MigrateStorageVolumes

        connection, cfg = self._connect_v1('cheeto_migrate_storage_test2')
        try:
            actors, groups = self._v1_actors('mignfs')
            await self._v2_actors('mignfs')

            OldZFSMountSource(
                name='dataset', sitename='mignfs',
                _host='nasX', _host_path='/tank/dataset', _quota='1T',
                owner=actors['ajfinger'], group=groups['ajfingergrp'],
            ).save()
            # equal-path export of the dataset root → matched, no new volume
            OldNFSMountSource(
                name='dataset-export', sitename='mignfs',
                _host='nasX', _host_path='/tank/dataset',
                owner=actors['ajfinger'], group=groups['ajfingergrp'],
            ).save()
            # uncovered export → bare volume
            OldNFSMountSource(
                name='loose', sitename='mignfs',
                _host='nasX', _host_path='/tank/loose',
                owner=actors['ajfinger'], group=groups['ajfingergrp'],
            ).save()

            op = MigrateStorageVolumes(beanie_client, None)
            await op._run()
            assert op.zfs_volumes == 1
            assert op.nfs_matched == 1
            assert op.nfs_bare_volumes == 1

            site = await Site.find_one(Site.name == 'mignfs')
            vols = await StorageVolume.find(
                StorageVolume.site.id == site.id,
            ).to_list()
            assert {v.name for v in vols} == {'dataset', 'loose'}
            loose = next(v for v in vols if v.name == 'loose')
            assert loose.quota is None
            # bare-NFS volumes are unmanaged export roots, not datasets we
            # provision: no ZFSConfig (the puppet export's nfs/zfs marker)
            assert loose.zfs is None
            dataset = next(v for v in vols if v.name == 'dataset')
            assert dataset.zfs is not None
        finally:
            connection.drop_database(cfg.database)

    async def test_collection_root_enriched_by_equal_zfs_source(
        self, beanie_client,
    ):
        """The Farm nas-4-1 'home' regression: Pass A creates the collection
        root volume bare from a trailing-slash prefix; the equal-path ZFS
        source must ENRICH it (quota, export config inherited from the
        collection, managed marker) rather than being skipped — and the
        stored host_path must be normalized."""
        from cheeto.database.storage import (
            ZFSMountSource as OldZFSMountSource,
            ZFSSourceCollection,
        )
        from cheeto.operations import MigrateStorageVolumes

        connection, cfg = self._connect_v1('cheeto_migrate_storage_test5')
        try:
            actors, groups = self._v1_actors('migenrich')
            await self._v2_actors('migenrich')

            share_col = ZFSSourceCollection(
                sitename='migenrich', name='share',
                _host='nas-4-1', prefix='/nas-4-1/home/',  # trailing slash
                _export_options='rw,no_root_squash,sync',
                _export_ranges=['10.17.0.0/16', '127.0.1.1'],
            )
            share_col.save()
            # equal-path ZFS source: quota of its own, export via collection
            OldZFSMountSource(
                name='home', sitename='migenrich',
                _host='nas-4-1', _host_path='/nas-4-1/home', _quota='25T',
                owner=actors['ajfinger'], group=groups['ajfingergrp'],
                collection=share_col,
            ).save()

            op = MigrateStorageVolumes(beanie_client, None)
            await op._run()
            assert op.collection_roots == 1
            assert op.enriched == 1
            assert op.zfs_volumes == 0

            site = await Site.find_one(Site.name == 'migenrich')
            vols = await StorageVolume.find(
                StorageVolume.site.id == site.id,
            ).to_list()
            assert len(vols) == 1
            vol = vols[0]
            assert vol.host_path == '/nas-4-1/home'  # normalized
            assert vol.quota == '25T'
            assert vol.zfs is not None
            assert vol.nfs_export is not None
            assert vol.nfs_export.export_options == 'rw,no_root_squash,sync'
            assert vol.nfs_export.export_ranges == [
                '10.17.0.0/16', '127.0.1.1',
            ]

            # idempotent re-run: nothing left to enrich
            op2 = MigrateStorageVolumes(beanie_client, None)
            await op2._run()
            assert op2.enriched == 0
            assert op2.skipped_existing >= 1
        finally:
            connection.drop_database(cfg.database)

    async def test_missing_owner_skips_storage_not_volume(self, beanie_client):
        from cheeto.database.storage import (
            Automount as OldAutomount,
            AutomountMap as OldAutomountMap,
            Storage as OldStorage,
            ZFSMountSource as OldZFSMountSource,
        )
        from cheeto.operations import (
            MigrateAutomountMaps,
            MigrateStorageVolumes,
            MigrateStorages,
        )

        connection, cfg = self._connect_v1('cheeto_migrate_storage_test3')
        try:
            actors, groups = self._v1_actors('migowner')
            site = Site(name='migowner', fqdn='migowner.test')
            await site.insert()
            # v2 has NO users/groups → owner resolution must fail

            src = OldZFSMountSource(
                name='orphangrp', sitename='migowner',
                _host='nasY', _host_path='/tank/orphangrp', _quota='5T',
                owner=actors['ajfinger'], group=groups['ajfingergrp'],
            )
            src.save()
            gmap = OldAutomountMap(
                sitename='migowner', tablename='group', prefix='/group',
            )
            gmap.save()
            mount = OldAutomount(sitename='migowner', name='orphangrp', map=gmap)
            mount.save()
            OldStorage(name='orphangrp', source=src, mount=mount).save()

            await MigrateAutomountMaps.run(beanie_client, None)
            await MigrateStorageVolumes.run(beanie_client, None)
            op = MigrateStorages(beanie_client, None)
            await op._run()

            assert op.owners_missing == 1
            assert op.migrated == 0
            # the volume still exists (it's a real dataset)
            assert await StorageVolume.find(
                StorageVolume.site.id == site.id,
            ).count() == 1
            assert await Storage.find(
                Storage.site.id == site.id,
            ).count() == 0
        finally:
            connection.drop_database(cfg.database)

    async def test_cross_site_source(self, beanie_client):
        """v1 mount_source_site shape: source on site A, mount on site B →
        v2 Storage on B, volume on A."""
        from cheeto.database.site import Site as OldSite
        from cheeto.database.storage import (
            Automount as OldAutomount,
            AutomountMap as OldAutomountMap,
            Storage as OldStorage,
            ZFSMountSource as OldZFSMountSource,
        )
        from cheeto.operations import (
            MigrateAutomountMaps,
            MigrateStorageVolumes,
            MigrateStorages,
        )
        from cheeto.models.base import link_target_id

        connection, cfg = self._connect_v1('cheeto_migrate_storage_test4')
        try:
            actors, groups = self._v1_actors('migsrca')
            OldSite(sitename='migmntb', fqdn='migmntb.test').save()
            site_a = await self._v2_actors('migsrca')
            site_b = Site(name='migmntb', fqdn='migmntb.test')
            await site_b.insert()

            src = OldZFSMountSource(
                name='shared', sitename='migsrca',
                _host='nasZ', _host_path='/tank/shared', _quota='10T',
                owner=actors['ajfinger'], group=groups['ajfingergrp'],
            )
            src.save()
            bmap = OldAutomountMap(
                sitename='migmntb', tablename='group', prefix='/group',
            )
            bmap.save()
            mount = OldAutomount(sitename='migmntb', name='shared', map=bmap)
            mount.save()
            OldStorage(name='shared', source=src, mount=mount).save()

            await MigrateAutomountMaps.run(beanie_client, None)
            await MigrateStorageVolumes.run(beanie_client, None)
            op = MigrateStorages(beanie_client, None)
            await op._run()

            assert op.cross_site == 1
            assert op.migrated == 1
            storage = await Storage.find_one(Storage.name == 'shared')
            assert link_target_id(storage.site) == site_b.id
            volume = await StorageVolume.find_one(
                StorageVolume.name == 'shared',
            )
            assert link_target_id(volume.site) == site_a.id
        finally:
            connection.drop_database(cfg.database)


class TestMigrateSitesFilter:
    """`--sites` restricts migration to the named v1 sites: deprecated
    sites' records (site doc, USIs, membership edges, slurm, storage) are
    never created in v2, and a deprecated site's per-site access is NOT
    folded into the migrated global user access."""

    def _connect_v1(self, dbname: str):
        from mongoengine import disconnect

        from cheeto.config import MongoConfig
        from cheeto.database import connect_mongoengine

        disconnect()
        mongo_cfg = MongoConfig(
            uri='127.0.0.1', port=MONGODB_PORT, user='', tls=False,
            password='', database=dbname,
        )
        connection = connect_mongoengine(mongo_cfg, quiet=True)
        connection.drop_database(dbname)
        return connection, mongo_cfg

    def _seed_two_sites_v1(self):
        from cheeto.database.site import Site as OldSite
        from cheeto.database.user import GlobalUser as OldGlobalUser
        from cheeto.database.user import SiteUser as OldSiteUser
        from cheeto.database.group import (
            GlobalGroup as OldGlobalGroup,
            SiteGroup as OldSiteGroup,
        )
        from cheeto.database.slurm import SiteSlurmPartition
        from cheeto.database.storage import (
            Automount as OldAutomount,
            AutomountMap as OldAutomountMap,
            Storage as OldStorage,
            ZFSMountSource as OldZFSMountSource,
        )

        OldSite(sitename='keepsite', fqdn='keep.test').save()
        OldSite(sitename='depsite', fqdn='dep.test').save()

        user = OldGlobalUser(
            username='filtuser', email='filt@test.com',
            uid=940001, gid=940001, fullname='Filt User',
            shell='/bin/bash', home_directory='/home/filtuser',
            type='user', status='active', access=['login-ssh'],
        )
        user.save()
        su_keep = OldSiteUser(
            username='filtuser', sitename='keepsite', parent=user,
            _status='active',
        )
        su_keep.save()
        # The deprecated site grants root-ssh — this must NOT fold into
        # the migrated global access list.
        su_dep = OldSiteUser(
            username='filtuser', sitename='depsite', parent=user,
            _status='active', _access=['root-ssh'],
        )
        su_dep.save()

        grp = OldGlobalGroup(groupname='filtgrp', gid=950001, type='group')
        grp.save()
        OldSiteGroup(
            groupname='filtgrp', sitename='keepsite', parent=grp,
            _members=[su_keep],
        ).save()
        OldSiteGroup(
            groupname='filtgrp', sitename='depsite', parent=grp,
            _members=[su_dep],
        ).save()

        for sitename in ('keepsite', 'depsite'):
            SiteSlurmPartition(
                sitename=sitename, partitionname='low',
            ).save()
            amap = OldAutomountMap(
                sitename=sitename, tablename='group', prefix='/group',
                _options=['fstype=nfs'],
            )
            amap.save()
            src = OldZFSMountSource(
                name='filtgrp', sitename=sitename,
                _host='nas-1', _host_path=f'/nas-1/{sitename}/filtgrp',
                _quota='10T', owner=user, group=grp,
            )
            src.save()
            mount = OldAutomount(
                sitename=sitename, name='filtgrp', map=amap,
            )
            mount.save()
            OldStorage(name='filtgrp', source=src, mount=mount).save()

    async def test_filter_excludes_deprecated_site(self, beanie_client):
        from cheeto.models.storage import StorageVolume
        from cheeto.operations import (
            MigrateAutomountMaps,
            MigrateGroups,
            MigrateSites,
            MigrateSlurmPartitions,
            MigrateStorageVolumes,
            MigrateStorages,
            MigrateUsers,
        )

        connection, cfg = self._connect_v1('cheeto_migrate_filter_test')
        try:
            self._seed_two_sites_v1()
            keep = ['keepsite']

            # Instantiate directly so the filtered tally is inspectable.
            sites_op = MigrateSites(beanie_client, None, sitenames=keep)
            await sites_op._run()
            assert sites_op.filtered == 1

            await MigrateUsers.run(beanie_client, None, sitenames=keep)
            await MigrateGroups.run(beanie_client, None, sitenames=keep)
            await MigrateSlurmPartitions.run(beanie_client, None, sitenames=keep)
            await MigrateAutomountMaps.run(beanie_client, None, sitenames=keep)
            await MigrateStorageVolumes.run(beanie_client, None, sitenames=keep)
            await MigrateStorages.run(beanie_client, None, sitenames=keep)

            # Only the kept site exists in v2.
            sites = await Site.find_all().to_list()
            assert [s.name for s in sites] == ['keepsite']

            # The user migrated, but the deprecated site's root-ssh access
            # was NOT folded into the global list, and only the kept site's
            # USI exists.
            user = await User.find_one(User.name == 'filtuser')
            assert user is not None
            access = await access_links_to_names(user.access)
            assert set(access) == {'login-ssh'}
            usis = await UserSiteInfo.find(
                UserSiteInfo.user.id == user.id, fetch_links=True,
                nesting_depth=1,
            ).to_list()
            assert [usi.site.name for usi in usis] == ['keepsite']

            # Membership edges only on the kept site.
            edges = await GroupMembership.find_all().to_list()
            assert len(edges) == 1

            # Slurm + storage records only for the kept site.
            assert await SlurmPartition.find_all().count() == 1
            assert await AutomountMap.find_all().count() == 1
            assert await StorageVolume.find_all().count() == 1
            assert await Storage.find_all().count() == 1
            vol = await StorageVolume.find_one({})
            assert vol.host_path == '/nas-1/keepsite/filtgrp'
        finally:
            connection.drop_database(cfg.database)

    async def test_validate_sites_filter_typo_guard(self, beanie_client):
        from cheeto.database.site import Site as OldSite
        from cheeto.cmds.ng.migrate import _validate_sites_filter
        from cheeto.log import Console

        connection, cfg = self._connect_v1('cheeto_migrate_filter_typo')
        try:
            OldSite(sitename='realsite', fqdn='real.test').save()
            console = Console()
            assert _validate_sites_filter(console, None) is True
            assert _validate_sites_filter(console, ['realsite']) is True
            assert _validate_sites_filter(console, ['realsite', 'nope']) is False
        finally:
            connection.drop_database(cfg.database)


class TestMigrateSlurmAccountsUserGroups:
    """A group of type='user' must never receive a SlurmAccount during
    migration. v1 carried a default SiteSlurmAccount on every group; for
    personal user groups it was vestigial and ignored by association-building
    and Slurm syncs. A non-default embedded limit makes such a group pass the
    `has_limits` gate, so the type='user' skip is what keeps it out."""

    def _connect_v1(self, dbname: str):
        from mongoengine import disconnect

        from cheeto.config import MongoConfig
        from cheeto.database import connect_mongoengine

        disconnect()
        mongo_cfg = MongoConfig(
            uri='127.0.0.1', port=MONGODB_PORT, user='', tls=False,
            password='', database=dbname,
        )
        connection = connect_mongoengine(mongo_cfg, quiet=True)
        connection.drop_database(dbname)
        return connection, mongo_cfg

    def _seed_v1(self):
        from cheeto.database.site import Site as OldSite
        from cheeto.database.user import GlobalUser as OldGlobalUser
        from cheeto.database.user import SiteUser as OldSiteUser
        from cheeto.database.group import (
            GlobalGroup as OldGlobalGroup,
            SiteGroup as OldSiteGroup,
            SiteSlurmAccount,
        )
        from cheeto.database.slurm import (
            SiteSlurmAssociation,
            SiteSlurmPartition,
            SiteSlurmQOS,
        )

        OldSite(sitename='site1', fqdn='site1.test').save()

        user = OldGlobalUser(
            username='ug_user', email='ug@test.com',
            uid=960001, gid=960001, fullname='UG User',
            shell='/bin/bash', home_directory='/home/ug_user',
            type='user', status='active', access=['login-ssh', 'slurm'],
        )
        user.save()
        su = OldSiteUser(
            username='ug_user', sitename='site1', parent=user, _status='active',
        )
        su.save()

        # A normal lab group, carrying a non-default limit -> gets an account.
        lab = OldGlobalGroup(groupname='labgrp', gid=960100, type='group')
        lab.save()
        lab_sg = OldSiteGroup(
            groupname='labgrp', sitename='site1', parent=lab, _members=[su],
            slurm=SiteSlurmAccount(max_user_jobs=5),
        )
        lab_sg.save()

        # The user's personal group, also carrying a non-default limit so it
        # passes the has_limits gate -> must still be skipped (type='user').
        ug = OldGlobalGroup(groupname='ug_user', gid=960001, type='user')
        ug.save()
        ug_sg = OldSiteGroup(
            groupname='ug_user', sitename='site1', parent=ug, _members=[su],
            slurm=SiteSlurmAccount(max_user_jobs=5),
        )
        ug_sg.save()

        # A partition + QOS + an association on *each* group, so the
        # association migration is exercised for both.
        part = SiteSlurmPartition(sitename='site1', partitionname='low')
        part.save()
        qos = SiteSlurmQOS(sitename='site1', qosname='normal')
        qos.save()
        SiteSlurmAssociation(
            sitename='site1', qos=qos, partition=part, group=lab_sg,
        ).save()
        SiteSlurmAssociation(
            sitename='site1', qos=qos, partition=part, group=ug_sg,
        ).save()

    async def test_user_group_skipped_for_slurm_account(self, beanie_client):
        from cheeto.operations import (
            MigrateGroups,
            MigrateSites,
            MigrateSlurmAccounts,
            MigrateUsers,
        )

        connection, cfg = self._connect_v1('cheeto_migrate_usergroup_acct')
        try:
            self._seed_v1()

            await MigrateSites.run(beanie_client, None)
            await MigrateUsers.run(beanie_client, None)
            await MigrateGroups.run(beanie_client, None)

            accounts_op = MigrateSlurmAccounts(beanie_client, None)
            await accounts_op._run()

            assert accounts_op.user_groups == 1
            assert accounts_op.migrated == 1

            site = await Site.find_one(Site.name == 'site1')
            lab = await Group.find_one(Group.name == 'labgrp')
            ug = await Group.find_one(Group.name == 'ug_user')
            assert ug.type == 'user'

            lab_acct = await SlurmAccount.find_one(
                SlurmAccount.group.id == lab.id,
                SlurmAccount.site.id == site.id,
            )
            ug_acct = await SlurmAccount.find_one(
                SlurmAccount.group.id == ug.id,
                SlurmAccount.site.id == site.id,
            )
            assert lab_acct is not None
            assert ug_acct is None
        finally:
            connection.drop_database(cfg.database)

    async def test_user_group_skipped_for_slurm_association(self, beanie_client):
        from cheeto.operations import (
            MigrateGroups,
            MigrateSites,
            MigrateSlurmAccounts,
            MigrateSlurmAssociations,
            MigrateSlurmPartitions,
            MigrateSlurmQOSes,
            MigrateUsers,
        )

        connection, cfg = self._connect_v1('cheeto_migrate_usergroup_assoc')
        try:
            self._seed_v1()

            await MigrateSites.run(beanie_client, None)
            await MigrateUsers.run(beanie_client, None)
            await MigrateGroups.run(beanie_client, None)
            await MigrateSlurmPartitions.run(beanie_client, None)
            await MigrateSlurmQOSes.run(beanie_client, None)
            await MigrateSlurmAccounts.run(beanie_client, None)

            assoc_op = MigrateSlurmAssociations(beanie_client, None)
            await assoc_op._run()

            # The user group's association is skipped before the (absent)
            # account lookup; the lab group's association migrates normally.
            assert assoc_op.user_groups == 1
            assert assoc_op.migrated == 1

            site = await Site.find_one(Site.name == 'site1')
            lab = await Group.find_one(Group.name == 'labgrp')
            lab_acct = await SlurmAccount.find_one(
                SlurmAccount.group.id == lab.id,
                SlurmAccount.site.id == site.id,
            )
            assert await SlurmAssociation.find_all().count() == 1
            lab_assocs = await SlurmAssociation.find(
                SlurmAssociation.account.id == lab_acct.id,
            ).to_list()
            assert len(lab_assocs) == 1
        finally:
            connection.drop_database(cfg.database)


class TestMigrateTwoPass:
    """`migrate --sites A` followed by `migrate --sites B` must converge:
    users and groups migrated in the first pass still get their
    UserSiteInfo records, access extras, and GroupMembership edges for the
    newly onboarded site."""

    # Reuse the two-site v1 seed/connect helpers (plain functions; they
    # don't touch instance state).
    _connect_v1 = TestMigrateSitesFilter._connect_v1
    _seed_two_sites_v1 = TestMigrateSitesFilter._seed_two_sites_v1

    async def test_second_pass_adds_new_site_state(self, beanie_client):
        from cheeto.operations import (
            MigrateGroups,
            MigrateSites,
            MigrateUsers,
        )

        connection, cfg = self._connect_v1('cheeto_migrate_twopass_test')
        try:
            self._seed_two_sites_v1()

            # Pass 1: only keepsite.
            await MigrateSites.run(beanie_client, None, sitenames=['keepsite'])
            await MigrateUsers.run(beanie_client, None, sitenames=['keepsite'])
            await MigrateGroups.run(beanie_client, None, sitenames=['keepsite'])

            user = await User.find_one(User.name == 'filtuser')
            first_id = user.id
            assert await UserSiteInfo.find(
                UserSiteInfo.user.id == user.id,
            ).count() == 1
            assert await GroupMembership.find_all().count() == 1

            # Pass 2: depsite is now onboarded.
            await MigrateSites.run(beanie_client, None, sitenames=['depsite'])
            users_op = MigrateUsers(
                beanie_client, None, sitenames=['depsite'],
            )
            await users_op._run()
            groups_op = MigrateGroups(
                beanie_client, None, sitenames=['depsite'],
            )
            await groups_op._run()

            assert users_op.migrated == 0
            assert users_op.updated == 1
            assert groups_op.migrated == 0
            assert groups_op.skipped == 1
            assert groups_op.edges_created == 1

            # User not recreated; the new site's per-site access extra
            # folded in (add-only).
            user = await User.find_one(User.name == 'filtuser')
            assert user.id == first_id
            access = await access_links_to_names(user.access)
            assert set(access) == {'login-ssh', 'root-ssh'}

            dep = await Site.find_one(Site.name == 'depsite')
            usi = await UserSiteInfo.find_one(
                UserSiteInfo.user.id == user.id,
                UserSiteInfo.site.id == dep.id,
            )
            assert usi is not None
            group = await Group.find_one(Group.name == 'filtgrp')
            edge = await GroupMembership.find_one(
                GroupMembership.user.id == user.id,
                GroupMembership.group.id == group.id,
                GroupMembership.site.id == dep.id,
            )
            assert edge is not None
            assert edge.roles == ['member']

            # Re-running pass 2 is fully convergent: nothing new, no
            # duplicate-key errors from the unique USI/edge indexes.
            users_op2 = MigrateUsers(
                beanie_client, None, sitenames=['depsite'],
            )
            await users_op2._run()
            groups_op2 = MigrateGroups(
                beanie_client, None, sitenames=['depsite'],
            )
            await groups_op2._run()
            assert users_op2.updated == 0
            assert users_op2.unchanged == 1
            assert groups_op2.edges_created == 0
            assert groups_op2.edges_existing == 1
            assert await UserSiteInfo.find_all().count() == 2
            assert await GroupMembership.find_all().count() == 2
        finally:
            connection.drop_database(cfg.database)

    async def test_single_user_migrate_on_existing_converges(
        self, beanie_client,
    ):
        from cheeto.operations import MigrateSites, MigrateUser

        connection, cfg = self._connect_v1('cheeto_migrate_twopass_single')
        try:
            self._seed_two_sites_v1()
            await MigrateSites.run(beanie_client, None, sitenames=['keepsite'])
            await MigrateUser.run(
                beanie_client, None,
                username='filtuser', sitenames=['keepsite'],
            )

            # Previously raised 'already exists'; now converges.
            await MigrateSites.run(beanie_client, None, sitenames=['depsite'])
            op = MigrateUser(
                beanie_client, None,
                username='filtuser', sitenames=['depsite'],
            )
            await op._run()
            assert op.created is False
            assert op.site_infos_created == 1
            assert op.access_added == 1
        finally:
            connection.drop_database(cfg.database)


class TestMigrateStorageDanglingRefs:
    """Real v1 data contains dangling DBRefs (sources/collections whose
    targets were deleted). The storage migration must quarantine those
    records and keep going instead of aborting mid-batch with
    mongoengine DoesNotExist."""

    def _connect_v1(self, dbname: str):
        from mongoengine import disconnect

        from cheeto.config import MongoConfig
        from cheeto.database import connect_mongoengine

        disconnect()
        mongo_cfg = MongoConfig(
            uri='127.0.0.1', port=MONGODB_PORT, user='', tls=False,
            password='', database=dbname,
        )
        connection = connect_mongoengine(mongo_cfg, quiet=True)
        connection.drop_database(dbname)
        return connection, mongo_cfg

    async def test_dangling_refs_quarantined(self, beanie_client):
        from cheeto.database.site import Site as OldSite
        from cheeto.database.user import GlobalUser as OldGlobalUser
        from cheeto.database.group import GlobalGroup as OldGlobalGroup
        from cheeto.database.storage import (
            Automount as OldAutomount,
            AutomountMap as OldAutomountMap,
            Storage as OldStorage,
            ZFSMountSource as OldZFSMountSource,
            ZFSSourceCollection,
        )
        from cheeto.models.storage import StorageVolume
        from cheeto.operations import (
            MigrateAutomountMaps,
            MigrateStorageVolumes,
            MigrateStorages,
        )

        connection, cfg = self._connect_v1('cheeto_migrate_storage_dangle')
        try:
            OldSite(sitename='danglefarm', fqdn='dangle.test').save()
            owner = OldGlobalUser(
                username='dangler', email='d@test.com',
                uid=960001, gid=960001, fullname='Dangler',
                shell='/bin/bash', home_directory='/home/dangler',
                type='user', status='active', access=['login-ssh'],
            )
            owner.save()
            grp = OldGlobalGroup(groupname='danglegrp', gid=970001, type='group')
            grp.save()

            await Site(name='danglefarm', fqdn='dangle.test').insert()
            await User(
                name='dangler', email='d@test.com', uid=960001, gid=960001,
                fullname='Dangler', home_directory='/home/dangler',
            ).insert()
            await Group(name='danglegrp', gid=970001).insert()

            group_map = OldAutomountMap(
                sitename='danglefarm', tablename='group', prefix='/group',
            )
            group_map.save()

            # Good storage: fully intact.
            good_src = OldZFSMountSource(
                name='/nas-9/goodgrp', sitename='danglefarm',
                _host='nas-9', _host_path='/nas-9/goodgrp', _quota='1T',
                owner=owner, group=grp,
            )
            good_src.save()
            good_mount = OldAutomount(
                sitename='danglefarm', name='goodgrp', map=group_map,
            )
            good_mount.save()
            OldStorage(name='goodgrp', source=good_src,
                       mount=good_mount).save()

            # Broken storage: its source is deleted after the Storage was
            # saved, leaving a dangling DBRef (the production crash).
            bad_src = OldZFSMountSource(
                name='/nas-9/badgrp', sitename='danglefarm',
                _host='nas-9', _host_path='/nas-9/badgrp', _quota='1T',
                owner=owner, group=grp,
            )
            bad_src.save()
            bad_mount = OldAutomount(
                sitename='danglefarm', name='badgrp', map=group_map,
            )
            bad_mount.save()
            OldStorage(name='badgrp', source=bad_src, mount=bad_mount).save()
            bad_src.delete()

            # Source whose COLLECTION dangles: resolves host/path through
            # the (deleted) collection → must land in `unresolvable`.
            dangle_col = ZFSSourceCollection(
                sitename='danglefarm', name='home',
                _host='nas-9', prefix='/nas-9/home', _quota='20G',
            )
            dangle_col.save()
            col_src = OldZFSMountSource(
                name='colhome', sitename='danglefarm',
                owner=owner, group=grp, collection=dangle_col,
            )
            col_src.save()
            dangle_col.delete()

            await MigrateAutomountMaps.run(beanie_client, None)

            vols_op = MigrateStorageVolumes(beanie_client, None)
            await vols_op._run()
            assert vols_op.unresolvable == 1   # col_src
            # only the intact good_src became a volume (bad_src was deleted)
            assert await StorageVolume.find_all().count() == 1

            st_op = MigrateStorages(beanie_client, None)
            await st_op._run()
            assert st_op.dangling_refs == 1    # badgrp quarantined
            assert st_op.migrated == 1         # goodgrp still migrated
            assert await Storage.find_one(Storage.name == 'goodgrp') is not None
            assert await Storage.find_one(Storage.name == 'badgrp') is None
        finally:
            connection.drop_database(cfg.database)


class TestMigrateSiteGlobalsRerun:
    """Re-running MigrateSiteGlobals against already-populated sticky lists
    must be idempotent. Regression: beanie stores the embedded sticky links
    as inline snapshots, so a reloaded site rehydrates them as full
    Group/SlurmAccount documents (no .ref) — the dedup sets crashed with
    AttributeError on the second run."""

    def _connect_v1(self, dbname: str):
        from mongoengine import disconnect

        from cheeto.config import MongoConfig
        from cheeto.database import connect_mongoengine

        disconnect()
        mongo_cfg = MongoConfig(
            uri='127.0.0.1', port=MONGODB_PORT, user='', tls=False,
            password='', database=dbname,
        )
        connection = connect_mongoengine(mongo_cfg, quiet=True)
        connection.drop_database(dbname)
        return connection, mongo_cfg

    async def test_rerun_with_populated_sticky(self, beanie_client):
        from cheeto.database.site import Site as OldSite
        from cheeto.database.group import (
            GlobalGroup as OldGlobalGroup,
            SiteGroup as OldSiteGroup,
        )
        from cheeto.operations import MigrateSiteGlobals

        connection, cfg = self._connect_v1('cheeto_migrate_globals_rerun')
        try:
            grp_v1 = OldGlobalGroup(
                groupname='globgrp', gid=980001, type='group',
            )
            grp_v1.save()
            sg = OldSiteGroup(
                groupname='globgrp', sitename='globsite', parent=grp_v1,
            )
            sg.save()
            OldSite(
                sitename='globsite', fqdn='glob.test',
                global_groups=[sg], global_slurmers=[sg],
            ).save()

            site = Site(name='globsite', fqdn='glob.test')
            await site.insert()
            group = Group(name='globgrp', gid=980001)
            await group.insert()
            await SlurmAccount(group=group, site=site).insert()

            op1 = MigrateSiteGlobals(beanie_client, None)
            await op1._run()
            assert op1.groups_added == 1
            assert op1.slurmers_added == 1

            # Second run: the site reloads with rehydrated sticky snapshots.
            # Must dedup cleanly, adding nothing.
            op2 = MigrateSiteGlobals(beanie_client, None)
            await op2._run()
            assert op2.groups_added == 0
            assert op2.slurmers_added == 0
            assert op2.sites_updated == 0

            site = await Site.find_one(Site.name == 'globsite')
            assert len(site.group.sticky) == 1
            assert len(site.slurm.sticky) == 1
        finally:
            connection.drop_database(cfg.database)


class TestNoLinksInEmbeddedModels:
    """Tripwire: `Link`/`BackLink` may only be declared on Document classes.
    Beanie discovers link fields by walking each Document's own top-level
    model_fields at init — a Link nested inside an embedded BaseModel is
    invisible to it and silently stores the linked document as an INLINE
    SNAPSHOT (rehydrating as a full document with no .ref). Embedded models
    must use `DocRef` (models/base.py) instead. This test makes the mine
    impossible to re-plant."""

    def test_no_links_in_embedded_models(self):
        import typing

        from beanie import Document, Link, BackLink
        from pydantic import BaseModel as PydanticBaseModel

        from cheeto.models import ALL_MODELS

        def _contains_link(annotation) -> bool:
            origin = typing.get_origin(annotation)
            if origin in (Link, BackLink):
                return True
            return any(
                _contains_link(arg) for arg in typing.get_args(annotation)
            )

        def _embedded_models(annotation, seen):
            """Non-Document pydantic models reachable from an annotation."""
            if (
                isinstance(annotation, type)
                and issubclass(annotation, PydanticBaseModel)
                and not issubclass(annotation, Document)
                and annotation not in seen
            ):
                seen.add(annotation)
                yield annotation
                for f in annotation.model_fields.values():
                    yield from _embedded_models(f.annotation, seen)
                return
            for arg in typing.get_args(annotation):
                yield from _embedded_models(arg, seen)

        offenders = []
        seen: set[type] = set()
        for doc_model in ALL_MODELS:
            for doc_field in doc_model.model_fields.values():
                for embedded in _embedded_models(doc_field.annotation, seen):
                    for name, f in embedded.model_fields.items():
                        if _contains_link(f.annotation):
                            offenders.append(
                                f'{embedded.__name__}.{name}: {f.annotation}'
                            )

        assert not offenders, (
            'Link/BackLink declared inside embedded (non-Document) models — '
            'beanie cannot track these and will silently store inline '
            'document snapshots. Use DocRef (cheeto/models/base.py) '
            f'instead: {offenders}'
        )

    def test_no_underscore_named_event_hooks(self):
        """Beanie's init_actions iterates dir(cls) and SILENTLY SKIPS any
        attribute starting with '_' — an underscore-named @before_event/
        @after_event hook is never registered and never fires. (Discovered
        the hard way: `_normalize_settings`/`_revalidate` were dead letters
        that only appeared to work because validate_self_before re-runs
        pydantic validation on save/replace.)"""
        from cheeto.models import ALL_MODELS

        offenders = []
        for doc_model in ALL_MODELS:
            for attr in dir(doc_model):
                f = getattr(doc_model, attr, None)
                if (
                    callable(f)
                    and hasattr(f, 'has_action')
                    and attr.startswith('_')
                ):
                    offenders.append(f'{doc_model.__name__}.{attr}')

        assert not offenders, (
            'Event hooks named with a leading underscore are silently '
            'skipped by beanie init_actions and never fire — rename them '
            f'to public names: {offenders}'
        )


class TestEmbeddedRefStorage:
    """The Site*Settings DocRef fields must store bare ObjectIds in mongo —
    never inline document snapshots — and must self-heal legacy snapshot
    rows on read."""

    async def _seed(self, sitename='refsite'):
        site = Site(name=sitename, fqdn=f'{sitename}.test')
        await site.insert()
        group = Group(name=f'{sitename}grp', gid=991001)
        await group.insert()
        account = SlurmAccount(group=group, site=site)
        await account.insert()
        return site, group, account

    @staticmethod
    async def _raw_site(name):
        return await Site.get_pymongo_collection().find_one({'name': name})

    async def test_sticky_ops_store_bare_object_ids(self, beanie_client):
        from bson import ObjectId
        from cheeto.operations import AddStickySlurmAccount, AddStickyGroup

        site, group, account = await self._seed('refops')
        await AddStickySlurmAccount.run(
            beanie_client, None, sitename='refops',
            groupname='refopsgrp', default=True,
        )
        await AddStickyGroup.run(
            beanie_client, None, sitename='refops', groupname='refopsgrp',
        )

        raw = await self._raw_site('refops')
        assert raw['slurm']['sticky'] == [account.id]
        assert type(raw['slurm']['sticky'][0]) is ObjectId
        assert raw['slurm']['default_account'] == account.id
        assert raw['group']['sticky'] == [group.id]

    async def test_legacy_snapshot_self_heals(self, beanie_client):
        """A stored inline snapshot (the damaged production shape) reads
        back as a bare id, and the next save persists pure ObjectIds."""
        from bson import ObjectId

        site, group, account = await self._seed('refheal')
        # Damage the row the way the old Link-in-embedded model did:
        # a full inline copy of the account document.
        snapshot = {
            '_id': account.id,
            'created_at': account.created_at,
            'group': {'_id': group.id},
            'site': {'_id': site.id},
        }
        await Site.get_pymongo_collection().update_one(
            {'name': 'refheal'},
            {'$set': {
                'slurm.sticky': [snapshot],
                'slurm.default_account': snapshot,
            }},
        )

        loaded = await Site.find_one(Site.name == 'refheal')
        assert loaded.slurm.sticky == [account.id]       # coerced on read
        assert loaded.slurm.default_account == account.id

        await loaded.save()
        raw = await self._raw_site('refheal')
        assert raw['slurm']['sticky'] == [account.id]
        assert type(raw['slurm']['sticky'][0]) is ObjectId
        assert type(raw['slurm']['default_account']) is ObjectId

    async def test_inplace_document_append_normalized_at_save(
        self, beanie_client,
    ):
        """Safety net: appending a full Document in place (bypassing
        validation) still serializes as a bare id — the before_event
        normalize-and-reassign coerces it."""
        from bson import ObjectId

        site, group, account = await self._seed('refappend')
        site.slurm.sticky.append(account)        # full document, not an id
        site.slurm.default_account = account     # ditto
        await site.save()

        raw = await self._raw_site('refappend')
        assert raw['slurm']['sticky'] == [account.id]
        assert type(raw['slurm']['sticky'][0]) is ObjectId
        assert raw['slurm']['default_account'] == account.id


# ---------------------------------------------------------------------------
# HiPPO event processor idempotency
# ---------------------------------------------------------------------------


class _RecordingHippoHandler:
    """Stands in for a registered handler: counts handle() calls (and the
    notify flag) instead of running operations or sending email."""

    action = 'CreateAccount'

    def __init__(self, fail_times: int = 0):
        self.calls: list[bool] = []
        self.fail_times = fail_times

    async def handle(self, event, context, notify=True):
        self.calls.append(notify)
        if len(self.calls) <= self.fail_times:
            raise RuntimeError('handler boom')


class _FakePostbackEndpoint:
    """Replaces hippoapi's event_queue_update_status module in the
    operations.hippo namespace."""

    def __init__(self, status_code: int = 200):
        self.status_code = status_code
        self.calls: list[tuple[int, str]] = []

    async def asyncio_detailed(self, *, client, body):
        from types import SimpleNamespace
        self.calls.append((body.id, body.status))
        return SimpleNamespace(status_code=self.status_code, content=b'')


class TestHippoEventProcessor:
    """The local HippoEvent record must gate processing: without postback,
    HiPPO keeps serving processed events as Pending, and re-handling them
    duplicates work and notification emails."""

    _EVENT_DATA = {
        'groups': [{'name': 'hippogrp'}],
        'accounts': [{'kerberos': 'hippouser',
                      'name': 'Hippo User',
                      'email': 'hippo@x.test',
                      'iam': '1000000001',
                      'mothra': '09999999',
                      'key': '',
                      'accessTypes': ['SshKey']}],
        'cluster': 'hipposite',
        'metadata': {},
    }

    def _upstream(self, event_id=1):
        from ..hippoapi.models.queued_event_model import QueuedEventModel
        return QueuedEventModel.from_dict({
            'id': event_id,
            'action': 'CreateAccount',
            'status': 'Pending',
            'data': dict(self._EVENT_DATA),
        })

    def _processor(self, client, handler, max_tries=3):
        from ..config import HippoConfig
        from ..operations.hippo import (
            HippoEventProcessor,
            HippoHandlerRegistry,
        )
        registry = HippoHandlerRegistry()
        registry.register(handler)
        config = HippoConfig(
            api_key='test', base_url='http://hippo.test',
            site_aliases={}, max_tries=max_tries,
        )
        return HippoEventProcessor(client, config, registry=registry)

    def _patch_postback(self, monkeypatch, status_code=200):
        from ..operations import hippo as hippo_ops
        fake = _FakePostbackEndpoint(status_code)
        monkeypatch.setattr(hippo_ops, 'event_queue_update_status', fake)
        return fake

    async def _record(self):
        return await HippoEvent.find_one(HippoEvent.hippo_id == 1)

    async def test_no_postback_processes_once(self, beanie_client, monkeypatch):
        postback = self._patch_postback(monkeypatch)
        handler = _RecordingHippoHandler()
        processor = self._processor(beanie_client, handler)

        await processor._process_events([self._upstream()], object(), False)
        await processor._process_events([self._upstream()], object(), False)

        assert handler.calls == [True]
        record = await self._record()
        assert record.status == 'Complete'
        assert record.posted_back_at is None
        assert postback.calls == []

    async def test_later_postback_heals_without_rehandling(
        self, beanie_client, monkeypatch,
    ):
        postback = self._patch_postback(monkeypatch)
        handler = _RecordingHippoHandler()
        processor = self._processor(beanie_client, handler)

        await processor._process_events([self._upstream()], object(), False)
        await processor._process_events([self._upstream()], object(), True)

        assert handler.calls == [True]
        assert postback.calls == [(1, 'Complete')]
        record = await self._record()
        assert record.posted_back_at is not None

        # Once posted back, no further postbacks either.
        await processor._process_events([self._upstream()], object(), True)
        assert postback.calls == [(1, 'Complete')]

    async def test_postback_from_the_start(self, beanie_client, monkeypatch):
        postback = self._patch_postback(monkeypatch)
        handler = _RecordingHippoHandler()
        processor = self._processor(beanie_client, handler)

        await processor._process_events([self._upstream()], object(), True)

        assert handler.calls == [True]
        assert postback.calls == [(1, 'Complete')]
        assert (await self._record()).posted_back_at is not None

    async def test_failed_postback_retried_without_rehandling(
        self, beanie_client, monkeypatch,
    ):
        postback = self._patch_postback(monkeypatch, status_code=500)
        handler = _RecordingHippoHandler()
        processor = self._processor(beanie_client, handler)

        await processor._process_events([self._upstream()], object(), True)
        assert postback.calls == [(1, 'Complete')]
        assert (await self._record()).posted_back_at is None

        postback.status_code = 200
        await processor._process_events([self._upstream()], object(), True)
        assert handler.calls == [True]
        assert postback.calls == [(1, 'Complete'), (1, 'Complete')]
        assert (await self._record()).posted_back_at is not None

    async def test_failed_terminal_not_reprocessed(
        self, beanie_client, monkeypatch,
    ):
        postback = self._patch_postback(monkeypatch)
        handler = _RecordingHippoHandler(fail_times=10)
        processor = self._processor(beanie_client, handler, max_tries=1)

        await processor._process_events([self._upstream()], object(), False)
        record = await self._record()
        assert record.status == 'Failed'
        assert len(handler.calls) == 1

        await processor._process_events([self._upstream()], object(), True)
        assert len(handler.calls) == 1
        assert postback.calls == [(1, 'Failed')]

    async def test_retry_path_unblocked(self, beanie_client, monkeypatch):
        self._patch_postback(monkeypatch)
        handler = _RecordingHippoHandler(fail_times=1)
        processor = self._processor(beanie_client, handler, max_tries=3)

        await processor._process_events([self._upstream()], object(), False)
        record = await self._record()
        assert record.status == 'Pending'
        assert record.n_tries == 1

        await processor._process_events([self._upstream()], object(), False)
        assert len(handler.calls) == 2
        assert (await self._record()).status == 'Complete'


class TestStorageMountManagement:
    """Create automount tables and change/clear a Storage's mount mechanism
    (single record + volume-subtree bulk) after migration/creation."""

    async def test_create_automount_map_and_dup(self, beanie_client):
        _, _, site = await _seed_storage_actors('mm0', 60000)
        amap = await CreateAutomountMap.run(
            beanie_client, None, site_name=site.name,
            name='home', prefix='/home', options=['vers=4.2'],
        )
        assert amap.name == 'home'
        fetched = await AutomountMap.find_one(
            AutomountMap.name == 'home', AutomountMap.site.id == site.id,
        )
        assert fetched is not None
        assert fetched.prefix == '/home'
        assert fetched.options == ['vers=4.2']
        with pytest.raises(ValueError, match='already exists'):
            await CreateAutomountMap.run(
                beanie_client, None, site_name=site.name,
                name='home', prefix='/home',
            )

    async def test_set_storage_mount_transitions(self, beanie_client):
        user, group, site = await _seed_storage_actors('mm1', 60001)
        volume = await _seed_volume(site, name='homevol', host='nas0',
                                    host_path='/nas0/home')
        amap = AutomountMap(name='home', site=site, prefix='/home')
        await amap.insert()
        smount = StaticMount(name='homestatic', site=site, fstype='nfs4',
                             volume=volume, mount_path='/home')
        await smount.insert()
        storage = Storage(name='mm1', site=site, category='home',
                          owner=user, group=group, volume=volume,
                          automount_map=amap, mount_name='mm1')
        await storage.insert()

        # automount -> static
        await SetStorageMount.run(
            beanie_client, None, site_name=site.name, name='mm1',
            static_mount='homestatic',
        )
        fetched = await Storage.find_one(
            Storage.name == 'mm1', fetch_links=True, nesting_depth=2,
        )
        assert fetched.automount_map is None
        assert fetched.mount_name == ''
        assert fetched.static_mount is not None
        assert fetched.static_mount.name == 'homestatic'
        assert fetched.mount_path == '/home'

        # static -> none
        await SetStorageMount.run(
            beanie_client, None, site_name=site.name, name='mm1', no_mount=True,
        )
        fetched = await Storage.find_one(
            Storage.name == 'mm1', fetch_links=True, nesting_depth=2,
        )
        assert fetched.automount_map is None
        assert fetched.static_mount is None
        assert fetched.mount_path == ''

        # none -> automount
        await SetStorageMount.run(
            beanie_client, None, site_name=site.name, name='mm1',
            automount_map='home',
        )
        fetched = await Storage.find_one(
            Storage.name == 'mm1', fetch_links=True, nesting_depth=2,
        )
        assert fetched.static_mount is None
        assert fetched.automount_map is not None
        assert fetched.mount_path == '/home/mm1'

    def test_set_storage_mount_requires_exactly_one(self, beanie_client):
        with pytest.raises(ValueError, match='exactly one'):
            SetStorageMount(beanie_client, None, site_name='x', name='mm1')

    async def test_set_volume_storage_mounts_subtree(self, beanie_client):
        user, group, site = await _seed_storage_actors('mm2', 60002)
        parent = await _seed_volume(site, name='home', host='nas0',
                                    host_path='/nas0/home')
        child_a = await _seed_volume(site, name='home/a', host='nas0',
                                     host_path='/nas0/home/a', parent=parent)
        # grandchild — exercises full-depth descent
        child_b = await _seed_volume(site, name='home/a/b', host='nas0',
                                     host_path='/nas0/home/a/b', parent=child_a)
        other = await _seed_volume(site, name='scratch', host='nas0',
                                   host_path='/nas0/scratch')
        amap = AutomountMap(name='home', site=site, prefix='/home')
        await amap.insert()

        async def _mk(name, vol, cat='home'):
            await Storage(name=name, site=site, category=cat, owner=user,
                          group=group, volume=vol).insert()

        await _mk('sa', child_a)
        await _mk('sb', child_b)
        await _mk('sother', other, cat='group')

        result = await SetVolumeStorageMounts.run(
            beanie_client, None, site_name=site.name, volume_name='home',
            automount_map='home',
        )
        assert result['updated'] == 2          # sa + sb; sother excluded
        assert result['mechanism'] == 'automount:home'

        sa = await Storage.find_one(Storage.name == 'sa', fetch_links=True,
                                    nesting_depth=2)
        sb = await Storage.find_one(Storage.name == 'sb', fetch_links=True,
                                    nesting_depth=2)
        sother = await Storage.find_one(Storage.name == 'sother',
                                        fetch_links=True, nesting_depth=2)
        assert sa.automount_map is not None and sa.mount_path == '/home/sa'
        assert sb.automount_map is not None and sb.mount_path == '/home/sb'
        assert sother.automount_map is None    # outside the subtree, untouched

    async def test_storage_show_renders_full_detail(self, beanie_client):
        import io
        from rich.console import Console as RichConsole
        from ..cmds.ng.storage import _render_storage_panel
        from ..queries.storage import get_storage

        user, group, site = await _seed_storage_actors('mm3', 60003)
        parent = await _seed_volume(site, name='home', host='nas0',
                                    host_path='/nas0/home')
        child = await _seed_volume(
            site, name='home/mm3', host='nas0',
            host_path='/nas0/home/mm3', parent=parent,
            allocations=[StorageAllocation(quota='20G', comment='init')],
        )
        amap = AutomountMap(name='home', site=site, prefix='/home',
                            options=['vers=4.2'])
        await amap.insert()
        await Storage(name='mm3', site=site, category='home', owner=user,
                      group=group, volume=child, automount_map=amap,
                      mount_name='mm3').insert()

        storage = await get_storage(site, 'mm3')
        panel = _render_storage_panel(storage)   # exercises every property
        buf = io.StringIO()
        RichConsole(file=buf, width=120).print(panel)
        out = buf.getvalue()
        # storage + backing-volume + mount-mechanism detail all present
        assert 'mm3' in out
        assert 'home/mm3' in out          # backing volume name
        assert '/nas0/home/mm3' in out    # volume host_path
        assert 'automount:home' in out    # mount type label
        assert '/home/mm3' in out         # derived mount path
        assert '20G' in out               # volume quota


class TestCreateAccountHomeStorage:
    """The HiPPO CreateAccount handler provisions the new user's home storage
    from the site's storage defaults (previously a no-op stub)."""

    _EVENT = {
        'groups': [],
        'accounts': [{'kerberos': 'hippouser', 'name': 'Hippo User',
                      'email': 'hippo@x.test', 'iam': '1000000001',
                      'mothra': '09990001', 'key': '',
                      'accessTypes': ['SshKey']}],
        'cluster': 'hipposite',
        'metadata': {},
    }

    async def _seed_site(self, beanie_client, *, defaults=True):
        site = Site(name='hipposite', fqdn='hippo.test')
        await site.insert()
        if defaults:
            await _seed_volume(site, name='home', host='nas0',
                               host_path='/nas0/home')
            await AutomountMap(name='home', site=site, prefix='/home').insert()
            from cheeto.operations import SetSiteStorageDefaults
            await SetSiteStorageDefaults.run(
                beanie_client, None, sitename='hipposite',
                home_volume='home', home_quota='20G', home_automount_map='home',
            )

    async def _run_handler(self, beanie_client):
        from ..config import HippoConfig
        from ..hippoapi.models.queued_event_model import QueuedEventModel
        from ..operations.hippo import CreateAccountHandler, HippoContext
        upstream = QueuedEventModel.from_dict({
            'id': 1, 'action': 'CreateAccount', 'status': 'Pending',
            'data': dict(self._EVENT),
        })
        # In-memory record (not inserted) — the handler only stashes attrs on
        # it; the processor owns persistence.
        record = HippoEvent(hippo_id=1, hippo_endpoint='http://hippo.test',
                            action='CreateAccount', status='Pending',
                            cluster='hipposite')
        config = HippoConfig(api_key='x', base_url='http://hippo.test',
                             site_aliases={}, max_tries=3)
        context = HippoContext(client=beanie_client, hippo_client=None,
                               config=config, event_record=record, author=None)
        await CreateAccountHandler().handle(upstream.data, context, notify=False)

    async def test_provisions_home_from_defaults(self, beanie_client):
        await self._seed_site(beanie_client)
        await self._run_handler(beanie_client)
        storage = await Storage.find_one(
            Storage.name == 'hippouser', Storage.category == 'home',
            fetch_links=True, nesting_depth=2,
        )
        assert storage is not None
        assert storage.mount_path == '/home/hippouser'

    async def test_idempotent_on_reprocess(self, beanie_client):
        await self._seed_site(beanie_client)
        await self._run_handler(beanie_client)
        await self._run_handler(beanie_client)   # must not raise
        homes = await Storage.find(
            Storage.name == 'hippouser', Storage.category == 'home',
        ).to_list()
        assert len(homes) == 1

    async def test_no_defaults_skips_without_failing(self, beanie_client):
        await self._seed_site(beanie_client, defaults=False)
        await self._run_handler(beanie_client)   # must not raise
        assert await User.find_one(User.name == 'hippouser') is not None
        assert await Storage.find_one(
            Storage.name == 'hippouser', Storage.category == 'home',
        ) is None
