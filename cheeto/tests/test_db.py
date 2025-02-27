
from pathlib import Path

import pytest

from cheeto.puppet import MIN_SYSTEM_UID

from ..database import *

from .conftest import drop_database, run_shell_cmd


def test_connect_to_database(db_config):
    conn = connect_to_database(db_config, quiet=True)
    print(conn.server_info)
    # Should only have admin, local, and config databases
    assert len(list(conn.list_databases())) == 3
    assert GlobalUser.objects.count() == 0


class TestSite:

    @pytest.fixture(autouse=True, scope='class')
    def setup_class(self, db_config):
        drop_database(db_config)
        yield
        drop_database(db_config)
    
    def test_create_site(self):
        create_site('test-site', 'test.site.com')
        assert Site.objects.count() == 1


    def test_query_site_exists(self):
        assert query_site_exists('test-site') == True
        assert query_site_exists('fake-site') == False


class TestUser:

    USER_ARGS = ('test-user', 'test-user@test.com', 10000, 'Test Testerson')

    @pytest.fixture(autouse=True)
    def setup_site(self, db_config):
        drop_database(db_config)
        create_site('test-site', 'test.site.com')
        ZFSSourceCollection(sitename='test-site', name='home').save()
        AutomountMap(sitename='test-site', prefix='/home', tablename='home').save()
        for doc in (GlobalUser, GlobalGroup, SiteUser, SiteGroup):
            doc.ensure_indexes()
        yield
        drop_database(db_config)

    def test_create_user(self):
        user, group = create_user(*self.USER_ARGS)
        assert user.username == 'test-user'
        assert group.gid == 10000
        assert GlobalUser.objects.count() == 1
        assert GlobalGroup.objects.count() == 1

    def test_create_user_with_site(self):
        user, group = create_user(*self.USER_ARGS,
                                   sitenames=['test-site'])
        assert SiteUser.objects.count() == 1
        assert SiteGroup.objects.count() == 1
        assert SiteUser.objects.get(username='test-user').parent == user

    def test_create_duplicate_user(self):
        create_user(*self.USER_ARGS)
        with pytest.raises(DuplicateGlobalUser):
            create_user(*self.USER_ARGS)
    
    def test_add_site_user(self):
        user, group = create_user(*self.USER_ARGS)
        suser, sgroup = add_site_user('test-site', user)

        assert suser.username == 'test-user'
        assert suser.parent == user
    
    def test_add_duplicate_site_user(self):
        user, group = create_user(*self.USER_ARGS)
        add_site_user('test-site', user)
        with pytest.raises(DuplicateSiteUser):
            add_site_user('test-site', user)
    
    def test_new_system_user_command(self, run_cmd):
        run_cmd('database', 'user', 'new', 'system',
                'test-user',
                '--email', 'test-user@test.com',
                '--fullname', 'Test Testerson')
        assert GlobalUser.objects.count() == 1
        assert GlobalUser.objects.get(username='test-user').email == 'test-user@test.com'
        assert GlobalUser.objects.get(username='test-user').fullname == 'Test Testerson'
        assert GlobalUser.objects.get(username='test-user').type == 'system'
        assert GlobalUser.objects.get(username='test-user').uid == MIN_SYSTEM_UID

    def test_add_user_site_command(self, run_cmd):
        run_cmd('database', 'user', 'new', 'system',
                'test-user',
                '--email', 'test-user@test.com',
                '--fullname', 'Test Testerson')
        run_cmd('database', 'user', 'add', 'site',
                '-u', 'test-user',
                '--site', 'test-site')
        assert SiteUser.objects.count() == 1
        assert SiteUser.objects.get(username='test-user').parent == GlobalUser.objects.get(username='test-user')
        assert SiteUser.objects.get(username='test-user').sitename == 'test-site'

    def test_add_user_site_create_storage_command(self, run_cmd):
        run_cmd('database', 'user', 'new', 'system',
                'test-user',
                '--email', 'test-user@test.com',
                '--fullname', 'Test Testerson')
        run_cmd('database', 'user', 'add', 'site',
                '-u', 'test-user',
                '--site', 'test-site',
                '--create-storage')
        assert SiteUser.objects.count() == 1
        assert SiteUser.objects.get(username='test-user').parent == GlobalUser.objects.get(username='test-user')
        assert SiteUser.objects.get(username='test-user').sitename == 'test-site'
        assert Storage.objects.count() == 1
        assert Storage.objects.get(name='test-user').source.sitename == 'test-site'
        
    def test_remove_user_site_command(self, run_cmd):
        run_cmd('database', 'user', 'new', 'system',
                'test-user',
                '--email', 'test-user@test.com',
                '--fullname', 'Test Testerson')
        run_cmd('database', 'user', 'add', 'site',
                '-u', 'test-user',
                '--site', 'test-site')
        run_cmd('database', 'user', 'remove', 'site',
                '-u', 'test-user',
                '--site', 'test-site')
        assert SiteUser.objects.count() == 0
        assert GlobalUser.objects.count() == 1
        
    def test_set_user_status_command_global(self, run_cmd):
        run_cmd('database', 'user', 'new', 'system',
                'test-user',
                '--email', 'test-user@test.com',
                '--fullname', 'Test Testerson')
        run_cmd('database', 'user', 'set', 'status', 'inactive', '-u', 'test-user', '-r', 'testing')
        assert GlobalUser.objects.get(username='test-user').status == 'inactive'
        assert 'testing' in GlobalUser.objects.get(username='test-user').comments[0]

    def test_set_user_status_command_site(self, run_cmd):
        run_cmd('database', 'user', 'new', 'system',
                'test-user',
                '--email', 'test-user@test.com',
                '--fullname', 'Test Testerson')
        run_cmd('database', 'user', 'add', 'site',
                '-u', 'test-user',
                '--site', 'test-site')
        run_cmd('database', 'user', 'set', 'status', 'inactive', '-u', 'test-user', '-r', 'testing', '-s', 'test-site')
        
        assert GlobalUser.objects.get(username='test-user').status == 'active'
        assert SiteUser.objects.get(username='test-user').status == 'inactive'
        assert 'testing' in GlobalUser.objects.get(username='test-user').comments[0]

class TestGroup:

    @pytest.fixture(autouse=True)
    def setup_site(self, db_config):
        drop_database(db_config)
        create_site('test-site', 'test.site.com')
        yield
        drop_database(db_config)

    def test_create_system_group_command(self, run_cmd):
        run_cmd('database', 'group', 'new', 'system',
                '--groups', 'test-group',
                '--site', 'test-site')
        assert GlobalGroup.objects.count() == 1
        assert SiteGroup.objects.count() == 1
        assert SiteGroup.objects.get(groupname='test-group', sitename='test-site').parent == GlobalGroup.objects.get(groupname='test-group')


class TestSlurm:

    @pytest.fixture(autouse=True)
    def setup_site(self, db_config):
        drop_database(db_config)
        create_site('test-site', 'test.site.com')
        yield
        drop_database(db_config)

    def test_create_qos(self):
        qos = create_slurm_qos('test-qos',
                               'test-site',
                               group_limits=SlurmTRES(cpus=16, mem='1000M', gpus=0),
                               user_limits=SlurmTRES(cpus=16, mem='1000M', gpus=0),
                               job_limits=SlurmTRES(cpus=16, mem='1000M', gpus=0))
        assert qos.qosname == 'test-qos'
        assert qos.sitename == 'test-site'
        assert SiteSlurmQOS.objects.count() == 1

    def test_create_qos_cleaning(self):
        qos = create_slurm_qos('test-qos',
                               'test-site',
                               group_limits=SlurmTRES(cpus=16, mem='1G', gpus=0),
                               user_limits=SlurmTRES(cpus=16, mem='1G', gpus=0),
                               job_limits=SlurmTRES(cpus=16, mem='1G', gpus=0))
        assert qos.group_limits.mem == '1024M'
        assert qos.user_limits.mem == '1024M'
        assert qos.job_limits.mem == '1024M'

    def test_create_qos_command(self, run_cmd):
        run_cmd('database', 'slurm', 'new', 'qos',
                '--qosname', 'test-qos',
                '--site', 'test-site',
                '--group-limits', 'cpus=16,mem=1G,gpus=0',
                '--user-limits', 'cpus=16,mem=1G,gpus=0',
                '--job-limits', 'cpus=16,mem=1G,gpus=0')
        assert SiteSlurmQOS.objects.count() == 1
        assert SiteSlurmQOS.objects.get(qosname='test-qos').group_limits.mem == '1024M'
        assert SiteSlurmQOS.objects.get(qosname='test-qos').user_limits.mem == '1024M'
        assert SiteSlurmQOS.objects.get(qosname='test-qos').job_limits.mem == '1024M'

    def test_edit_qos_command(self, run_cmd):
        run_cmd('database', 'slurm', 'new', 'qos',
                '--qosname', 'test-qos',
                '--site', 'test-site',
                '--group-limits', 'cpus=16,mem=1G,gpus=0',
                '--user-limits', 'cpus=16,mem=1G,gpus=0',
                '--job-limits', 'cpus=16,mem=1G,gpus=0')
        run_cmd('database', 'slurm', 'edit', 'qos',
                '--qosname', 'test-qos',
                '--site', 'test-site',
                '--group-limits', 'cpus=32,mem=16G,gpus=1',
                '--flags', 'DenyOnLimit')
        assert SiteSlurmQOS.objects.get(qosname='test-qos').group_limits.mem == '16384M'
        assert SiteSlurmQOS.objects.get(qosname='test-qos').group_limits.cpus == 32
        assert SiteSlurmQOS.objects.get(qosname='test-qos').group_limits.gpus == 1
        assert SiteSlurmQOS.objects.get(qosname='test-qos').flags == ['DenyOnLimit']
    
    def test_remove_qos_command(self, run_cmd):
        run_cmd('database', 'slurm', 'new', 'qos',
                '--qosname', 'test-qos',
                '--site', 'test-site',
                '--group-limits', 'cpus=16,mem=1G,gpus=0',
                '--user-limits', 'cpus=16,mem=1G,gpus=0',
                '--job-limits', 'cpus=16,mem=1G,gpus=0')
        run_cmd('database', 'slurm', 'remove', 'qos',
                '--qosname', 'test-qos',
                '--site', 'test-site')
        assert SiteSlurmQOS.objects.count() == 0
    
    def test_create_partition_command(self, run_cmd):
        run_cmd('database', 'slurm', 'new', 'partition',
                '--name', 'test-partition',
                '--site', 'test-site')
        assert SiteSlurmPartition.objects.count() == 1
        assert SiteSlurmPartition.objects.get(partitionname='test-partition').sitename == 'test-site'

    def test_remove_partition_command(self, run_cmd):
        run_cmd('database', 'slurm', 'new', 'partition',
                '--name', 'test-partition',
                '--site', 'test-site')
        assert SiteSlurmPartition.objects.count() == 1
        run_cmd('database', 'slurm', 'remove', 'partition',
                '--name', 'test-partition',
                '--site', 'test-site')
        assert SiteSlurmPartition.objects.count() == 0

    def test_create_assoc_command(self, run_cmd):
        create_group('test-group', 10000, sites=['test-site'])
        create_slurm_partition('test-partition', 'test-site')

        run_cmd('database', 'slurm', 'new', 'qos',
                '--qosname', 'test-qos',
                '--site', 'test-site',
                '--group-limits', 'cpus=16,mem=1G,gpus=0',
                '--user-limits', 'cpus=16,mem=1G,gpus=0',
                '--job-limits', 'cpus=16,mem=1G,gpus=0')
        run_cmd('database', 'slurm', 'new', 'assoc',
                '--site', 'test-site',
                '--group', 'test-group',
                '--partition', 'test-partition',
                '--qos', 'test-qos')
        assert SiteSlurmAssociation.objects.count() == 1
        assoc = query_slurm_association('test-site', 'test-qos', 'test-partition', 'test-group')
        assert assoc.group.groupname == 'test-group'
        assert assoc.partition.partitionname == 'test-partition'
        assert assoc.qos.qosname == 'test-qos'

    def test_remove_qos_assoc_cascade(self, run_cmd):
        create_group('test-group', 10000, sites=['test-site'])
        create_slurm_partition('test-partition', 'test-site')
        create_slurm_qos('test-qos', 'test-site')
        create_slurm_association('test-site', 'test-partition', 'test-group', 'test-qos')
        run_cmd('database', 'slurm', 'remove', 'qos',
                '--qosname', 'test-qos', '--site', 'test-site')
        assert SiteSlurmQOS.objects.count() == 0
        assert SiteSlurmAssociation.objects.count() == 0
