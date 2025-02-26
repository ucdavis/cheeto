
from pathlib import Path

import pytest

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
