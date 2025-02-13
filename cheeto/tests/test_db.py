
from pathlib import Path

import pytest

from ..database import *

from .conftest import drop_database


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