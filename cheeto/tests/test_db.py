
from pathlib import Path

import pytest

from ..config import get_config
from ..database.base import connect_to_database


@pytest.fixture(scope='session')
def db_config(request):
    data_dir = Path(__file__).parent / 'data'
    return get_config(data_dir / 'config.yaml').mongo


def drop_database(config):
    conn = connect_to_database(config, quiet=True)
    conn.drop_database(config.database)


@pytest.fixture
def drop_before(db_config):
    drop_database(db_config)


@pytest.fixture
def drop_after(db_config):
    yield
    drop_database(db_config)


@pytest.fixture
def drop_before_after(db_config):
    drop_database(db_config)
    yield
    drop_database(db_config)


def test_connect_to_database(db_config):
    from ..database.base import connect_to_database
    from ..database import GlobalUser
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
        from ..database.site import Site
        from ..database.crud import create_site
        create_site('test-site', 'test.site.com')
        assert Site.objects.count() == 1


    def test_query_site_exists(self):
        from ..database.crud import query_site_exists
        assert query_site_exists('test-site') == True
        assert query_site_exists('fake-site') == False

