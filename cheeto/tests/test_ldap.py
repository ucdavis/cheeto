

from ldap3 import Server, MOCK_SYNC
import pytest
from rich import print

from ..config import LDAPConfig
from ..ldap import LDAPManager, sort_on_attr


TEST_LOGIN_DN = 'uid=test-user,ou=Services,dc=hpc,dc=ucdavis,dc=edu'
TEST_PASSWORD = 'test-password'


def get_ldap_config():
    config = LDAPConfig(servers=['test-server'],
                        searchbase='dc=hpc,dc=ucdavis,dc=edu',
                        login_dn=TEST_LOGIN_DN,
                        password=TEST_PASSWORD,
                        user_classes=['inetOrgPerson', 'posixAccount']
                        )
    return config


@pytest.fixture
def mock_config():
    return get_ldap_config()


@pytest.fixture
def mock_ldap(testdata, mock_config):
    server_info, server_schema, server_data = testdata('server_info.json',
                                                       'server_schema.json',
                                                       'server_entries.json')

    server = Server.from_definition('test-server', server_info, server_schema)
    ldap_mgr = LDAPManager(mock_config,
                           servers=[server],
                           strategy=MOCK_SYNC,
                           auto_bind=False)
    ldap_mgr.connection.strategy.add_entry(TEST_LOGIN_DN,
                                           {'userPassword': TEST_PASSWORD,
                                            'sn': 'bind_sn'})
    ldap_mgr.connection.strategy.entries_from_json(server_data)
    assert ldap_mgr.connection.bind()

    return ldap_mgr


class TestLDAPManager:

    def test_query_user(self, mock_ldap):
        result = mock_ldap.query_user('omen')
        assert result.failed == False
        assert len(result) == 1
        user = result.entries[0]
        assert user.uid == 'omen'
        assert user.cn == 'Omen Wild'
        assert user.uidNumber == 457597

    def test_query_users(self, mock_ldap):
        result = mock_ldap.query_users(['omen', 'janca'])
        assert len(result) == 2
        users = result.entries
        sort_on_attr(users, attr='uid')
        assert users[0].uid == 'janca'
        assert users[1].uid == 'omen'

