

from enum import verify
from ldap3 import Server, MOCK_SYNC
import pytest
from rich import print

from ..config import get_config, LDAPConfig
from ..ldap import LDAPInvalidUser, LDAPManager, LDAPUser, sort_on_attr



@pytest.fixture
def mock_config(testdata):
    config = get_config(testdata('config.yaml'))
    return config.ldap['test-server']


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
    ldap_mgr.connection.strategy.add_entry(mock_config.login_dn,
                                           {'userPassword': mock_config.password,
                                            'sn': 'bind_sn'})
    ldap_mgr.connection.strategy.entries_from_json(server_data)
    assert ldap_mgr.connection.bind()

    return ldap_mgr


@pytest.fixture
def testuser():
    return LDAPUser.Schema().load(dict(dn='uid=test-user,ou=users,dc=hpc,dc=ucdavis,dc=edu',
                                       uid='test-user',
                                       email='test@test.edu',
                                       uid_number=11111111,
                                       gid_number=11111111,
                                       fullname='Test User',
                                       surname='User'))


class TestLDAPUser:

    def test_home_directory_default(self, testuser):
        assert testuser.home_directory == '/home/test-user'


class TestLDAPManager:

    def test_private_search_user(self, mock_ldap):
        status, response = mock_ldap._search_user(['omen'])
        assert status
        assert response[0]['attributes']['uidNumber'] ==  457597

    def test_private_query_user(self, mock_ldap):
        result = mock_ldap._query_user('omen')
        assert result.failed == False
        assert len(result) == 1
        user = result.entries[0]
        assert user.uid == 'omen'
        assert user.cn == 'Omen Wild'
        assert user.uidNumber == 457597

    def test_query_user(self, mock_ldap):
        user = mock_ldap.query_user('omen')[0]
        assert user is not None
        assert user.uid == 'omen'
        assert user.fullname == 'Omen Wild'
        assert user.uid_number == 457597

    def test_private_query_users(self, mock_ldap):
        result = mock_ldap._query_user(['omen', 'janca'])
        assert len(result) == 2
        users = result.entries
        sort_on_attr(users, attr='uid')
        assert users[0].uid == 'janca'
        assert users[1].uid == 'omen'

    def test_query_users(self, mock_ldap):
        users = mock_ldap.query_user(['omen', 'janca'])
        assert len(users) == 2
        print(users)
        users.sort(key=lambda u: u.uid)
        assert users[0].uid == 'janca'
        assert users[1].uid == 'omen'

    def test_add_user(self, mock_ldap, testuser):
        entry = mock_ldap.add_user(testuser)
        newuser = mock_ldap.query_user(testuser.uid)[0]
        assert newuser == testuser

    def test_private_query_group(self, mock_ldap):
        result = mock_ldap._query_group('compute-ssh-users', 'hive')
        assert result.failed == False
        assert len(result) == 1
        group = result.entries[0]
        assert group.gidNumber == 4100000000
        assert group.memberUid == ['janca', 'omen']
        assert group.cn == 'compute-ssh-users'

    def test_query_group(self, mock_ldap):
        group = mock_ldap.query_group('compute-ssh-users', 'hive')
        assert group is not None
        assert group.gid == 'compute-ssh-users'
        assert group.gid_number == 4100000000
        assert group.members == {'janca', 'omen'}

    def test_add_user_to_group(self, mock_ldap):
        assert mock_ldap.query_group('ctbrowngrp', 'hive').members == {'janca'}
        committed = mock_ldap.add_user_to_group('omen', 'ctbrowngrp', 'hive')
        assert mock_ldap.query_group('ctbrowngrp', 'hive').members == {'janca', 'omen'}

    def test_add_nonexistent_user_to_group(self, mock_ldap):
        assert mock_ldap.query_group('ctbrowngrp', 'hive').members == {'janca'}
        with pytest.raises(LDAPInvalidUser):
            committed = mock_ldap.add_user_to_group('plumbus', 'ctbrowngrp', 'hive', verify_uid=True)
