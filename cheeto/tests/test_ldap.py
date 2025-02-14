

from enum import verify
from ldap3 import Server, MOCK_SYNC
import pytest
from rich import print

from ..ldap import LDAPGroup, LDAPInvalidUser, LDAPManager, LDAPUser, sort_on_attr



@pytest.fixture
def mock_ldap(testdata, config):
    server_info, server_schema, server_data = testdata('server_info.json',
                                                       'server_schema.json',
                                                       'server_entries.json')

    server = Server.from_definition('test-server', server_info, server_schema)
    ldap_mgr = LDAPManager(config.ldap,
                           servers=[server],
                           strategy=MOCK_SYNC,
                           auto_bind=False)
    ldap_mgr.connection.strategy.add_entry(config.ldap.login_dn,
                                           {'userPassword': config.ldap.password,
                                            'sn': 'bind_sn'})
    ldap_mgr.connection.strategy.entries_from_json(server_data)
    assert ldap_mgr.connection.bind()

    return ldap_mgr


@pytest.fixture
def testuser():
    return LDAPUser.Schema().load(dict(dn='uid=test-user,ou=users,dc=hpc,dc=ucdavis,dc=edu',
                                       username='test-user',
                                       email='test@test.edu',
                                       uid=11111111,
                                       gid=11111111,
                                       fullname='Test User',
                                       surname='User',
                                       ssh_keys=['ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQCzQ8z']))


@pytest.fixture
def testgroup():
    return LDAPGroup.Schema().load(dict(groupname='test-group',
                                        gid=22222222))


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
        assert user.uid== 'omen'
        assert user.cn == 'Omen Wild'
        assert user.uidNumber == 457597

    def test_query_user(self, mock_ldap):
        user = mock_ldap.query_user('omen')[0]
        assert user is not None
        assert user.username == 'omen'
        assert user.fullname == 'Omen Wild'
        assert user.uid == 457597

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
        users.sort(key=lambda u: u.username)
        assert users[0].username == 'janca'
        assert users[1].username == 'omen'

    def test_add_user(self, mock_ldap, testuser):
        entry = mock_ldap.add_user(testuser)
        newuser = mock_ldap.query_user(testuser.username)[0]
        assert newuser == testuser

    def test_private_query_group(self, mock_ldap):
        result = mock_ldap._query_group('compute-ssh-users', 'hpccf')
        assert result.failed == False
        assert len(result) == 1
        group = result.entries[0]
        assert group.gidNumber == 4000001004
        assert set(group.memberUid) == set(['janca', 'omen', 'camw'])
        assert group.cn == 'compute-ssh-users'

    def test_query_group(self, mock_ldap):
        group = mock_ldap.query_group('compute-ssh-users', 'hpccf')
        assert group is not None
        assert group.groupname == 'compute-ssh-users'
        assert group.gid == 4000001004
        assert group.members == {'janca', 'omen', 'camw'}

    def test_add_user_to_group(self, mock_ldap):
        assert mock_ldap.query_group('camw', 'hpccf').members == {'camw'}
        committed = mock_ldap.add_user_to_group('omen', 'camw', 'hpccf')
        assert mock_ldap.query_group('camw', 'hpccf').members == {'camw', 'omen'}

    def test_add_nonexistent_user_to_group(self, mock_ldap):
        assert mock_ldap.query_group('camw', 'hpccf').members == {'camw'}
        with pytest.raises(LDAPInvalidUser):
            committed = mock_ldap.add_user_to_group('plumbus', 'camw', 'hpccf', verify_user=True)

    def test_add_group_dn(self, mock_ldap, testgroup):
        dn = 'cn=test-group,ou=groups,ou=test-cluster,dc=hpc,dc=ucdavis,dc=edu'
        testgroup = LDAPGroup.from_other(testgroup, dn=dn)
        entry = mock_ldap.add_group(testgroup, 'ignore')
        newgroup = mock_ldap.query_group(testgroup.groupname, 'test-cluster')

        assert newgroup.dn == dn
        assert newgroup.gid == testgroup.gid
        assert newgroup.groupname == testgroup.groupname

    def test_add_group_no_dn(self, mock_ldap, testgroup):
        entry = mock_ldap.add_group(testgroup, 'hpccf')
        newgroup = mock_ldap.query_group(testgroup.groupname, 'hpccf')

        assert newgroup.dn == 'cn=test-group,ou=groups,ou=hpccf,dc=hpc,dc=ucdavis,dc=edu'
        assert newgroup.gid == testgroup.gid
        assert newgroup.groupname == testgroup.groupname
        assert newgroup.members == testgroup.members
