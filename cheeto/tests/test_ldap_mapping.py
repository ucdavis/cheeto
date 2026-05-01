"""Pure-function tests for cheeto/ldap_async.py — no fixtures, no network.

Exercises DNBuilder, escape_dn_value, and the entry_to_*/+*_to_entry_attrs
mapping helpers. The remainder of ldap_async.py (AsyncLDAPManager) is
covered by test_ldap_async.py against a real ephemeral slapd.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from ..ldap_async import (
    DNBuilder,
    LDAPAutomountRecord,
    LDAPGroupRecord,
    LDAPUserRecord,
    automount_to_entry_attrs,
    entry_to_group,
    entry_to_user,
    escape_dn_value,
    group_to_entry_attrs,
    user_to_entry_attrs,
)


USER_ATTRS = {
    'username': 'uid',
    'email': 'mail',
    'uid': 'uidNumber',
    'gid': 'gidNumber',
    'fullname': 'displayName',
    'surname': 'sn',
    'home_directory': 'homeDirectory',
    'shell': 'loginShell',
    'ssh_keys': 'sshPublicKey',
    'password': 'userPassword',
}

GROUP_ATTRS = {
    'groupname': 'cn',
    'gid': 'gidNumber',
    'members': 'memberUid',
}


class TestEscapeDNValue:

    def test_unspecial(self):
        assert escape_dn_value('alice') == 'alice'

    def test_comma(self):
        assert escape_dn_value('a,b') == 'a\\,b'

    def test_backslash(self):
        assert escape_dn_value('a\\b') == 'a\\\\b'

    def test_leading_hash(self):
        assert escape_dn_value('#x') == '\\#x'

    def test_leading_space(self):
        assert escape_dn_value(' x') == '\\ x'

    def test_trailing_space(self):
        assert escape_dn_value('x ') == 'x\\ '

    def test_empty(self):
        assert escape_dn_value('') == ''

    def test_combined(self):
        # All RFC 4514 specials in one string
        assert escape_dn_value('a,b\\c"d=e') == 'a\\,b\\\\c\\"d\\=e'


class TestDNBuilder:

    def test_basic_dns(self):
        b = DNBuilder(
            searchbase='dc=test', user_base='ou=users,dc=test',
            sitename='farm',
        )
        assert b.site_ou_dn == 'ou=farm,dc=test'
        assert b.groups_ou_dn == 'ou=groups,ou=farm,dc=test'
        assert b.automount_ou_dn == 'ou=automount,ou=farm,dc=test'
        assert b.user_dn('alice') == 'uid=alice,ou=users,dc=test'
        assert b.group_dn('staff') == 'cn=staff,ou=groups,ou=farm,dc=test'
        assert b.automount_map_dn('auto.home') == (
            'automountMapName=auto.home,ou=automount,ou=farm,dc=test'
        )
        assert b.automount_dn('alice', 'auto.home') == (
            'automountKey=alice,automountMapName=auto.home,'
            'ou=automount,ou=farm,dc=test'
        )

    def test_escapes_unsafe_sitename(self):
        b = DNBuilder(
            searchbase='dc=test', user_base='ou=users,dc=test',
            sitename='farm,evil',
        )
        assert b.site_ou_dn == 'ou=farm\\,evil,dc=test'

    def test_escapes_unsafe_username(self):
        b = DNBuilder(
            searchbase='dc=test', user_base='ou=users,dc=test',
            sitename='farm',
        )
        assert b.user_dn('a,b') == 'uid=a\\,b,ou=users,dc=test'


class TestEntryMapping:

    def _mock_entry(self, mapping: dict[str, list]) -> MagicMock:
        m = MagicMock()
        m.get = lambda k: mapping.get(k)
        m.__getitem__ = lambda _, k: mapping[k]
        return m

    def test_entry_to_user_basic(self):
        entry = self._mock_entry({
            'uid': ['alice'],
            'mail': ['alice@example.test'],
            'uidNumber': ['10001'],
            'gidNumber': ['10001'],
            'displayName': ['Alice'],
            'homeDirectory': ['/home/alice'],
            'loginShell': ['/usr/bin/bash'],
            'sshPublicKey': ['ssh-ed25519 AAA', 'ssh-rsa BBB'],
        })
        record = entry_to_user(entry, USER_ATTRS)
        assert record.username == 'alice'
        assert record.email == 'alice@example.test'
        assert record.uid == 10001
        assert record.gid == 10001
        assert record.fullname == 'Alice'
        assert record.home_directory == '/home/alice'
        assert record.shell == '/usr/bin/bash'
        assert record.ssh_keys == ['ssh-ed25519 AAA', 'ssh-rsa BBB']

    def test_entry_to_user_missing_optional(self):
        entry = self._mock_entry({
            'uid': ['bob'],
            'mail': ['bob@example.test'],
            'uidNumber': [10002],
            'gidNumber': [10002],
            'displayName': ['Bob'],
            'homeDirectory': ['/home/bob'],
            'loginShell': ['/usr/bin/bash'],
            # no ssh keys, no password
        })
        record = entry_to_user(entry, USER_ATTRS)
        assert record.ssh_keys == []
        assert record.password is None

    def test_user_to_entry_attrs_basic(self):
        record = LDAPUserRecord(
            username='carol', email='carol@example.test',
            uid=10003, gid=10003, fullname='Carol',
            home_directory='/home/carol', shell='/usr/bin/zsh',
            ssh_keys=['ssh-ed25519 AAA'],
            password='*',
        )
        attrs = user_to_entry_attrs(
            record, USER_ATTRS,
            object_classes=['inetOrgPerson', 'posixAccount', 'ldapPublicKey'],
        )
        assert attrs['objectClass'] == [
            'inetOrgPerson', 'posixAccount', 'ldapPublicKey',
        ]
        assert attrs['cn'] == ['Carol']
        assert attrs['uid'] == ['carol']
        assert attrs['mail'] == ['carol@example.test']
        assert attrs['uidNumber'] == [10003]
        assert attrs['sshPublicKey'] == ['ssh-ed25519 AAA']
        assert attrs['userPassword'] == ['*']

    def test_user_to_entry_attrs_skips_empty(self):
        record = LDAPUserRecord(
            username='dave', email='dave@example.test',
            uid=10004, gid=10004, fullname='Dave',
            home_directory='/home/dave', shell='/usr/bin/bash',
            ssh_keys=[],            # skipped
            password=None,           # skipped
        )
        attrs = user_to_entry_attrs(
            record, USER_ATTRS,
            object_classes=['inetOrgPerson', 'posixAccount'],
        )
        assert 'sshPublicKey' not in attrs
        assert 'userPassword' not in attrs

    def test_entry_to_group(self):
        entry = self._mock_entry({
            'cn': ['staff'],
            'gidNumber': ['9001'],
            'memberUid': ['alice', 'bob'],
        })
        record = entry_to_group(entry, GROUP_ATTRS)
        assert record.groupname == 'staff'
        assert record.gid == 9001
        assert record.members == {'alice', 'bob'}

    def test_entry_to_group_no_members(self):
        entry = self._mock_entry({
            'cn': ['empty'],
            'gidNumber': ['9002'],
        })
        record = entry_to_group(entry, GROUP_ATTRS)
        assert record.members == set()

    def test_group_to_entry_attrs(self):
        record = LDAPGroupRecord(
            groupname='staff', gid=9001, members={'alice', 'bob'},
        )
        attrs = group_to_entry_attrs(
            record, GROUP_ATTRS,
            object_classes=['posixGroup', 'groupOfMembers'],
        )
        assert attrs['objectClass'] == ['posixGroup', 'groupOfMembers']
        assert attrs['cn'] == ['staff']
        assert attrs['gidNumber'] == [9001]
        assert set(attrs['memberUid']) == {'alice', 'bob'}

    def test_group_to_entry_attrs_no_members(self):
        record = LDAPGroupRecord(groupname='empty', gid=9002, members=set())
        attrs = group_to_entry_attrs(
            record, GROUP_ATTRS, object_classes=['posixGroup'],
        )
        assert 'memberUid' not in attrs

    def test_automount_to_entry_attrs_with_options(self):
        record = LDAPAutomountRecord(
            mountname='alice', mapname='auto.home',
            host='nfs.example', path='/srv/home/alice',
            options='-rw,nosuid',
        )
        attrs = automount_to_entry_attrs(record)
        assert attrs['objectClass'] == ['automount']
        assert attrs['automountKey'] == ['alice']
        assert attrs['automountInformation'] == [
            '-rw,nosuid nfs.example:/srv/home/alice',
        ]

    def test_automount_to_entry_attrs_no_options(self):
        record = LDAPAutomountRecord(
            mountname='alice', mapname='auto.home',
            host='nfs.example', path='/srv/home/alice',
        )
        attrs = automount_to_entry_attrs(record)
        assert attrs['automountInformation'] == ['nfs.example:/srv/home/alice']
