"""Pure-function tests for cheeto/ldap_async.py — no fixtures, no network.

Exercises DNBuilder, escape_dn_value, and the entry_to_*/+*_to_entry_attrs
mapping helpers. The remainder of ldap_async.py (AsyncLDAPManager) is
covered by test_ldap_async.py against a real ephemeral slapd.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

from ..ldap_async import (
    GROUP_OBJECT_CLASSES,
    USER_OBJECT_CLASSES,
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


def _mock_entry(mapping: dict[str, list]) -> MagicMock:
    m = MagicMock()
    m.get = lambda k: mapping.get(k)
    m.__getitem__ = lambda _, k: mapping[k]
    return m


class TestEntryToUser:

    def test_basic(self):
        entry = _mock_entry({
            'uid': ['alice'],
            'mail': ['alice@example.test'],
            'uidNumber': ['10001'],
            'gidNumber': ['10001'],
            'displayName': ['Alice'],
            'homeDirectory': ['/home/alice'],
            'loginShell': ['/usr/bin/bash'],
            'sshPublicKey': ['ssh-ed25519 AAA', 'ssh-rsa BBB'],
        })
        record = entry_to_user(entry)
        assert record.username == 'alice'
        assert record.email == 'alice@example.test'
        assert record.uid == 10001
        assert record.gid == 10001
        assert record.fullname == 'Alice'
        assert record.home_directory == '/home/alice'
        assert record.shell == '/usr/bin/bash'
        assert record.ssh_keys == ['ssh-ed25519 AAA', 'ssh-rsa BBB']
        assert record.expires_at is None

    def test_missing_optional(self):
        entry = _mock_entry({
            'uid': ['bob'],
            'mail': ['bob@example.test'],
            'uidNumber': [10002],
            'gidNumber': [10002],
            'displayName': ['Bob'],
            'homeDirectory': ['/home/bob'],
            'loginShell': ['/usr/bin/bash'],
            # no ssh keys, no password, no shadowExpire
        })
        record = entry_to_user(entry)
        assert record.ssh_keys == []
        assert record.password is None
        assert record.expires_at is None

    def test_fullname_falls_back_to_cn(self):
        # No displayName, but cn is present.
        entry = _mock_entry({
            'uid': ['carol'],
            'mail': ['carol@x.test'],
            'uidNumber': ['10003'],
            'gidNumber': ['10003'],
            'cn': ['Carol Lastname'],
            'homeDirectory': ['/home/carol'],
            'loginShell': ['/usr/bin/zsh'],
        })
        record = entry_to_user(entry)
        assert record.fullname == 'Carol Lastname'

    def test_reads_password_str(self):
        entry = _mock_entry({
            'uid': ['pw'],
            'userPassword': ['{CRYPT}$y$j9T$salt$hash'],
        })
        assert entry_to_user(entry).password == '{CRYPT}$y$j9T$salt$hash'

    def test_reads_password_bytes(self):
        # userPassword is Octet String syntax; bonsai may return bytes.
        entry = _mock_entry({
            'uid': ['pw'],
            'userPassword': [b'{CRYPT}$y$j9T$salt$hash'],
        })
        assert entry_to_user(entry).password == '{CRYPT}$y$j9T$salt$hash'

    def test_password_absent_is_none(self):
        entry = _mock_entry({'uid': ['pw']})
        assert entry_to_user(entry).password is None


class TestUserToEntryAttrs:

    def test_basic(self):
        record = LDAPUserRecord(
            username='carol', email='carol@example.test',
            uid=10003, gid=10003, fullname='Carol',
            home_directory='/home/carol', shell='/usr/bin/zsh',
            ssh_keys=['ssh-ed25519 AAA'],
            password='*',
        )
        attrs = user_to_entry_attrs(record)
        assert attrs['objectClass'] == list(USER_OBJECT_CLASSES)
        # cn and displayName both written from fullname (operator-conventional).
        assert attrs['cn'] == ['Carol']
        assert attrs['displayName'] == ['Carol']
        assert attrs['uid'] == ['carol']
        assert attrs['mail'] == ['carol@example.test']
        assert attrs['uidNumber'] == [10003]
        assert attrs['sshPublicKey'] == ['ssh-ed25519 AAA']
        # Schemeless values get the RFC-2307 {CRYPT} prefix on the wire.
        assert attrs['userPassword'] == ['{CRYPT}*']

    def test_skips_empty_optionals(self):
        record = LDAPUserRecord(
            username='dave', email='dave@example.test',
            uid=10004, gid=10004, fullname='Dave',
            home_directory='/home/dave', shell='/usr/bin/bash',
            ssh_keys=[],          # skipped
            password=None,        # skipped
        )
        attrs = user_to_entry_attrs(record)
        assert 'sshPublicKey' not in attrs
        assert 'userPassword' not in attrs
        assert 'sn' not in attrs
        assert 'shadowExpire' not in attrs

    def test_writes_surname(self):
        record = LDAPUserRecord(
            username='eve', email='eve@x.test',
            uid=10005, gid=10005, fullname='Eve Smith',
            home_directory='/home/eve', shell='/usr/bin/bash',
            surname='Smith',
        )
        attrs = user_to_entry_attrs(record)
        assert attrs['sn'] == ['Smith']

    def test_password_crypt_prefix(self):
        # Bare yescrypt MCF hashes (what CreateUser/SetUserPassword store)
        # get the {CRYPT} scheme prefix slapd needs to verify them as
        # crypt(3) hashes; values already carrying a scheme pass through.
        record = LDAPUserRecord(
            username='pw', email='pw@x.test',
            uid=10006, gid=10006, fullname='PW',
            home_directory='/home/pw', shell='/usr/bin/bash',
            password='$y$j9T$abcdefsalt$abcdefhash',
        )
        attrs = user_to_entry_attrs(record)
        assert attrs['userPassword'] == ['{CRYPT}$y$j9T$abcdefsalt$abcdefhash']

        record.password = '{SSHA}already-schemed'
        attrs = user_to_entry_attrs(record)
        assert attrs['userPassword'] == ['{SSHA}already-schemed']


class TestShadowExpireRoundTrip:

    # 2030-01-01 00:00:00 UTC is exactly day 21915 since epoch.
    _DAY = 21915
    _DT = datetime(2030, 1, 1)

    def test_write_then_read_back(self):
        record = LDAPUserRecord(
            username='exp', email='exp@x.test',
            uid=10006, gid=10006, fullname='Exp',
            home_directory='/home/exp', shell='/usr/bin/bash',
            expires_at=self._DT,
        )
        attrs = user_to_entry_attrs(record)
        assert attrs['shadowExpire'] == [str(self._DAY)]

        entry = _mock_entry({
            'uid': ['exp'],
            'mail': ['exp@x.test'],
            'uidNumber': ['10006'],
            'gidNumber': ['10006'],
            'displayName': ['Exp'],
            'homeDirectory': ['/home/exp'],
            'loginShell': ['/usr/bin/bash'],
            'shadowExpire': [str(self._DAY)],
        })
        round_tripped = entry_to_user(entry)
        assert round_tripped.expires_at == self._DT

    def test_tz_aware_naive_equivalence(self):
        # A tz-aware UTC value and the matching naive value project to the
        # same shadowExpire day count.
        aware = LDAPUserRecord(
            username='a', email='a@x.test', uid=1, gid=1, fullname='A',
            home_directory='/home/a', shell='/usr/bin/bash',
            expires_at=datetime(2030, 1, 1, tzinfo=timezone.utc),
        )
        naive = LDAPUserRecord(
            username='a', email='a@x.test', uid=1, gid=1, fullname='A',
            home_directory='/home/a', shell='/usr/bin/bash',
            expires_at=datetime(2030, 1, 1),
        )
        assert (
            user_to_entry_attrs(aware)['shadowExpire']
            == user_to_entry_attrs(naive)['shadowExpire']
        )


class TestGroupMapping:

    def test_entry_to_group(self):
        entry = _mock_entry({
            'cn': ['staff'],
            'gidNumber': ['9001'],
            'memberUid': ['alice', 'bob'],
        })
        record = entry_to_group(entry)
        assert record.groupname == 'staff'
        assert record.gid == 9001
        assert record.members == {'alice', 'bob'}

    def test_entry_to_group_no_members(self):
        entry = _mock_entry({
            'cn': ['empty'],
            'gidNumber': ['9002'],
        })
        record = entry_to_group(entry)
        assert record.members == set()

    def test_group_to_entry_attrs(self):
        record = LDAPGroupRecord(
            groupname='staff', gid=9001, members={'alice', 'bob'},
        )
        attrs = group_to_entry_attrs(record)
        assert attrs['objectClass'] == list(GROUP_OBJECT_CLASSES)
        assert attrs['cn'] == ['staff']
        assert attrs['gidNumber'] == [9001]
        assert set(attrs['memberUid']) == {'alice', 'bob'}

    def test_group_to_entry_attrs_no_members(self):
        record = LDAPGroupRecord(groupname='empty', gid=9002, members=set())
        attrs = group_to_entry_attrs(record)
        assert 'memberUid' not in attrs


class TestAutomountMapping:

    def test_with_options(self):
        record = LDAPAutomountRecord(
            mountname='alice', mapname='auto.home',
            host='nfs.example', path='/srv/home/alice',
            options='-rw,nosuid',
        )
        attrs = automount_to_entry_attrs(record)
        assert attrs['objectClass'] == ['automount']
        assert attrs['automountKey'] == ['alice']
        # The host carries the ${HOST_SUFFIX} autofs variable (substituted
        # client-side at mount time) for backwards compatibility.
        assert attrs['automountInformation'] == [
            '-rw,nosuid nfs.example${HOST_SUFFIX}:/srv/home/alice',
        ]

    def test_no_options(self):
        record = LDAPAutomountRecord(
            mountname='alice', mapname='auto.home',
            host='nfs.example', path='/srv/home/alice',
        )
        attrs = automount_to_entry_attrs(record)
        assert attrs['automountInformation'] == [
            'nfs.example${HOST_SUFFIX}:/srv/home/alice',
        ]
