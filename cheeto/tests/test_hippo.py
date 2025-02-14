import pytest

from cheeto.hippoapi.api import notify

from ..hippoapi.models.queued_event_data_model import QueuedEventDataModel

from ..database import *
from ..hippo import handle_createaccount_event

from .conftest import drop_database

class TestCreateAccount:

    EVENT_DATA = {
        'groups': [{'name': 'testgrp'}],
        'accounts': [{'kerberos': 'test-user',
            'name': 'Test Testerson',
            'email': 'test-user@ucdavis.edu',
            'iam': '1000999999',
            'mothra': '09999999',
            'key': 'ssh-rsa AAAAB3NzaC1yc2EAAAADAQABwWSyQAeCDeKyiCsiVv comment',
            'accessTypes': ['OpenOnDemand', 'SshKey']}],
        'cluster': 'test-cluster',
        'metadata': {}
    }

    @pytest.fixture(autouse=True)
    def setup_site(self, db_config):
        drop_database(db_config)
        create_site('test-cluster', 'test.cluster.com')
        ZFSSourceCollection(name='home', sitename='test-cluster').save()
        AutomountMap(tablename='home', prefix='/home', sitename='test-cluster').save()
        create_group('testgrp', 5000, sites=['test-cluster'])
        yield
        drop_database(db_config)

    def test_setup(self):
        assert Site.objects.count() == 1
        assert SiteGroup.objects.count() == 1


    def test_handler(self, hippo_config):
        event = QueuedEventDataModel.from_dict(self.EVENT_DATA)
        client = object()
        handle_createaccount_event(event, client, hippo_config, notify=False)
        assert GlobalUser.objects.count() == 1
        assert GlobalUser.objects.get(username='test-user').iam_id == 1000999999
        assert SiteUser.objects.count() == 1
        assert 'test-user' in SiteGroup.objects.get(groupname='testgrp', sitename='test-cluster').members
        home = query_user_home_storage('test-cluster', GlobalUser.objects.get(username='test-user'))
        assert home.name == 'test-user'

    def test_handler_duplicate(self, hippo_config):
        event = QueuedEventDataModel.from_dict(self.EVENT_DATA)
        client = object()
        handle_createaccount_event(event, client, hippo_config, notify=False)
        handle_createaccount_event(event, client, hippo_config, notify=False)
        assert GlobalUser.objects.count() == 1
        assert SiteUser.objects.count() == 1
        assert Storage.objects.count() == 1

    def test_handler_updated_ssh(self, hippo_config):
        event = QueuedEventDataModel.from_dict(self.EVENT_DATA)
        client = object()
        handle_createaccount_event(event, client, hippo_config, notify=False)
        event_data = self.EVENT_DATA.copy()
        event_data['accounts'][0]['key'] = 'ssh-rsa AAAAB3NzaC1yc2EAAAADAQABwWSyQAeCDeKyiCsiVv comment2'
        event = QueuedEventDataModel.from_dict(event_data)
        handle_createaccount_event(event, client, hippo_config, notify=False)
        assert GlobalUser.objects.count() == 1
        assert GlobalUser.objects.get(username='test-user').ssh_key == ['ssh-rsa AAAAB3NzaC1yc2EAAAADAQABwWSyQAeCDeKyiCsiVv comment2']