from typing import TYPE_CHECKING, Any, Dict, List, Type, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.queued_event_account_model import QueuedEventAccountModel
    from ..models.queued_event_data_model_metadata import QueuedEventDataModelMetadata
    from ..models.queued_event_group_model import QueuedEventGroupModel


T = TypeVar("T", bound="QueuedEventDataModel")


@_attrs_define
class QueuedEventDataModel:
    """
    Attributes:
        groups (List['QueuedEventGroupModel']):
        accounts (List['QueuedEventAccountModel']):
        cluster (str):
        metadata (QueuedEventDataModelMetadata):
    """

    groups: List["QueuedEventGroupModel"]
    accounts: List["QueuedEventAccountModel"]
    cluster: str
    metadata: "QueuedEventDataModelMetadata"

    def to_dict(self) -> Dict[str, Any]:
        groups = []
        for groups_item_data in self.groups:
            groups_item = groups_item_data.to_dict()
            groups.append(groups_item)

        accounts = []
        for accounts_item_data in self.accounts:
            accounts_item = accounts_item_data.to_dict()
            accounts.append(accounts_item)

        cluster = self.cluster

        metadata = self.metadata.to_dict()

        field_dict: Dict[str, Any] = {}
        field_dict.update(
            {
                "groups": groups,
                "accounts": accounts,
                "cluster": cluster,
                "metadata": metadata,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        from ..models.queued_event_account_model import QueuedEventAccountModel
        from ..models.queued_event_data_model_metadata import (
            QueuedEventDataModelMetadata,
        )
        from ..models.queued_event_group_model import QueuedEventGroupModel

        d = src_dict.copy()
        groups = []
        _groups = d.pop("groups")
        for groups_item_data in _groups:
            groups_item = QueuedEventGroupModel.from_dict(groups_item_data)

            groups.append(groups_item)

        accounts = []
        _accounts = d.pop("accounts")
        for accounts_item_data in _accounts:
            accounts_item = QueuedEventAccountModel.from_dict(accounts_item_data)

            accounts.append(accounts_item)

        cluster = d.pop("cluster")

        metadata = QueuedEventDataModelMetadata.from_dict(d.pop("metadata"))

        queued_event_data_model = cls(
            groups=groups,
            accounts=accounts,
            cluster=cluster,
            metadata=metadata,
        )

        return queued_event_data_model
