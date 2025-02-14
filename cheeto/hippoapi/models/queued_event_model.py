import datetime
from typing import TYPE_CHECKING, Any, Dict, Type, TypeVar, Union

from attrs import define as _attrs_define
from dateutil.parser import isoparse

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.queued_event_data_model import QueuedEventDataModel


T = TypeVar("T", bound="QueuedEventModel")


@_attrs_define
class QueuedEventModel:
    """
    Attributes:
        action (str):
        status (str):
        data (QueuedEventDataModel):
        id (Union[Unset, int]):
        created_at (Union[Unset, datetime.datetime]):
        updated_at (Union[Unset, datetime.datetime]):
    """

    action: str
    status: str
    data: "QueuedEventDataModel"
    id: Union[Unset, int] = UNSET
    created_at: Union[Unset, datetime.datetime] = UNSET
    updated_at: Union[Unset, datetime.datetime] = UNSET

    def to_dict(self) -> Dict[str, Any]:
        action = self.action

        status = self.status

        data = self.data.to_dict()

        id = self.id

        created_at: Union[Unset, str] = UNSET
        if not isinstance(self.created_at, Unset):
            created_at = self.created_at.isoformat()

        updated_at: Union[Unset, str] = UNSET
        if not isinstance(self.updated_at, Unset):
            updated_at = self.updated_at.isoformat()

        field_dict: Dict[str, Any] = {}
        field_dict.update(
            {
                "action": action,
                "status": status,
                "data": data,
            }
        )
        if id is not UNSET:
            field_dict["id"] = id
        if created_at is not UNSET:
            field_dict["createdAt"] = created_at
        if updated_at is not UNSET:
            field_dict["updatedAt"] = updated_at

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        from ..models.queued_event_data_model import QueuedEventDataModel

        d = src_dict.copy()
        action = d.pop("action")

        status = d.pop("status")

        data = QueuedEventDataModel.from_dict(d.pop("data"))

        id = d.pop("id", UNSET)

        _created_at = d.pop("createdAt", UNSET)
        created_at: Union[Unset, datetime.datetime]
        if isinstance(_created_at, Unset):
            created_at = UNSET
        else:
            created_at = isoparse(_created_at)

        _updated_at = d.pop("updatedAt", UNSET)
        updated_at: Union[Unset, datetime.datetime]
        if isinstance(_updated_at, Unset):
            updated_at = UNSET
        else:
            updated_at = isoparse(_updated_at)

        queued_event_model = cls(
            action=action,
            status=status,
            data=data,
            id=id,
            created_at=created_at,
            updated_at=updated_at,
        )

        return queued_event_model
