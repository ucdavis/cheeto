from __future__ import annotations

import datetime
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

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
        id (int | Unset):
        created_at (datetime.datetime | Unset):
        updated_at (datetime.datetime | Unset):
    """

    action: str
    status: str
    data: QueuedEventDataModel
    id: int | Unset = UNSET
    created_at: datetime.datetime | Unset = UNSET
    updated_at: datetime.datetime | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        action = self.action

        status = self.status

        data = self.data.to_dict()

        id = self.id

        created_at: str | Unset = UNSET
        if not isinstance(self.created_at, Unset):
            created_at = self.created_at.isoformat()

        updated_at: str | Unset = UNSET
        if not isinstance(self.updated_at, Unset):
            updated_at = self.updated_at.isoformat()

        field_dict: dict[str, Any] = {}

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
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.queued_event_data_model import QueuedEventDataModel

        d = dict(src_dict)
        action = d.pop("action")

        status = d.pop("status")

        data = QueuedEventDataModel.from_dict(d.pop("data"))

        id = d.pop("id", UNSET)

        _created_at = d.pop("createdAt", UNSET)
        created_at: datetime.datetime | Unset
        if isinstance(_created_at, Unset):
            created_at = UNSET
        else:
            created_at = isoparse(_created_at)

        _updated_at = d.pop("updatedAt", UNSET)
        updated_at: datetime.datetime | Unset
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
