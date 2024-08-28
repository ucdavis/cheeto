from typing import Any, Dict, Type, TypeVar, Union

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="QueuedEventUpdateModel")


@_attrs_define
class QueuedEventUpdateModel:
    """
    Attributes:
        status (str): The new status of the event. Marking it as 'Complete' will trigger Action-specific processing in
            Hippo.
        id (Union[Unset, int]):
    """

    status: str
    id: Union[Unset, int] = UNSET

    def to_dict(self) -> Dict[str, Any]:
        status = self.status

        id = self.id

        field_dict: Dict[str, Any] = {}
        field_dict.update(
            {
                "status": status,
            }
        )
        if id is not UNSET:
            field_dict["id"] = id

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        status = d.pop("status")

        id = d.pop("id", UNSET)

        queued_event_update_model = cls(
            status=status,
            id=id,
        )

        return queued_event_update_model
