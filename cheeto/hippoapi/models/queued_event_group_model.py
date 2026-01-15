from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="QueuedEventGroupModel")


@_attrs_define
class QueuedEventGroupModel:
    """
    Attributes:
        name (str):
    """

    name: str

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        field_dict: dict[str, Any] = {}
        field_dict.update(
            {
                "name": name,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: dict[str, Any]) -> T:
        d = src_dict.copy()
        name = d.pop("name")

        queued_event_group_model = cls(
            name=name,
        )

        return queued_event_group_model
