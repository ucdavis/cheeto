from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="DirectoryLoadCursor")


@_attrs_define
class DirectoryLoadCursor:
    """
    Attributes:
        estimated_result_count (Union[Unset, int]):
        current_index (Union[Unset, int]):
        more_results_url (Union[Unset, str]):
    """

    estimated_result_count: Union[Unset, int] = UNSET
    current_index: Union[Unset, int] = UNSET
    more_results_url: Union[Unset, str] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        estimated_result_count = self.estimated_result_count

        current_index = self.current_index

        more_results_url = self.more_results_url

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if estimated_result_count is not UNSET:
            field_dict["estimatedResultCount"] = estimated_result_count
        if current_index is not UNSET:
            field_dict["currentIndex"] = current_index
        if more_results_url is not UNSET:
            field_dict["moreResultsUrl"] = more_results_url

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: dict[str, Any]) -> T:
        d = src_dict.copy()
        estimated_result_count = d.pop("estimatedResultCount", UNSET)

        current_index = d.pop("currentIndex", UNSET)

        more_results_url = d.pop("moreResultsUrl", UNSET)

        directory_load_cursor = cls(
            estimated_result_count=estimated_result_count,
            current_index=current_index,
            more_results_url=more_results_url,
        )

        directory_load_cursor.additional_properties = d
        return directory_load_cursor

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
