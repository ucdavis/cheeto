from typing import TYPE_CHECKING, Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.directory_response_object import DirectoryResponseObject


T = TypeVar("T", bound="DirectorySearchReturnObject")


@_attrs_define
class DirectorySearchReturnObject:
    """
    Attributes:
        response_details (Union[Unset, str]):
        response_status (Union[Unset, int]):
        reponse_data (Union[Unset, DirectoryResponseObject]):
        api_deprecated_error (Union[Unset, str]):
    """

    response_details: Union[Unset, str] = UNSET
    response_status: Union[Unset, int] = UNSET
    reponse_data: Union[Unset, "DirectoryResponseObject"] = UNSET
    api_deprecated_error: Union[Unset, str] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        response_details = self.response_details

        response_status = self.response_status

        reponse_data: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.reponse_data, Unset):
            reponse_data = self.reponse_data.to_dict()

        api_deprecated_error = self.api_deprecated_error

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if response_details is not UNSET:
            field_dict["responseDetails"] = response_details
        if response_status is not UNSET:
            field_dict["responseStatus"] = response_status
        if reponse_data is not UNSET:
            field_dict["reponseData"] = reponse_data
        if api_deprecated_error is not UNSET:
            field_dict["apiDeprecatedError"] = api_deprecated_error

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: dict[str, Any]) -> T:
        from ..models.directory_response_object import DirectoryResponseObject

        d = src_dict.copy()
        response_details = d.pop("responseDetails", UNSET)

        response_status = d.pop("responseStatus", UNSET)

        _reponse_data = d.pop("reponseData", UNSET)
        reponse_data: Union[Unset, DirectoryResponseObject]
        if isinstance(_reponse_data, Unset):
            reponse_data = UNSET
        else:
            reponse_data = DirectoryResponseObject.from_dict(_reponse_data)

        api_deprecated_error = d.pop("apiDeprecatedError", UNSET)

        directory_search_return_object = cls(
            response_details=response_details,
            response_status=response_status,
            reponse_data=reponse_data,
            api_deprecated_error=api_deprecated_error,
        )

        directory_search_return_object.additional_properties = d
        return directory_search_return_object

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
