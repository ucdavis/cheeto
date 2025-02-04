from typing import TYPE_CHECKING, Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.identity_store_response_object import IdentityStoreResponseObject


T = TypeVar("T", bound="IdentityStoreSearchReturnObject")


@_attrs_define
class IdentityStoreSearchReturnObject:
    """
    Attributes:
        response_details (Union[Unset, str]):
        response_status (Union[Unset, int]):
        response_data (Union[Unset, IdentityStoreResponseObject]):
        api_deprecated_error (Union[Unset, str]):
    """

    response_details: Union[Unset, str] = UNSET
    response_status: Union[Unset, int] = UNSET
    response_data: Union[Unset, "IdentityStoreResponseObject"] = UNSET
    api_deprecated_error: Union[Unset, str] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        response_details = self.response_details

        response_status = self.response_status

        response_data: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.response_data, Unset):
            response_data = self.response_data.to_dict()

        api_deprecated_error = self.api_deprecated_error

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if response_details is not UNSET:
            field_dict["responseDetails"] = response_details
        if response_status is not UNSET:
            field_dict["responseStatus"] = response_status
        if response_data is not UNSET:
            field_dict["responseData"] = response_data
        if api_deprecated_error is not UNSET:
            field_dict["apiDeprecatedError"] = api_deprecated_error

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: dict[str, Any]) -> T:
        from ..models.identity_store_response_object import IdentityStoreResponseObject

        d = src_dict.copy()
        response_details = d.pop("responseDetails", UNSET)

        response_status = d.pop("responseStatus", UNSET)

        _response_data = d.pop("responseData", UNSET)
        response_data: Union[Unset, IdentityStoreResponseObject]
        if isinstance(_response_data, Unset):
            response_data = UNSET
        else:
            response_data = IdentityStoreResponseObject.from_dict(_response_data)

        api_deprecated_error = d.pop("apiDeprecatedError", UNSET)

        identity_store_search_return_object = cls(
            response_details=response_details,
            response_status=response_status,
            response_data=response_data,
            api_deprecated_error=api_deprecated_error,
        )

        identity_store_search_return_object.additional_properties = d
        return identity_store_search_return_object

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
