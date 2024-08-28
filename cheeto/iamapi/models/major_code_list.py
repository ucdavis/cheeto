from typing import TYPE_CHECKING, Any, Dict, List, Type, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.major_code import MajorCode


T = TypeVar("T", bound="MajorCodeList")


@_attrs_define
class MajorCodeList:
    """
    Attributes:
        response_details (Union[Unset, str]):
        response_status (Union[Unset, int]):
        major_code_list (Union[Unset, List['MajorCode']]):
        api_deprecated_error (Union[Unset, str]):
    """

    response_details: Union[Unset, str] = UNSET
    response_status: Union[Unset, int] = UNSET
    major_code_list: Union[Unset, List["MajorCode"]] = UNSET
    api_deprecated_error: Union[Unset, str] = UNSET
    additional_properties: Dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        response_details = self.response_details

        response_status = self.response_status

        major_code_list: Union[Unset, List[Dict[str, Any]]] = UNSET
        if not isinstance(self.major_code_list, Unset):
            major_code_list = []
            for major_code_list_item_data in self.major_code_list:
                major_code_list_item = major_code_list_item_data.to_dict()
                major_code_list.append(major_code_list_item)

        api_deprecated_error = self.api_deprecated_error

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if response_details is not UNSET:
            field_dict["responseDetails"] = response_details
        if response_status is not UNSET:
            field_dict["responseStatus"] = response_status
        if major_code_list is not UNSET:
            field_dict["majorCodeList"] = major_code_list
        if api_deprecated_error is not UNSET:
            field_dict["apiDeprecatedError"] = api_deprecated_error

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        from ..models.major_code import MajorCode

        d = src_dict.copy()
        response_details = d.pop("responseDetails", UNSET)

        response_status = d.pop("responseStatus", UNSET)

        major_code_list = []
        _major_code_list = d.pop("majorCodeList", UNSET)
        for major_code_list_item_data in _major_code_list or []:
            major_code_list_item = MajorCode.from_dict(major_code_list_item_data)

            major_code_list.append(major_code_list_item)

        api_deprecated_error = d.pop("apiDeprecatedError", UNSET)

        major_code_list = cls(
            response_details=response_details,
            response_status=response_status,
            major_code_list=major_code_list,
            api_deprecated_error=api_deprecated_error,
        )

        major_code_list.additional_properties = d
        return major_code_list

    @property
    def additional_keys(self) -> List[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
