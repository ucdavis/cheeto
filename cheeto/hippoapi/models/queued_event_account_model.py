from typing import Any, TypeVar, Union, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="QueuedEventAccountModel")


@_attrs_define
class QueuedEventAccountModel:
    """
    Attributes:
        kerberos (str):
        name (str):
        email (str):
        iam (str):
        mothra (str):
        key (Union[None, Unset, str]):
        access_types (Union[None, Unset, list[str]]):
    """

    kerberos: str
    name: str
    email: str
    iam: str
    mothra: str
    key: Union[None, Unset, str] = UNSET
    access_types: Union[None, Unset, list[str]] = UNSET

    def to_dict(self) -> dict[str, Any]:
        kerberos = self.kerberos

        name = self.name

        email = self.email

        iam = self.iam

        mothra = self.mothra

        key: Union[None, Unset, str]
        if isinstance(self.key, Unset):
            key = UNSET
        else:
            key = self.key

        access_types: Union[None, Unset, list[str]]
        if isinstance(self.access_types, Unset):
            access_types = UNSET
        elif isinstance(self.access_types, list):
            access_types = self.access_types

        else:
            access_types = self.access_types

        field_dict: dict[str, Any] = {}
        field_dict.update(
            {
                "kerberos": kerberos,
                "name": name,
                "email": email,
                "iam": iam,
                "mothra": mothra,
            }
        )
        if key is not UNSET:
            field_dict["key"] = key
        if access_types is not UNSET:
            field_dict["accessTypes"] = access_types

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: dict[str, Any]) -> T:
        d = src_dict.copy()
        kerberos = d.pop("kerberos")

        name = d.pop("name")

        email = d.pop("email")

        iam = d.pop("iam")

        mothra = d.pop("mothra")

        def _parse_key(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        key = _parse_key(d.pop("key", UNSET))

        def _parse_access_types(data: object) -> Union[None, Unset, list[str]]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, list):
                    raise TypeError()
                access_types_type_0 = cast(list[str], data)

                return access_types_type_0
            except:  # noqa: E722
                pass
            return cast(Union[None, Unset, list[str]], data)

        access_types = _parse_access_types(d.pop("accessTypes", UNSET))

        queued_event_account_model = cls(
            kerberos=kerberos,
            name=name,
            email=email,
            iam=iam,
            mothra=mothra,
            key=key,
            access_types=access_types,
        )

        return queued_event_account_model
