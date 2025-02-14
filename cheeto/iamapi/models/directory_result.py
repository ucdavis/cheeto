from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="DirectoryResult")


@_attrs_define
class DirectoryResult:
    """
    Attributes:
        check_sum (Union[Unset, str]):
        ucd_person_uuid (Union[Unset, str]):
        mail (Union[Unset, str]):
        cn (Union[Unset, str]):
        display_name (Union[Unset, str]):
        given_name (Union[Unset, str]):
        sn (Union[Unset, str]):
        edu_person_nickname (Union[Unset, str]):
        telephone_number (Union[Unset, str]):
        pager (Union[Unset, str]):
        mobile (Union[Unset, str]):
        postal_address (Union[Unset, str]):
        street (Union[Unset, str]):
        l (Union[Unset, str]):
        c (Union[Unset, str]):
        postal_code (Union[Unset, str]):
        st (Union[Unset, str]):
        labeled_uri (Union[Unset, str]):
        ou (Union[Unset, str]):
        title (Union[Unset, str]):
    """

    check_sum: Union[Unset, str] = UNSET
    ucd_person_uuid: Union[Unset, str] = UNSET
    mail: Union[Unset, str] = UNSET
    cn: Union[Unset, str] = UNSET
    display_name: Union[Unset, str] = UNSET
    given_name: Union[Unset, str] = UNSET
    sn: Union[Unset, str] = UNSET
    edu_person_nickname: Union[Unset, str] = UNSET
    telephone_number: Union[Unset, str] = UNSET
    pager: Union[Unset, str] = UNSET
    mobile: Union[Unset, str] = UNSET
    postal_address: Union[Unset, str] = UNSET
    street: Union[Unset, str] = UNSET
    l: Union[Unset, str] = UNSET
    c: Union[Unset, str] = UNSET
    postal_code: Union[Unset, str] = UNSET
    st: Union[Unset, str] = UNSET
    labeled_uri: Union[Unset, str] = UNSET
    ou: Union[Unset, str] = UNSET
    title: Union[Unset, str] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        check_sum = self.check_sum

        ucd_person_uuid = self.ucd_person_uuid

        mail = self.mail

        cn = self.cn

        display_name = self.display_name

        given_name = self.given_name

        sn = self.sn

        edu_person_nickname = self.edu_person_nickname

        telephone_number = self.telephone_number

        pager = self.pager

        mobile = self.mobile

        postal_address = self.postal_address

        street = self.street

        l = self.l

        c = self.c

        postal_code = self.postal_code

        st = self.st

        labeled_uri = self.labeled_uri

        ou = self.ou

        title = self.title

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if check_sum is not UNSET:
            field_dict["checkSum"] = check_sum
        if ucd_person_uuid is not UNSET:
            field_dict["ucdPersonUUID"] = ucd_person_uuid
        if mail is not UNSET:
            field_dict["mail"] = mail
        if cn is not UNSET:
            field_dict["cn"] = cn
        if display_name is not UNSET:
            field_dict["displayName"] = display_name
        if given_name is not UNSET:
            field_dict["givenName"] = given_name
        if sn is not UNSET:
            field_dict["sn"] = sn
        if edu_person_nickname is not UNSET:
            field_dict["eduPersonNickname"] = edu_person_nickname
        if telephone_number is not UNSET:
            field_dict["telephoneNumber"] = telephone_number
        if pager is not UNSET:
            field_dict["pager"] = pager
        if mobile is not UNSET:
            field_dict["mobile"] = mobile
        if postal_address is not UNSET:
            field_dict["postalAddress"] = postal_address
        if street is not UNSET:
            field_dict["street"] = street
        if l is not UNSET:
            field_dict["l"] = l
        if c is not UNSET:
            field_dict["c"] = c
        if postal_code is not UNSET:
            field_dict["postalCode"] = postal_code
        if st is not UNSET:
            field_dict["st"] = st
        if labeled_uri is not UNSET:
            field_dict["labeledURI"] = labeled_uri
        if ou is not UNSET:
            field_dict["ou"] = ou
        if title is not UNSET:
            field_dict["title"] = title

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: dict[str, Any]) -> T:
        d = src_dict.copy()
        check_sum = d.pop("checkSum", UNSET)

        ucd_person_uuid = d.pop("ucdPersonUUID", UNSET)

        mail = d.pop("mail", UNSET)

        cn = d.pop("cn", UNSET)

        display_name = d.pop("displayName", UNSET)

        given_name = d.pop("givenName", UNSET)

        sn = d.pop("sn", UNSET)

        edu_person_nickname = d.pop("eduPersonNickname", UNSET)

        telephone_number = d.pop("telephoneNumber", UNSET)

        pager = d.pop("pager", UNSET)

        mobile = d.pop("mobile", UNSET)

        postal_address = d.pop("postalAddress", UNSET)

        street = d.pop("street", UNSET)

        l = d.pop("l", UNSET)

        c = d.pop("c", UNSET)

        postal_code = d.pop("postalCode", UNSET)

        st = d.pop("st", UNSET)

        labeled_uri = d.pop("labeledURI", UNSET)

        ou = d.pop("ou", UNSET)

        title = d.pop("title", UNSET)

        directory_result = cls(
            check_sum=check_sum,
            ucd_person_uuid=ucd_person_uuid,
            mail=mail,
            cn=cn,
            display_name=display_name,
            given_name=given_name,
            sn=sn,
            edu_person_nickname=edu_person_nickname,
            telephone_number=telephone_number,
            pager=pager,
            mobile=mobile,
            postal_address=postal_address,
            street=street,
            l=l,
            c=c,
            postal_code=postal_code,
            st=st,
            labeled_uri=labeled_uri,
            ou=ou,
            title=title,
        )

        directory_result.additional_properties = d
        return directory_result

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
