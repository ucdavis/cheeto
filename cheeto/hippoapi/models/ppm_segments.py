from typing import Any, Dict, Type, TypeVar, Union, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="PpmSegments")


@_attrs_define
class PpmSegments:
    """
    Attributes:
        project (Union[Unset, str]):
        task (Union[Unset, str]):
        organization (Union[Unset, str]):
        expenditure_type (Union[Unset, str]):
        award (Union[None, Unset, str]):
        funding_source (Union[None, Unset, str]):
    """

    project: Union[Unset, str] = UNSET
    task: Union[Unset, str] = UNSET
    organization: Union[Unset, str] = UNSET
    expenditure_type: Union[Unset, str] = UNSET
    award: Union[None, Unset, str] = UNSET
    funding_source: Union[None, Unset, str] = UNSET

    def to_dict(self) -> Dict[str, Any]:
        project = self.project

        task = self.task

        organization = self.organization

        expenditure_type = self.expenditure_type

        award: Union[None, Unset, str]
        if isinstance(self.award, Unset):
            award = UNSET
        else:
            award = self.award

        funding_source: Union[None, Unset, str]
        if isinstance(self.funding_source, Unset):
            funding_source = UNSET
        else:
            funding_source = self.funding_source

        field_dict: Dict[str, Any] = {}
        field_dict.update({})
        if project is not UNSET:
            field_dict["project"] = project
        if task is not UNSET:
            field_dict["task"] = task
        if organization is not UNSET:
            field_dict["organization"] = organization
        if expenditure_type is not UNSET:
            field_dict["expenditureType"] = expenditure_type
        if award is not UNSET:
            field_dict["award"] = award
        if funding_source is not UNSET:
            field_dict["fundingSource"] = funding_source

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        project = d.pop("project", UNSET)

        task = d.pop("task", UNSET)

        organization = d.pop("organization", UNSET)

        expenditure_type = d.pop("expenditureType", UNSET)

        def _parse_award(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        award = _parse_award(d.pop("award", UNSET))

        def _parse_funding_source(data: object) -> Union[None, Unset, str]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(Union[None, Unset, str], data)

        funding_source = _parse_funding_source(d.pop("fundingSource", UNSET))

        ppm_segments = cls(
            project=project,
            task=task,
            organization=organization,
            expenditure_type=expenditure_type,
            award=award,
            funding_source=funding_source,
        )

        return ppm_segments
