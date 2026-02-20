from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="PpmSegments")


@_attrs_define
class PpmSegments:
    """
    Attributes:
        project (str | Unset):
        task (str | Unset):
        organization (str | Unset):
        expenditure_type (str | Unset):
        award (None | str | Unset):
        funding_source (None | str | Unset):
    """

    project: str | Unset = UNSET
    task: str | Unset = UNSET
    organization: str | Unset = UNSET
    expenditure_type: str | Unset = UNSET
    award: None | str | Unset = UNSET
    funding_source: None | str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        project = self.project

        task = self.task

        organization = self.organization

        expenditure_type = self.expenditure_type

        award: None | str | Unset
        if isinstance(self.award, Unset):
            award = UNSET
        else:
            award = self.award

        funding_source: None | str | Unset
        if isinstance(self.funding_source, Unset):
            funding_source = UNSET
        else:
            funding_source = self.funding_source

        field_dict: dict[str, Any] = {}

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
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        project = d.pop("project", UNSET)

        task = d.pop("task", UNSET)

        organization = d.pop("organization", UNSET)

        expenditure_type = d.pop("expenditureType", UNSET)

        def _parse_award(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        award = _parse_award(d.pop("award", UNSET))

        def _parse_funding_source(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

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
