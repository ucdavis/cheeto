from http import HTTPStatus
from typing import Any, Dict, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    ret_type: Union[Unset, str] = UNSET,
    iam_id: Union[Unset, str] = UNSET,
    dept_code: Union[Unset, str] = UNSET,
    is_ucdhs: Union[Unset, bool] = UNSET,
    bou_org_o_id: Union[Unset, str] = UNSET,
    admin_bou_org_o_id: Union[Unset, str] = UNSET,
    admin_dept_code: Union[Unset, str] = UNSET,
    admin_is_ucdhs: Union[Unset, bool] = UNSET,
    appt_bou_org_o_id: Union[Unset, str] = UNSET,
    appt_dept_code: Union[Unset, str] = UNSET,
    appt_is_ucdhs: Union[Unset, bool] = UNSET,
    assoc_rank: Union[Unset, str] = UNSET,
    title_code: Union[Unset, str] = UNSET,
    modify_date_after: Union[Unset, str] = UNSET,
    key: Union[Unset, str] = UNSET,
    v: Union[Unset, str] = UNSET,
) -> Dict[str, Any]:
    params: Dict[str, Any] = {}

    params["retType"] = ret_type

    params["iamId"] = iam_id

    params["deptCode"] = dept_code

    params["isUCDHS"] = is_ucdhs

    params["bouOrgOId"] = bou_org_o_id

    params["adminBOUOrgOId"] = admin_bou_org_o_id

    params["adminDeptCode"] = admin_dept_code

    params["adminIsUCDHS"] = admin_is_ucdhs

    params["apptBOUOrgOId"] = appt_bou_org_o_id

    params["apptDeptCode"] = appt_dept_code

    params["apptIsUCDHS"] = appt_is_ucdhs

    params["assocRank"] = assoc_rank

    params["titleCode"] = title_code

    params["modifyDateAfter"] = modify_date_after

    params["key"] = key

    params["v"] = v

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: Dict[str, Any] = {
        "method": "get",
        "url": "/iam/associations/pps/search",
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Any]:
    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Response[Any]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: Union[AuthenticatedClient, Client],
    ret_type: Union[Unset, str] = UNSET,
    iam_id: Union[Unset, str] = UNSET,
    dept_code: Union[Unset, str] = UNSET,
    is_ucdhs: Union[Unset, bool] = UNSET,
    bou_org_o_id: Union[Unset, str] = UNSET,
    admin_bou_org_o_id: Union[Unset, str] = UNSET,
    admin_dept_code: Union[Unset, str] = UNSET,
    admin_is_ucdhs: Union[Unset, bool] = UNSET,
    appt_bou_org_o_id: Union[Unset, str] = UNSET,
    appt_dept_code: Union[Unset, str] = UNSET,
    appt_is_ucdhs: Union[Unset, bool] = UNSET,
    assoc_rank: Union[Unset, str] = UNSET,
    title_code: Union[Unset, str] = UNSET,
    modify_date_after: Union[Unset, str] = UNSET,
    key: Union[Unset, str] = UNSET,
    v: Union[Unset, str] = UNSET,
) -> Response[Any]:
    """IAM PPS Associations Search

     Employee Associations information Search

    Args:
        ret_type (Union[Unset, str]):
        iam_id (Union[Unset, str]):
        dept_code (Union[Unset, str]):
        is_ucdhs (Union[Unset, bool]):
        bou_org_o_id (Union[Unset, str]):
        admin_bou_org_o_id (Union[Unset, str]):
        admin_dept_code (Union[Unset, str]):
        admin_is_ucdhs (Union[Unset, bool]):
        appt_bou_org_o_id (Union[Unset, str]):
        appt_dept_code (Union[Unset, str]):
        appt_is_ucdhs (Union[Unset, bool]):
        assoc_rank (Union[Unset, str]):
        title_code (Union[Unset, str]):
        modify_date_after (Union[Unset, str]):
        key (Union[Unset, str]):
        v (Union[Unset, str]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any]
    """

    kwargs = _get_kwargs(
        ret_type=ret_type,
        iam_id=iam_id,
        dept_code=dept_code,
        is_ucdhs=is_ucdhs,
        bou_org_o_id=bou_org_o_id,
        admin_bou_org_o_id=admin_bou_org_o_id,
        admin_dept_code=admin_dept_code,
        admin_is_ucdhs=admin_is_ucdhs,
        appt_bou_org_o_id=appt_bou_org_o_id,
        appt_dept_code=appt_dept_code,
        appt_is_ucdhs=appt_is_ucdhs,
        assoc_rank=assoc_rank,
        title_code=title_code,
        modify_date_after=modify_date_after,
        key=key,
        v=v,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


async def asyncio_detailed(
    *,
    client: Union[AuthenticatedClient, Client],
    ret_type: Union[Unset, str] = UNSET,
    iam_id: Union[Unset, str] = UNSET,
    dept_code: Union[Unset, str] = UNSET,
    is_ucdhs: Union[Unset, bool] = UNSET,
    bou_org_o_id: Union[Unset, str] = UNSET,
    admin_bou_org_o_id: Union[Unset, str] = UNSET,
    admin_dept_code: Union[Unset, str] = UNSET,
    admin_is_ucdhs: Union[Unset, bool] = UNSET,
    appt_bou_org_o_id: Union[Unset, str] = UNSET,
    appt_dept_code: Union[Unset, str] = UNSET,
    appt_is_ucdhs: Union[Unset, bool] = UNSET,
    assoc_rank: Union[Unset, str] = UNSET,
    title_code: Union[Unset, str] = UNSET,
    modify_date_after: Union[Unset, str] = UNSET,
    key: Union[Unset, str] = UNSET,
    v: Union[Unset, str] = UNSET,
) -> Response[Any]:
    """IAM PPS Associations Search

     Employee Associations information Search

    Args:
        ret_type (Union[Unset, str]):
        iam_id (Union[Unset, str]):
        dept_code (Union[Unset, str]):
        is_ucdhs (Union[Unset, bool]):
        bou_org_o_id (Union[Unset, str]):
        admin_bou_org_o_id (Union[Unset, str]):
        admin_dept_code (Union[Unset, str]):
        admin_is_ucdhs (Union[Unset, bool]):
        appt_bou_org_o_id (Union[Unset, str]):
        appt_dept_code (Union[Unset, str]):
        appt_is_ucdhs (Union[Unset, bool]):
        assoc_rank (Union[Unset, str]):
        title_code (Union[Unset, str]):
        modify_date_after (Union[Unset, str]):
        key (Union[Unset, str]):
        v (Union[Unset, str]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any]
    """

    kwargs = _get_kwargs(
        ret_type=ret_type,
        iam_id=iam_id,
        dept_code=dept_code,
        is_ucdhs=is_ucdhs,
        bou_org_o_id=bou_org_o_id,
        admin_bou_org_o_id=admin_bou_org_o_id,
        admin_dept_code=admin_dept_code,
        admin_is_ucdhs=admin_is_ucdhs,
        appt_bou_org_o_id=appt_bou_org_o_id,
        appt_dept_code=appt_dept_code,
        appt_is_ucdhs=appt_is_ucdhs,
        assoc_rank=assoc_rank,
        title_code=title_code,
        modify_date_after=modify_date_after,
        key=key,
        v=v,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)
