from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    ret_type: Union[Unset, str] = UNSET,
    iam_id: Union[Unset, str] = UNSET,
    level_code: Union[Unset, str] = UNSET,
    level_name: Union[Unset, str] = UNSET,
    class_code: Union[Unset, str] = UNSET,
    class_name: Union[Unset, str] = UNSET,
    college_code: Union[Unset, str] = UNSET,
    college_name: Union[Unset, str] = UNSET,
    assoc_rank: Union[Unset, str] = UNSET,
    major_code: Union[Unset, str] = UNSET,
    major_name: Union[Unset, str] = UNSET,
    ferpa_code: Union[Unset, str] = UNSET,
    key: Union[Unset, str] = UNSET,
    v: Union[Unset, str] = UNSET,
) -> dict[str, Any]:
    params: dict[str, Any] = {}

    params["retType"] = ret_type

    params["iamId"] = iam_id

    params["levelCode"] = level_code

    params["levelName"] = level_name

    params["classCode"] = class_code

    params["className"] = class_name

    params["collegeCode"] = college_code

    params["collegeName"] = college_name

    params["assocRank"] = assoc_rank

    params["majorCode"] = major_code

    params["majorName"] = major_name

    params["ferpaCode"] = ferpa_code

    params["key"] = key

    params["v"] = v

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/iam/associations/sis/search",
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
    level_code: Union[Unset, str] = UNSET,
    level_name: Union[Unset, str] = UNSET,
    class_code: Union[Unset, str] = UNSET,
    class_name: Union[Unset, str] = UNSET,
    college_code: Union[Unset, str] = UNSET,
    college_name: Union[Unset, str] = UNSET,
    assoc_rank: Union[Unset, str] = UNSET,
    major_code: Union[Unset, str] = UNSET,
    major_name: Union[Unset, str] = UNSET,
    ferpa_code: Union[Unset, str] = UNSET,
    key: Union[Unset, str] = UNSET,
    v: Union[Unset, str] = UNSET,
) -> Response[Any]:
    """IAM Student Associations Information search

     Student Associations informaion search

    Args:
        ret_type (Union[Unset, str]):
        iam_id (Union[Unset, str]):
        level_code (Union[Unset, str]):
        level_name (Union[Unset, str]):
        class_code (Union[Unset, str]):
        class_name (Union[Unset, str]):
        college_code (Union[Unset, str]):
        college_name (Union[Unset, str]):
        assoc_rank (Union[Unset, str]):
        major_code (Union[Unset, str]):
        major_name (Union[Unset, str]):
        ferpa_code (Union[Unset, str]):
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
        level_code=level_code,
        level_name=level_name,
        class_code=class_code,
        class_name=class_name,
        college_code=college_code,
        college_name=college_name,
        assoc_rank=assoc_rank,
        major_code=major_code,
        major_name=major_name,
        ferpa_code=ferpa_code,
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
    level_code: Union[Unset, str] = UNSET,
    level_name: Union[Unset, str] = UNSET,
    class_code: Union[Unset, str] = UNSET,
    class_name: Union[Unset, str] = UNSET,
    college_code: Union[Unset, str] = UNSET,
    college_name: Union[Unset, str] = UNSET,
    assoc_rank: Union[Unset, str] = UNSET,
    major_code: Union[Unset, str] = UNSET,
    major_name: Union[Unset, str] = UNSET,
    ferpa_code: Union[Unset, str] = UNSET,
    key: Union[Unset, str] = UNSET,
    v: Union[Unset, str] = UNSET,
) -> Response[Any]:
    """IAM Student Associations Information search

     Student Associations informaion search

    Args:
        ret_type (Union[Unset, str]):
        iam_id (Union[Unset, str]):
        level_code (Union[Unset, str]):
        level_name (Union[Unset, str]):
        class_code (Union[Unset, str]):
        class_name (Union[Unset, str]):
        college_code (Union[Unset, str]):
        college_name (Union[Unset, str]):
        assoc_rank (Union[Unset, str]):
        major_code (Union[Unset, str]):
        major_name (Union[Unset, str]):
        ferpa_code (Union[Unset, str]):
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
        level_code=level_code,
        level_name=level_name,
        class_code=class_code,
        class_name=class_name,
        college_code=college_code,
        college_name=college_name,
        assoc_rank=assoc_rank,
        major_code=major_code,
        major_name=major_name,
        ferpa_code=ferpa_code,
        key=key,
        v=v,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)
