from http import HTTPStatus
from typing import Any, Dict, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    iam_id: Union[Unset, str] = UNSET,
    ret_type: Union[Unset, str] = UNSET,
    user_id: Union[Unset, str] = UNSET,
    uu_id: Union[Unset, str] = UNSET,
    key: Union[Unset, str] = UNSET,
    v: Union[Unset, str] = UNSET,
) -> Dict[str, Any]:
    params: Dict[str, Any] = {}

    params["iamId"] = iam_id

    params["retType"] = ret_type

    params["userId"] = user_id

    params["uuId"] = uu_id

    params["key"] = key

    params["v"] = v

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: Dict[str, Any] = {
        "method": "get",
        "url": "/iam/people/prihsadacct/search",
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
    iam_id: Union[Unset, str] = UNSET,
    ret_type: Union[Unset, str] = UNSET,
    user_id: Union[Unset, str] = UNSET,
    uu_id: Union[Unset, str] = UNSET,
    key: Union[Unset, str] = UNSET,
    v: Union[Unset, str] = UNSET,
) -> Response[Any]:
    """IAM HS Primary Kerberos Account Info.Search

     HS Primary Kerberos Account Information by search criteria

    Args:
        iam_id (Union[Unset, str]):
        ret_type (Union[Unset, str]):
        user_id (Union[Unset, str]):
        uu_id (Union[Unset, str]):
        key (Union[Unset, str]):
        v (Union[Unset, str]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any]
    """

    kwargs = _get_kwargs(
        iam_id=iam_id,
        ret_type=ret_type,
        user_id=user_id,
        uu_id=uu_id,
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
    iam_id: Union[Unset, str] = UNSET,
    ret_type: Union[Unset, str] = UNSET,
    user_id: Union[Unset, str] = UNSET,
    uu_id: Union[Unset, str] = UNSET,
    key: Union[Unset, str] = UNSET,
    v: Union[Unset, str] = UNSET,
) -> Response[Any]:
    """IAM HS Primary Kerberos Account Info.Search

     HS Primary Kerberos Account Information by search criteria

    Args:
        iam_id (Union[Unset, str]):
        ret_type (Union[Unset, str]):
        user_id (Union[Unset, str]):
        uu_id (Union[Unset, str]):
        key (Union[Unset, str]):
        v (Union[Unset, str]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any]
    """

    kwargs = _get_kwargs(
        iam_id=iam_id,
        ret_type=ret_type,
        user_id=user_id,
        uu_id=uu_id,
        key=key,
        v=v,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)
