from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    o_first_name: Union[Unset, str] = UNSET,
    o_middle_name: Union[Unset, str] = UNSET,
    o_last_name: Union[Unset, str] = UNSET,
    d_first_name: Union[Unset, str] = UNSET,
    d_middle_name: Union[Unset, str] = UNSET,
    d_last_name: Union[Unset, str] = UNSET,
    external_id: Union[Unset, str] = UNSET,
    iam_id: Union[Unset, str] = UNSET,
    email: Union[Unset, str] = UNSET,
    key: Union[Unset, str] = UNSET,
    v: Union[Unset, str] = UNSET,
) -> dict[str, Any]:
    params: dict[str, Any] = {}

    params["oFirstName"] = o_first_name

    params["oMiddleName"] = o_middle_name

    params["oLastName"] = o_last_name

    params["dFirstName"] = d_first_name

    params["dMiddleName"] = d_middle_name

    params["dLastName"] = d_last_name

    params["externalId"] = external_id

    params["iamId"] = iam_id

    params["email"] = email

    params["key"] = key

    params["v"] = v

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/iam/people/ucdext/search",
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
    o_first_name: Union[Unset, str] = UNSET,
    o_middle_name: Union[Unset, str] = UNSET,
    o_last_name: Union[Unset, str] = UNSET,
    d_first_name: Union[Unset, str] = UNSET,
    d_middle_name: Union[Unset, str] = UNSET,
    d_last_name: Union[Unset, str] = UNSET,
    external_id: Union[Unset, str] = UNSET,
    iam_id: Union[Unset, str] = UNSET,
    email: Union[Unset, str] = UNSET,
    key: Union[Unset, str] = UNSET,
    v: Union[Unset, str] = UNSET,
) -> Response[Any]:
    """IAM UCDavis external people Search

     This is a process intense API call and will need special permissions setup to be used

    Args:
        o_first_name (Union[Unset, str]):
        o_middle_name (Union[Unset, str]):
        o_last_name (Union[Unset, str]):
        d_first_name (Union[Unset, str]):
        d_middle_name (Union[Unset, str]):
        d_last_name (Union[Unset, str]):
        external_id (Union[Unset, str]):
        iam_id (Union[Unset, str]):
        email (Union[Unset, str]):
        key (Union[Unset, str]):
        v (Union[Unset, str]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any]
    """

    kwargs = _get_kwargs(
        o_first_name=o_first_name,
        o_middle_name=o_middle_name,
        o_last_name=o_last_name,
        d_first_name=d_first_name,
        d_middle_name=d_middle_name,
        d_last_name=d_last_name,
        external_id=external_id,
        iam_id=iam_id,
        email=email,
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
    o_first_name: Union[Unset, str] = UNSET,
    o_middle_name: Union[Unset, str] = UNSET,
    o_last_name: Union[Unset, str] = UNSET,
    d_first_name: Union[Unset, str] = UNSET,
    d_middle_name: Union[Unset, str] = UNSET,
    d_last_name: Union[Unset, str] = UNSET,
    external_id: Union[Unset, str] = UNSET,
    iam_id: Union[Unset, str] = UNSET,
    email: Union[Unset, str] = UNSET,
    key: Union[Unset, str] = UNSET,
    v: Union[Unset, str] = UNSET,
) -> Response[Any]:
    """IAM UCDavis external people Search

     This is a process intense API call and will need special permissions setup to be used

    Args:
        o_first_name (Union[Unset, str]):
        o_middle_name (Union[Unset, str]):
        o_last_name (Union[Unset, str]):
        d_first_name (Union[Unset, str]):
        d_middle_name (Union[Unset, str]):
        d_last_name (Union[Unset, str]):
        external_id (Union[Unset, str]):
        iam_id (Union[Unset, str]):
        email (Union[Unset, str]):
        key (Union[Unset, str]):
        v (Union[Unset, str]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any]
    """

    kwargs = _get_kwargs(
        o_first_name=o_first_name,
        o_middle_name=o_middle_name,
        o_last_name=o_last_name,
        d_first_name=d_first_name,
        d_middle_name=d_middle_name,
        d_last_name=d_last_name,
        external_id=external_id,
        iam_id=iam_id,
        email=email,
        key=key,
        v=v,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)
