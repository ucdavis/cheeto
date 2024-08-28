from http import HTTPStatus
from typing import Any, Dict, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    cn: Union[Unset, str] = UNSET,
    display_name: Union[Unset, str] = UNSET,
    given_name: Union[Unset, str] = UNSET,
    sn: Union[Unset, str] = UNSET,
    key: str,
    v: Union[Unset, str] = UNSET,
) -> Dict[str, Any]:
    params: Dict[str, Any] = {}

    params["cn"] = cn

    params["displayName"] = display_name

    params["givenName"] = given_name

    params["sn"] = sn

    params["key"] = key

    params["v"] = v

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: Dict[str, Any] = {
        "method": "get",
        "url": "/directory/search",
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
    cn: Union[Unset, str] = UNSET,
    display_name: Union[Unset, str] = UNSET,
    given_name: Union[Unset, str] = UNSET,
    sn: Union[Unset, str] = UNSET,
    key: str,
    v: Union[Unset, str] = UNSET,
) -> Response[Any]:
    """Online Direcotry Search

     These APIs provide a basic search API for data that is published from the Online Directory system.
    This API will return a person's primary listing and the first device/address results for that
    listing.

    Args:
        cn (Union[Unset, str]):
        display_name (Union[Unset, str]):
        given_name (Union[Unset, str]):
        sn (Union[Unset, str]):
        key (str):
        v (Union[Unset, str]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any]
    """

    kwargs = _get_kwargs(
        cn=cn,
        display_name=display_name,
        given_name=given_name,
        sn=sn,
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
    cn: Union[Unset, str] = UNSET,
    display_name: Union[Unset, str] = UNSET,
    given_name: Union[Unset, str] = UNSET,
    sn: Union[Unset, str] = UNSET,
    key: str,
    v: Union[Unset, str] = UNSET,
) -> Response[Any]:
    """Online Direcotry Search

     These APIs provide a basic search API for data that is published from the Online Directory system.
    This API will return a person's primary listing and the first device/address results for that
    listing.

    Args:
        cn (Union[Unset, str]):
        display_name (Union[Unset, str]):
        given_name (Union[Unset, str]):
        sn (Union[Unset, str]):
        key (str):
        v (Union[Unset, str]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any]
    """

    kwargs = _get_kwargs(
        cn=cn,
        display_name=display_name,
        given_name=given_name,
        sn=sn,
        key=key,
        v=v,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)
