from enum import Enum
from functools import partial, wraps
import json
import logging
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    List,
    Optional,
    Tuple,
    TypeVar,
    TypedDict,
    Literal,
)

from requests import HTTPError

from .return_functions import records_and_response, response_json_only

# Assuming SalesforceClient is imported correctly
from ..client import SalesforceClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


T = TypeVar("T")


class CRUDProps(TypedDict):
    client: SalesforceClient
    object_type: str
    records: List[dict]
    all_or_none: bool
    batch_size: int


class InsertProps(CRUDProps):
    pass


class UpdateProps(CRUDProps):
    pass


class UpsertProps(CRUDProps):
    external_id_field: Optional[str]  # defaults to "Id"


class DeleteProps(CRUDProps):
    pass


CRUDLiteral = Literal["insert", "upsert", "update", "delete"]

COLLECTION_METHODS = {
    "insert": "POST",
    "update": "PATCH",
    "upsert": "PATCH",
    "delete": "DELETE",
}


def collections(
    operation: CRUDLiteral,
    return_function: Callable[
        [List[Dict[str, Any]], List[Dict[str, Any]]], T
    ] = response_json_only,
) -> Callable[[Callable[..., CRUDProps]], Callable[..., T]]:
    def decorator(func: Callable[..., CRUDProps]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            props: CRUDProps = func(*args, **kwargs)
            client = props["client"]
            records_to_process = props["records"]

            crud_function = partial(client.request, COLLECTION_METHODS[operation])

            results: List[Dict[str, Any]] = []

            # batch records
            for records in batch_records(records_to_process, props["batch_size"]):
                url, body, params = build_payload(
                    operation, {**props, "records": records}
                )

                dml_response = crud_function(url=url, body=body, params=params)

                if not isinstance(dml_response, list):
                    raise ValueError(
                        f"Expected a list of responses, but received: {dml_response}"
                    )
                results.extend(dml_response)

            return return_function(records_to_process, results)

        return wrapper

    return decorator


# PyCharm is working on support for argument annotations using @wraps
# https://youtrack.jetbrains.com/issue/PY-62760/Support-functools.wraps
@collections("insert", return_function=records_and_response)
def insert(
    object_type: str,
    records: List[dict],
    all_or_none: bool = True,
    batch_size: int = 200,
    client: SalesforceClient | None = None,
) -> InsertProps:
    if client is None:
        client = SalesforceClient()
    return {
        "client": client,
        "object_type": object_type,
        "records": records,
        "all_or_none": all_or_none,
        "batch_size": batch_size,
    }


# PyCharm is working on support for argument annotations using @wraps
# https://youtrack.jetbrains.com/issue/PY-62760/Support-functools.wraps
@collections("update", return_function=records_and_response)
def update(
    object_type: str,
    records: List[dict],
    all_or_none: bool = True,
    batch_size: int = 200,
    client: SalesforceClient | None = None,
) -> UpdateProps:
    if client is None:
        client = SalesforceClient()
    return {
        "client": client,
        "object_type": object_type,
        "records": records,
        "all_or_none": all_or_none,
        "batch_size": batch_size,
    }


# PyCharm is working on support for argument annotations using @wraps
# https://youtrack.jetbrains.com/issue/PY-62760/Support-functools.wraps
@collections("upsert", return_function=records_and_response)
def upsert(
    object_type: str,
    records: List[dict],
    external_id_field: str | None = None,
    all_or_none: bool = True,
    batch_size: int = 200,
    client: SalesforceClient | None = None,
) -> UpsertProps:
    if client is None:
        client = SalesforceClient()
    return {
        "client": client,
        "object_type": object_type,
        "records": records,
        "external_id_field": external_id_field,
        "all_or_none": all_or_none,
        "batch_size": batch_size,
    }


# PyCharm is working on support for argument annotations using @wraps
# https://youtrack.jetbrains.com/issue/PY-62760/Support-functools.wraps
@collections("delete", return_function=records_and_response)
def delete(
    object_type: str,
    records: List[dict],
    all_or_none: bool = True,
    batch_size: int = 200,
    client: SalesforceClient | None = None,
) -> DeleteProps:
    if client is None:
        client = SalesforceClient()
    return {
        "client": client,
        "object_type": object_type,
        "records": records,
        "all_or_none": all_or_none,
        "batch_size": batch_size,
    }


# a function that batches a list of records into chunks of size batch_size
def batch_records(records: List[dict], batch_size: int) -> List[List[dict]]:
    return [records[i : i + batch_size] for i in range(0, len(records), batch_size)]


def add_attributes(records: List[dict], object_type: str) -> List[dict]:
    for record in records:
        record["attributes"] = {"type": object_type}
    return records


# Case insensitive function to get id's from records
def get_id_list(records: List[dict]) -> List[str]:
    ids = []
    for record in records:
        if "Id" in record:
            ids.append(record["Id"])
        elif "id" in record:
            ids.append(record["id"])
        else:
            raise ValueError(f"Record does not contain an Id/id field: {record}")
    return ids


def build_payload(
    operation: CRUDLiteral,
    props: CRUDProps,
) -> Tuple[str, dict | None, dict | None]:
    records = props["records"]
    all_or_none = props["all_or_none"]

    if operation == "delete":
        params = {"ids": ",".join(get_id_list(records)), "allOrNone": all_or_none}
        return (
            f"/services/data/v61.0/composite/sobjects?ids={','.join(get_id_list(records))}&allOrNone={all_or_none}",
            None,
            params,
        )

    object_type = props["object_type"]
    body = {"allOrNone": all_or_none, "records": add_attributes(records, object_type)}

    if operation == "upsert":
        if "external_id_field" in props and props["external_id_field"] is not None:
            external_id = props["external_id_field"]
        else:
            external_id = "Id"
        return (
            f"/services/data/v61.0/composite/sobjects/{object_type}/{external_id}",
            body,
            None,
        )

    return f"/services/data/v60.0/composite/sobjects/", body, None
