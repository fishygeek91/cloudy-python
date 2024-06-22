from enum import Enum
from functools import partial, wraps
import json
import logging
from typing import Any, Callable, Dict, List, Optional, Tuple, TypedDict, Literal

from requests import HTTPError

# Assuming SalesforceClient is imported correctly
from ..client import SalesforceClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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


def collections_request(
    method: str, url: str, client: SalesforceClient, body: dict | None
) -> List[Dict[str, Any]]:
    session = client.get_session()
    instance_url = client.get_instance_url()
    request_url = f"{instance_url}{url}"
    try:
        response = session.request(method, request_url, json=body)
        response.raise_for_status()
        return response.json()

    except HTTPError as http_err:
        logger.error(f"HTTP error occurred during query: {http_err}")
        raise
    except Exception as err:
        logger.error(f"Other error occurred during query: {err}")
        raise


CRUDLiteral = Literal["insert", "upsert", "update", "delete"]


class CRUDOperation(Enum):
    # /services/data/v61.0/composite/sobjects/
    INSERT = partial(collections_request, "POST")
    UPDATE = partial(collections_request, "PATCH")
    UPSERT = partial(collections_request, "PATCH")
    DELETE = partial(collections_request, "DELETE")


def collections(
    operation: CRUDLiteral,
) -> Callable[[Callable[..., CRUDProps]], Callable[..., List[Dict[str, Any]]]]:
    def decorator(
        func: Callable[..., CRUDProps]
    ) -> Callable[..., List[Dict[str, Any]]]:
        # PyCharm is working on support for argument annotations using @wraps
        # https://youtrack.jetbrains.com/issue/PY-62760/Support-functools.wraps
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> List[Dict[str, Any]]:
            result: CRUDProps = func(*args, **kwargs)
            crud_function = CRUDOperation[operation.upper()]
            client = result["client"]

            results: List[Dict[str, Any]] = []
            failures = {"count": 0, "results": []}
            successes = {"count": 0, "results": []}
            # batch records
            for records in batch_records(result["records"], result["batch_size"]):
                url, body = build_payload(
                    operation, records, result["object_type"], result["all_or_none"]
                )
                dml_response: List[Dict[str, Any]] = crud_function.value(
                    client=client, url=url, body=body
                )

                # zip the records with the response
                for record, response in zip(records, dml_response):
                    record_result = {"record": record, "response": response}
                    if not response["success"]:
                        failures["count"] += 1
                        failures["results"].append(record_result)
                    else:
                        successes["count"] += 1
                        successes["results"].append(record_result)

                    results.append(record_result)

            # log the results
            logger.info(f"Results for {operation} operation")
            logger.info(f"Successes: {successes['count']}")
            # logger.info(json.dumps(successes, indent=2))
            logger.info(f"Failures: {failures['count']}")
            logger.info(json.dumps(failures, indent=2))

            return results

        return wrapper

    return decorator


# PyCharm is working on support for argument annotations using @wraps
# https://youtrack.jetbrains.com/issue/PY-62760/Support-functools.wraps
@collections("insert")
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
@collections("update")
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
@collections("upsert")
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
@collections("delete")
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
    records: List[dict],
    object_type: str,
    all_or_none: bool,
    external_id: str | None = None,
) -> Tuple[str, dict | None]:
    if operation == "delete":
        return (
            f"/services/data/v61.0/composite/sobjects?ids={','.join(get_id_list(records))}&allOrNone={all_or_none}",
            None,
        )

    body = {"allOrNone": all_or_none, "records": add_attributes(records, object_type)}

    if operation == "upsert":
        if external_id is None:
            external_id = "Id"
        return (
            f"/services/data/v61.0/composite/sobjects/{object_type}/{external_id}",
            body,
        )

    return f"/services/data/v60.0/composite/sobjects/", body
