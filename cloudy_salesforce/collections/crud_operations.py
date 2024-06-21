from enum import Enum
from functools import partial, wraps
import json
import logging
from typing import Any, Callable, Dict, List, Tuple, TypeVar, TypedDict, Literal, assert_never

from requests import HTTPError

from cloudy_salesforce.client.auth import UsernamePasswordAuthentication

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
    external_id_field: str

class DeleteProps(CRUDProps):
    pass

T = TypeVar('T', bound=CRUDProps)



def collections_request(method: str, url: str, client: SalesforceClient, body: dict | None):
    session = client.get_session()
    print(session.headers)
    print(body)
    instance_url = client.get_instance_url()
    request_url = f"{instance_url}{url}"
    print(request_url)
    try:
        response = session.request(method, request_url, json=body)
        print(response.text)
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




def collections(operation: CRUDLiteral) -> Callable[[Callable[..., T]], Callable[..., List[Dict[str, Any]]]]:
    def decorator(func: Callable[..., T]) -> Callable[..., List[Dict[str, Any]]]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> List[Dict[str, Any]]:
            result: T = func(*args, **kwargs)
            crud_function = CRUDOperation[operation.upper()]
            client = result["client"]

            # batch records
            for records in batch_records(result['records'], result['batch_size']):
                url, body = build_payload(operation, records, result['object_type'], result['all_or_none'])
                response = crud_function.value(client=client, url=url, body=body)
                print(response)

                

            
                
            
            
            
            # Placeholder for actual Salesforce logic
            return [{"status": "success"}]
        
        return wrapper
    return decorator


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

# a function that batches a list of records into chunks of size batch_size
def batch_records(records: List[dict], batch_size: int) -> List[List[dict]]:
    return [records[i : i + batch_size] for i in range(0, len(records), batch_size)]

def add_attributes(records: List[dict], object_type: str) -> List[dict]:
    print(type(records))
    for record in records:
        record['attributes'] = {'type': object_type}
    return records

# Case insensitive function to get id's from records
def get_id_list(records: List[dict]) -> List[str]:
    ids = []
    for record in records:
        if 'Id' in record:
            ids.append(record['Id'])
        elif 'id' in record:
            ids.append(record['id'])
        else:
            raise ValueError(f"Record does not contain an Id/id field: {record}")
    return ids

def build_payload(operation: CRUDLiteral, records: List[dict], object_type: str, all_or_none: bool, external_id: str | None = None) -> Tuple[str, dict | None]:
    if operation == 'delete':
        return f"/services/data/v61.0/composite/sobjects?ids={','.join(get_id_list(records))}&allOrNone={all_or_none}", None
    
    print(type(records))

    body = {
        'allOrNone': all_or_none,
        'records': add_attributes(records, object_type)
    }

    if operation == 'upsert':
        if external_id is None:
            external_id = 'Id'
        return f"/services/data/v61.0/composite/sobjects/{object_type}/{external_id}", body
    
    return f"/services/data/v60.0/composite/sobjects/", body



