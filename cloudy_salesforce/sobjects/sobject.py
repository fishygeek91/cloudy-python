from typing import Type, TypeVar, TypedDict, get_type_hints, get_args, get_origin
from functools import partial
from cloudy_salesforce.client import SalesforceClient
from cloudy_salesforce.query import QueryProps, soql_query

T = TypeVar("T")


def parse_sobject_reponse(sobject: Type[T], response: dict) -> list[T]:
    assert response["records"]
    records = response["records"]

    records_to_return: list[T] = []
    object_types = get_type_hints(object)

    def parse_record(sobject: Type[T], record: dict) -> T:
        record_to_return: T = sobject()
        for key, value in record.items():
            if key == "attributes":
                continue
            if key not in object_types:
                raise ValueError(f"Field {key} not found in python object {object}")

            object_type = object_types[key]

            if hasattr(object_type, "__sf_meta__"):
                value = parse_record(object_type, value)
                setattr(record_to_return, key, value)
                continue

            if object_type is list:
                inner_type = get_inner_type(object_type)
                if not inner_type:
                    continue
                if hasattr(object_type, "__sf_meta__"):
                    value = parse_sobject_reponse(inner_type, value)
                    setattr(record_to_return, key, value)
                    continue

        return record_to_return

    for record in records:
        record_to_return = parse_record(sobject, record)
        records_to_return.append(record_to_return)

    return records_to_return


def get_inner_type(typ):
    print(typ)
    origin = get_origin(typ)
    print(origin)
    if origin is list:
        args = get_args(typ)
        return args[0] if args else None
    return None


def sobject(api_name: str | None = None):
    def decorator(cls):
        api_name_to_set = api_name or cls.__name__
        cls.__sf_meta__ = {"api_name": api_name_to_set}
        return cls

    return decorator


class SObjects:
    def __init__(self, sf_client: SalesforceClient | None = None):
        if sf_client is None:
            sf_client = SalesforceClient.get_default_instance()
        self.sf_client = sf_client

    # Should go in the sobject folder
    def describe_sobject(self, sobject) -> dict:
        """
        Queries the Salesforce API to describe the specified sObject.

        Parameters:
        sf_client (Salesforce): The Salesforce client object.
        sobject (str): The API name of the sObject to describe.

        Returns:
        dict: The description of the specified sObject.
        """
        # Build the URL for the describe API endpoint
        url = f"/services/data/v61.0/sobjects/{sobject}/describe/"

        # Make the API call to get the sObject description
        response = self.sf_client.request("GET", url)
        if type(response) is dict:
            return response
        else:
            raise Exception(f"Error with describe_sobject: {response}")

    def get_object_fields(self, objects: list[str]) -> dict[str, list[dict]]:
        object_field_dict = {}
        for ob in objects:
            response = self.describe_sobject(ob)
            fields = response["fields"]
            object_field_dict[ob] = fields
        return object_field_dict

    def query_sobject(
        self,
        object_type: Type[T],
        query_string: str,
        client: SalesforceClient | None = None,
    ) -> list[T]:
        return_fctn = partial(parse_sobject_reponse, object_type)
        if client is None:
            client = SalesforceClient.get_default_instance()

        @soql_query(endpoint="queryAll", return_function=return_fctn)
        def query(query_string: str) -> QueryProps:
            return {
                "client": client,
                "soql_query": query_string,
            }

        return query(query_string)
