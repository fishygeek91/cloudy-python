from typing import List, OrderedDict
from cloudy_salesforce.client import SalesforceClient


class SObject:
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
        url = f"/sobjects/{sobject}/describe/"

        # Make the API call to get the sObject description
        response = self.sf_client.request("GET", url)

        if type(response) is OrderedDict:
            return response
        else:
            raise Exception(f"Error: ")

    def get_object_fields(self, objects: List[str]) -> dict[str, List[dict]]:
        object_field_dict = {}
        for ob in objects:
            response = self.describe_sobject(ob)
            fields = response["fields"]
            object_field_dict[ob] = fields
        return object_field_dict
