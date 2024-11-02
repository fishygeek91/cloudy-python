import json
import os
from jinja2 import Environment, PackageLoader
from typing import List, TypedDict

from cloudy_salesforce.client import SalesforceClient
from cloudy_salesforce.client.auth import BaseAuthentication
from cloudy_salesforce.sobjects import SObject


class FieldDict(TypedDict):
    name: str
    type: str
    picklist: List[str] | None


class ObjectDict(TypedDict):
    class_name: str
    fields: List[FieldDict]


class SObjectGenerator:
    def __init__(
        self,
        authentication: BaseAuthentication,
        template_dir: str = "templates",
        template_name: str = "sobject.jinja2",
        output_dir: str = "sobjects",
    ):
        self.sf_client = SalesforceClient(auth_strategy=authentication)

        env = Environment(
            loader=PackageLoader("cloudy_salesforce.generator", template_dir)
        )
        self.template = env.get_template(template_name)
        self.output_dir = output_dir

        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    def get_objects(
        self,
        object_names: List[str] | str | None = None,
        path: str = ".cloudy_config",
    ) -> List[ObjectDict]:
        # First step is to get the object names
        # 1. config json file
        if not object_names:
            try:
                with open(path, "r") as file:
                    object_names = json.load(file)["sobjects"]
                    assert object_names
            except FileNotFoundError:
                raise FileNotFoundError(
                    "No object names provided and no config file found."
                )

        # 2. passed in as a single string
        if isinstance(object_names, str):
            object_names = [object_names]
        # 3. passed in as a list of strings
        sobject_client = SObject(sf_client=self.sf_client)

        object_field_dict = sobject_client.get_object_fields(object_names)
        gen_objects = []

        for ob, fields in object_field_dict.items():
            fields_dicts: List[FieldDict] = self.parse_sf_fields(fields)
            gen_objects.append(ObjectDict(class_name=ob, fields=fields_dicts))
        return gen_objects

    def generate_all(
        self,
        object_names: List[str] | str | None = None,
        path: str = "./.cloudy_config",
    ):
        objects = self.get_objects(object_names, path)
        class_names = [obj["class_name"] for obj in objects]
        for obj in objects:
            self.generate(obj["class_name"], obj["fields"])
        self.generate_init_file(class_names)

    def generate(self, sobject: str, fields: List[FieldDict]):
        prepared_fields = [(f["name"], f["type"]) for f in fields]
        picklist_fields = [(f["type"], f["picklist"]) for f in fields if f["picklist"]]

        generated_file = self.template.render(
            sobject=sobject,
            fields=prepared_fields,
            picklist_fields=picklist_fields,
        )

        absolute_path = os.path.join(self.output_dir, f"{sobject}.py")

        with open(absolute_path, "w") as file:
            file.write(generated_file)
        print(f"{absolute_path} generated.")

    def generate_init_file(self, class_names: List[str]):
        init_file_path = os.path.join(self.output_dir, "__init__.py")
        with open(init_file_path, "w") as file:
            for class_name in class_names:
                file.write(f"from .{class_name} import {class_name}\n")
        print(f"{init_file_path} generated.")

    # ------------------------------------------------#

    def parse_sf_fields(self, fields: List[dict]) -> List[FieldDict]:
        field_dict_list = []
        for field in fields:
            field_name = field["name"]
            field_type = parse_type(field_name, field["type"])
            picklist = (
                field.get("picklistValues") if field["type"] == "picklist" else None
            )
            if picklist:
                picklist = [item["value"] for item in picklist if item["active"]]
            field_dict_list.append(
                FieldDict(name=field_name, type=field_type, picklist=picklist)
            )
        return field_dict_list


def parse_type(field_name: str, field_type: str) -> str:
    if field_type == "picklist":
        # Remove all underscores, convert to uppercase, and append PICKLIST
        field_name = field_name.replace("_", "")
        field_name = f"{field_name.upper()}PICKLIST"
        return field_name
    return salesforce_to_python_type_map[field_type]


salesforce_to_python_type_map = {
    "reference": "str",  # References are usually strings in Salesforce
    "string": "str",  # String type
    "phone": "str",  # Phone numbers are represented as strings
    "id": "str",  # Salesforce IDs are strings
    "email": "str",  # Emails are strings
    "percent": "float",  # Percentages are typically floats
    "boolean": "bool",  # Boolean type
    "double": "float",  # Double precision numbers are floats in Python
    "url": "str",  # URLs are strings
    "textarea": "str",  # Textarea fields are strings
    "date": "str",  # Date type, requires import from datetime module
    "int": "int",  # Integer type
    "datetime": "str",  # Datetime type, requires import from datetime module
    "address": "str",  # Addresses can be represented as strings or custom objects
    "encryptedstring": "str",  # Encrypted strings are still strings
    "currency": "float",  # Currency values are floats
    "multipicklist": "str",  # Multipicklist values are strings
    "combobox": "str",  # Combobox values are strings
}
