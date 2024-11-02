import json
from .generator import SObjectGenerator
from cloudy_salesforce.client import UsernamePasswordAuthentication

import argparse
import sys
import os
from dotenv import load_dotenv, find_dotenv


def generate(args):
    """
    Handle the 'generate' command.
    """
    print(find_dotenv(raise_error_if_not_found=True))
    load_dotenv(dotenv_path=find_dotenv(raise_error_if_not_found=True))
    print("Environment Variables:", os.environ)
    sobjects = args.sobjects
    alias_name = args.alias
    print(f"Generating code with option1={sobjects} and option2={alias_name}")
    # Implement your code generation logic here
    # load json file .cloudy_config to get auth details and object names

    # load auth details
    with open(".cloudy_config", "r") as file:
        config = json.load(file)
        assert config
        assert config["auth"]

    auth_details = config["auth"]

    if alias_name == "default":
        alias_name = auth_details["default_alias"]

    alias = auth_details["aliases"][alias_name]

    def get_auth(alias):

        if alias["type"] == "basic":
            username = os.getenv("PROD_USERNAME")
            password = os.getenv(alias["credentials"]["password"])
            security_token = os.environ.get(alias["credentials"]["security_token"])
            print(username)
            print(alias["credentials"]["username"])
            print(password)
            print(security_token)
            return "yo"
            # need to add params like sandbox and url
            # return UsernamePasswordAuthentication(
            #     username=username, password=password, security_token=security_token
            # )

        raise Exception(f"Auth type not supported yet: {alias['type']}")

    auth = get_auth(alias)

    # generator = SObjectGenerator(auth)
    # generator.generate_all(sobjects)


def main():
    path_to_env = os.path.join(os.path.dirname(__file__), ".env")
    print("hello")
    print(os.path.dirname(__file__))
    load_dotenv(path_to_env)
    parser = argparse.ArgumentParser(
        description="Your Package CLI", usage="%(prog)s [command] [options]"
    )

    subparsers = parser.add_subparsers(title="Commands", dest="command")
    subparsers.required = True  # Make the command required

    # 'generate' command parser
    generate_parser = subparsers.add_parser(
        "generate", help="Generate code based on provided options."
    )
    generate_parser.add_argument(
        "--sobjects",
        "-sob",
        default=None,
        help="(Optional) Enter a list of sobject names or a single sobject name",
    )
    generate_parser.add_argument(
        "--alias", "-a", default="default", help="Description for option2."
    )
    generate_parser.set_defaults(func=generate)

    # Parse the arguments
    args = parser.parse_args()

    # Call the appropriate function based on the command
    args.func(args)
