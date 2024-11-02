from .auth import BaseAuthentication
import logging
from requests.exceptions import HTTPError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from typing import Any, Dict, List, Type
import requests


class SalesforceClient:
    _default_instance = None

    def __init__(self, auth_strategy: BaseAuthentication):
        if not isinstance(auth_strategy, BaseAuthentication):
            raise TypeError(
                "auth_strategy must be an instance of a subclass of BaseAuthentication"
            )
        self.auth_strategy = auth_strategy

        # Set the default instance directly
        if self._default_instance is None:
            self.__class__._default_instance = self

    @classmethod
    def set_default_instance(cls, auth_strategy: BaseAuthentication):
        """
        Sets the default SalesforceClient instance.

        :param auth_strategy: An instance of a subclass of BaseAuthentication.
        """
        cls._default_instance = cls(auth_strategy)

    @classmethod
    def get_default_instance(cls):
        """
        Retrieves the default SalesforceClient instance.

        :return: The default SalesforceClient instance.
        :raises ValueError: If the default instance has not been set.
        """
        if cls._default_instance is None:
            raise ValueError("Default instance not set")
        return cls._default_instance

    def get_session(self) -> requests.Session:
        return self.auth_strategy.session

    def get_instance_url(self) -> str:
        return self.auth_strategy.instance_url

    def request(
        self,
        method: str,
        url: str,
        body: dict | None = None,
        params: dict | None = None,
    ) -> Dict[str, Any] | List[Dict[str, Any]]:
        request_url = f"{self.get_instance_url()}{url}"
        try:
            response = self.get_session().request(
                method, request_url, json=body, params=params
            )
            response.raise_for_status()
            return response.json()

        except HTTPError as http_err:
            logger.error(f"HTTP error occurred during query: {http_err}")
            raise
        except Exception as err:
            logger.error(f"Other error occurred during query: {err}")
            raise
