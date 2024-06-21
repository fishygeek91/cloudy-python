from abc import ABC, abstractmethod
import requests

class BaseAuthentication(ABC):
    def __init__(self, session: requests.Session, instance_url: str):
        self.session = session
        self.instance_url = instance_url

    @abstractmethod
    def authenticate(self) -> tuple[requests.Session, str]:
        pass

    @staticmethod
    def get_headers(access_token) -> dict[str, str]:
        return {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }


import logging
from requests.exceptions import HTTPError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class UsernamePasswordAuthentication(BaseAuthentication):
    def __init__(self, username, password, security_token):
        
        self.username = username
        self.password = password
        self.security_token = security_token
        session, instance_url = self.authenticate()
        super().__init__(session, instance_url)

    def authenticate(self):
        session = requests.Session()
        auth_url = "https://login.salesforce.com/services/Soap/u/52.0"
        headers = {
            'Content-Type': 'text/xml',
            'SOAPAction': 'login'
        }
        soap_body = f"""
        <env:Envelope xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:env="http://schemas.xmlsoap.org/soap/envelope/">
            <env:Body>
                <n1:login xmlns:n1="urn:partner.soap.sforce.com">
                    <n1:username>{self.username}</n1:username>
                    <n1:password>{self.password}{self.security_token}</n1:password>
                </n1:login>
            </env:Body>
        </env:Envelope>
        """
        try:
            response = session.post(auth_url, headers=headers, data=soap_body)
            response.raise_for_status()
            response_content = response.content.decode('utf-8')
            if "faultstring" in response_content:
                raise Exception(f"SOAP Fault: {response_content}")
            access_token = self._extract_access_token(response_content)
            instance_url = self._extract_instance_url(response_content)
            logger.info("Authentication successful")
            session.headers.update(self.get_headers(access_token))
            return session, instance_url
        except HTTPError as http_err:
            logger.error(f"HTTP error occurred: {http_err}")
            raise
        except Exception as err:
            logger.error(f"Other error occurred: {err}")
            raise

    def _extract_access_token(self, response_content):
        start_tag = "<sessionId>"
        end_tag = "</sessionId>"
        start_index = response_content.find(start_tag) + len(start_tag)
        end_index = response_content.find(end_tag)
        return response_content[start_index:end_index]

    def _extract_instance_url(self, response_content):
        start_tag = "<serverUrl>"
        end_tag = "</serverUrl>"
        start_index = response_content.find(start_tag) + len(start_tag)
        end_index = response_content.find(end_tag)
        server_url = response_content[start_index:end_index]
        instance_url = server_url.split('/services')[0]
        return instance_url

    
