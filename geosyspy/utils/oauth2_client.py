""" Oauth2api class"""
import logging
from oauthlib.oauth2 import LegacyApplicationClient
from requests_oauthlib import OAuth2Session
from . import geosys_platform_urls



class Oauth2Api:
    def __init__(
            self,
            client_id: str,
            client_secret: str,
            username: str,
            password: str,
            enum_env: str,
            enum_region: str,
            bearer_token: str = None
    ):
        """Initializes a Geosys instance with the required credentials
        to connect to the GEOSYS API.
        """
        self.logger = logging.getLogger(__name__)
        self.client_id = client_id
        self.server_url = geosys_platform_urls.IDENTITY_URLS[enum_region][enum_env]
        self.client_secret = client_secret
        self.token = None
        self.username = username
        self.password = password

        if bearer_token:
            self.token = {"access_token": bearer_token}
        else:
            self.__authenticate()

    def __authenticate(self):
        """Authenticates the http_client to the API.

        This method connects the user to the API which generates a token that
        will be valid for one hour. A refresh token is also generated, which
        makes it possible for the http methods wrappers to get a new token
        once the previous one is no more valid through the renew_access_token
        decorator. This method is only run once when a Geosys object is instantiated.

        """

        try:
            oauth = OAuth2Session(
                client=LegacyApplicationClient(client_id=self.client_id)
            )
            self.token = oauth.fetch_token(
                token_url=self.server_url,
                username=self.username,
                password=self.password,
                client_id=self.client_id,
                client_secret=self.client_secret,
            )
            self.token["refresh_token"] = oauth.cookies["refresh_token"]
            self.logger.info("Authenticated")
        except Exception as e:
            logging.error(e)

    def get_refresh_token(self):
        """Fetches a new token."""
        client = OAuth2Session(self.client_id, token=self.token)
        return client.refresh_token(
            self.server_url,
            client_id=self.client_id,
            client_secret=self.client_secret,
        )
