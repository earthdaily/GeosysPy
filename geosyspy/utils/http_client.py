from . import oauth2_client
from oauthlib.oauth2 import TokenExpiredError
from requests_oauthlib import OAuth2Session


def renew_access_token(func):
    """Decorator used to wrap the Geosys class's http methods.

    This decorator wraps the geosys http methods (get,post...) and checks
    whether the used token is still valid or not. If not, it fetches a new token and
    uses it to make another request.

    """

    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except TokenExpiredError:
            self.access_token = self.__client_oauth.get_refresh_token()
            return func(self, *args, **kwargs)

    return wrapper


class HttpClient:

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
        self.__client_oauth = oauth2_client.Oauth2Api(
            client_id=client_id,
            client_secret=client_secret,
            password=password,
            username=username,
            enum_env=enum_env,
            enum_region=enum_region,
            bearer_token=bearer_token
        )
        self.access_token = self.__client_oauth.token

        self.__client = OAuth2Session(self.__client_oauth.client_id, token=self.__client_oauth.token)

    @renew_access_token
    def get(self, url_endpoint: str, headers={}):
        """Gets the url_endpopint.

        Args:
            url_endpoint : A string representing the url to get.

        Returns:
            A response object.
        """
        return self.__client.get(url_endpoint, headers=headers)

    @renew_access_token
    def post(self, url_endpoint: str, payload: dict, headers={}):
        """Posts payload to the url_endpoint.

        Args:
            url_endpoint : A string representing the url to post paylaod to.
            payload : A python dict representing the payload.

        Returns:
            A response object.
        """
        return self.__client.post(url_endpoint, json=payload, headers=headers)

    @renew_access_token
    def patch(self, url_endpoint: str, payload: dict):
        """Patchs payload to the url_endpoint.

        Args:
            url_endpoint : A string representing the url to patch paylaod to.
            payload : A python dict representing the payload.

        Returns:
            A response object.
        """
        return self.__client.patch(url_endpoint, json=payload)

    def get_access_token(self):
        return self.access_token
