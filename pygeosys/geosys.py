from oauthlib.oauth2 import LegacyApplicationClient
from requests_oauthlib import OAuth2Session
from oauthlib.oauth2 import TokenExpiredError
import json
import re
from urllib.parse import urljoin
import pandas as pd
from . import platforms
import logging


def renew_access_token(func):
    """Decorator used to wrap the Geosys class's http methods.

    This decorator wraps the geosys http methods (get,post...) and checks 
    wether the used token is still valid or not. If not, it fetches a new token and
    uses it to make another request.

    """
    
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except TokenExpiredError:
            self._geosys__refresh_token()
            return func(self, *args, **kwargs)
    return wrapper


class Geosys:
    """ The main class for accessing the API's methods.

    Geosys is the main class used to access all the client's methods.

    Attributes:
        str_id_server_url : The API's identity server's url
        base_url : The Geosys APIs root endpoint's url
        master_data_management_endpoint : The master data management api's endpoint
        vts_endpoint : The vts api's endpoint
        vts_by_pixel_endpoint : The vts by pixel api's endpoint
        str_api_client_id : The client's api's client id
        str_api_client_secret : The client's api's client secret
        str_api_username : The client's api's username
        str_api_password : The client's api's password
        token : A valid token used to connect to the API

    """

    def __init__(
        self,
        str_api_client_id,
        str_api_client_secret,
        str_api_username,
        str_api_password,
        str_env='prod'
    ):
        """Initializes a Geosys instance with the required credentials
        to connect to the GEOSYS API.
        """

        self.str_id_server_url = (platforms.IDENTITY_URLS['na'][str_env])
        self.base_url = platforms.GEOSYS_API_URLS['na'][str_env]
        self.master_data_management_endpoint = "master-data-management/v6/seasonfields"
        self.vts_endpoint = "vegetation-time-series/v1/season-fields"
        self.vts_by_pixel_endpoint = "vegetation-time-series/v1/season-fields/pixels"
        self.str_api_client_id = str_api_client_id
        self.str_api_client_secret = str_api_client_secret
        self.str_api_username = str_api_username
        self.str_api_password = str_api_password
        self.token = None
        self.__authenticate()

    def __authenticate(self):
        """ Authenticates the client to the API.

        This method connects the user to the API which generates a token that
        will be valid for one hour. A refresh token is also generated, which 
        makes it possible for the http methods wrappers to get a new token
        once the previous one is no more valid through the renew_access_token 
        decorator. This method is only run once when a Geosys object is instantiated.
        
        """


        try:
            oauth = OAuth2Session(
                client=LegacyApplicationClient(client_id=self.str_api_client_id)
            )
            self.token = oauth.fetch_token(
                token_url=self.str_id_server_url,
                username=self.str_api_username,
                password=self.str_api_password,
                client_id=self.str_api_client_id,
                client_secret=self.str_api_client_secret,
            )
            self.token["refresh_token"] = oauth.cookies["refresh_token"]
            logging.info("Authenticated")
        except Exception as e:
            logging.error(e)

    def __refresh_token(self):
        """Fetches a new token.
        """

        client = OAuth2Session(self.str_api_client_id, token=self.token)
        self.token = client.refresh_token(
            self.str_id_server_url,
            client_id=self.str_api_client_id,
            client_secret=self.str_api_client_secret,
        )

    def __get_matched_str_from_pattern(self, pattern, text):
        """Returns the first occurence of the matched pattern in text.

        Args:
            pattern : A string representing the regex pattern to look for.
            text : The text to look into.
        
        Returns:
            A string representing the first occurence in text of the pattern.

        """
        p = re.compile(pattern)
        return p.findall(text)[0]

    @renew_access_token
    def __get(self, url_endpoint):
        """ Gets the url_endpopint.

        Args:
            url_endpoint : A string representing the url to get.

        Returns:
            A response object.
        """
        client = OAuth2Session(self.str_api_client_id, token=self.token)
        return client.get(url_endpoint)

    @renew_access_token
    def __post(self, url_endpoint, payload):
        """ Posts payload to the url_endpoint.

        Args:
            url_endpoint : A string representing the url to post paylaod to.
            payload : A python dict representing the payload.

        Returns:
            A response object.
        """
        client = OAuth2Session(self.str_api_client_id, token=self.token)
        return client.post(url_endpoint, json=payload)

    def __create_season_field_id(self, polygon):
        """ Posts the payload below to the master data management endpoint.

        This method returns a season field id. The season field id is required
        to request other APIs endpoints.

        Args:
            polygon : A string representing a polygon.

        Returns:
            A response object.

        """
        payload = {
            "Geometry": polygon,
            "Crop": {"Id": "CORN"},
            "SowingDate": "2022-01-01",
        }
        str_mdm_url = urljoin(self.base_url, self.master_data_management_endpoint)

        return self.__post(str_mdm_url, payload)

    def __extract_season_field_id(self, polygon):
        """ Extracts the season field id from the response object.

        Args:
            polygon : A string representing a polygon.

        Returns:
            A string rerpesenting the season field id.

        Raises:
            ValueError: The response status code is not as expected.
        """

        response = self.__create_season_field_id(polygon)
        dict_response = response.json()

        if (
            response.status_code == 400 and "sowingDate" in dict_response["errors"]["body"]
        ):
            pattern = r"\sId:\s(\w+),"
            str_text = dict_response["errors"]["body"]["sowingDate"][0]["message"]
            return self.__get_matched_str_from_pattern(pattern, str_text)

        elif response.status_code == 201:
            return dict_response["id"]
        else:
            raise ValueError(
                f"Cannot handle HTTP response : {str(response.status_code)} : {str(response.json())}"
            )

    def get_time_series(self, polygon, start_date, end_date, indicator):
        """ Returns a pandas DataFrame.
        
        This method returns a time series of 'indicator' within the range 
        'start_date' -> 'end_date' as a pandas DataFrame :

                     | index     | value |
                     _____________________
            date     | indicator |   1   |
        __________________________________
         start-date  | indicator |   2   |
         ...         | ...       |  ...  |
           end-date  | indicator |   8   |

        Args:
            polygon : A string representing a polygon.
            start_date : A datetime object representing the start date of the date interval 
        the user wants to filter on.
            end_date : A datetime object representing the final date of the date interval 
        the user wants to filter on.
            indicator : A string representing the indicator whose time series the user wants.

        Returns:
            df : A Pandas DataFrame containing two columns : index and value, and an index called 'date'.

        """
        logging.info("Calling APIs for aggregated time series")
        str_season_field_id = self.__extract_season_field_id(polygon)
        str_start_date = start_date.strftime("%Y-%m-%d")
        str_end_date = end_date.strftime("%Y-%m-%d")
        parameters = f"/values?$offset=0&$limit=2000&$count=false&SeasonField.Id={str_season_field_id}&index={indicator}&$filter=Date >= '{str_start_date}' and Date <= '{str_end_date}'"
        str_vts_url = urljoin(self.base_url, self.vts_endpoint + parameters)

        response = self.__get(str_vts_url)

        if response.status_code == 200:
            dict_response = response.json()
            df = pd.read_json(json.dumps(dict_response))
            df.set_index("date", inplace=True)
            return df
        else:
            logging.info(response.status_code)

    def get_time_series_by_pixel(self, polygon, start_date, end_date, indicator):
        """ Returns a pandas DataFrame.
        
        This method returns a time series of 'indicator' by pixel within the range 
        'start_date' -> 'end_date' as a pandas DataFrame :

                     | index     | value | pixel.id | pixel.coverageRatio
                     ____________________________________________________
            date     | indicator |       |          |
        _________________________________________________________________
         start-date  | indicator |   2   |    1     |         1
         ...         | ...       |  ...  |   ...    |         1
           end-date  | indicator |   8   |   1000   |         1

        Args:
            polygon : A string representing a polygon.
            start_date : A datetime object representing the start date of the date interval 
        the user wants to filter on.
            end_date : A datetime object representing the final date of the date interval 
        the user wants to filter on.
            indicator : A string representing the indicator whose time series the user wants.

        Returns:
            df : A Pandas DataFrame containing four columns : index, value, pixel.id, pixel.coverageRatio and an index called 'date'.

        """

        logging.info("Calling APIs for time series by the pixel")
        str_season_field_id = self.__extract_season_field_id(polygon)
        str_start_date = start_date.strftime("%Y-%m-%d")
        str_end_date = end_date.strftime("%Y-%m-%d")
        parameters = f"/values?$offset=0&$limit=2000&$count=false&SeasonField.Id={str_season_field_id}&index={indicator}&$filter=Date >= '{str_start_date}' and Date <= '{str_end_date}'"
        str_vts_url = urljoin(self.base_url, self.vts_by_pixel_endpoint + parameters)

        response = self.__get(str_vts_url)

        if response.status_code == 200:
            df = pd.json_normalize(response.json())
            df.set_index("date", inplace=True)
            return df
        else:
            logging.info(response.status_code)
