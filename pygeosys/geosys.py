from oauthlib.oauth2 import LegacyApplicationClient
from requests_oauthlib import OAuth2Session
from oauthlib.oauth2 import TokenExpiredError
import json
import re
from urllib.parse import urljoin
import pandas as pd
import logging


def renew_access_token(func):
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except TokenExpiredError:
            self._geosys__refresh_token()
            return func(self, *args, **kwargs)

    return wrapper


class Geosys:
    def __init__(
        self,
        str_api_client_id,
        str_api_client_secret,
        str_api_username,
        str_api_password,
    ):
        """ """
        self.base_url = "https://api-pp.geosys-na.net"
        self.master_data_management_endpoint = "master-data-management/v6/seasonfields"
        self.vts_endpoint = "vegetation-time-series/v1/season-fields"
        self.vts_by_pixel_endpoint = "vegetation-time-series/v1/season-fields/pixels"
        self.str_id_server_url = (
            "https://identity.preprod.geosys-na.com/v2.1/connect/token"
        )
        self.str_api_client_id = str_api_client_id
        self.str_api_client_secret = str_api_client_secret
        self.str_api_username = str_api_username
        self.str_api_password = str_api_password
        self.token = None
        self.__authenticate()

    def __authenticate(self):
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
        client = OAuth2Session(self.str_api_client_id, token=self.token)
        self.token = client.refresh_token(
            self.str_id_server_url,
            client_id=self.str_api_client_id,
            client_secret=self.str_api_client_secret,
        )

    def __get_matched_str_from_pattern(self, pattern, text):
        p = re.compile(pattern)
        return p.findall(text)[0]

    @renew_access_token
    def get(self, url_endpoint):
        client = OAuth2Session(self.str_api_client_id, token=self.token)
        return client.get(url_endpoint)

    @renew_access_token
    def post(self, url_endpoint, payload):
        client = OAuth2Session(self.str_api_client_id, token=self.token)
        return client.post(url_endpoint, json=payload)

    def __create_season_field_id(self, polygon):
        payload = {
            "Geometry": polygon,
            "Crop": {"Id": "CORN"},
            "SowingDate": "2022-01-01",
        }
        str_mdm_url = urljoin(self.base_url, self.master_data_management_endpoint)

        return self.post(str_mdm_url, payload)

    def __extract_season_field_id(self, polygon):

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

        logging.info("Calling APIs for aggregated time series")
        str_season_field_id = self.__extract_season_field_id(polygon)
        str_start_date = start_date.strftime("%Y-%m-%d")
        str_end_date = end_date.strftime("%Y-%m-%d")
        parameters = f"/values?$offset=0&$limit=2000&$count=false&SeasonField.Id={str_season_field_id}&index={indicator}&$filter=Date > '{str_start_date}' and Date < '{str_end_date}'"
        str_vts_url = urljoin(self.base_url, self.vts_endpoint + parameters)

        response = self.get(str_vts_url)

        if response.status_code == 200:
            dict_response = response.json()
            df = pd.read_json(json.dumps(dict_response))
            df.set_index("date", inplace=True)
            return df
        else:
            logging.info(response.status_code)

    def get_time_series_by_pixel(self, polygon, start_date, end_date, indicator):

        logging.info("Calling APIs for time series by the pixel")
        str_season_field_id = self.__extract_season_field_id(polygon)
        str_start_date = start_date.strftime("%Y-%m-%d")
        str_end_date = end_date.strftime("%Y-%m-%d")
        parameters = f"/values?$offset=0&$limit=2000&$count=false&SeasonField.Id={str_season_field_id}&index={indicator}&$filter=Date > '{str_start_date}' and Date < '{str_end_date}'"
        str_vts_url = urljoin(self.base_url, self.vts_by_pixel_endpoint + parameters)

        response = self.get(str_vts_url)

        if response.status_code == 200:
            df = pd.json_normalize(response.json())
            df.set_index("date", inplace=True)
            return df
        else:
            logging.info(response.status_code)
