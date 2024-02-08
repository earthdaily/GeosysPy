from datetime import datetime
from unittest.mock import patch
from geosyspy import Geosys
from dotenv import load_dotenv
import datetime as dt
import numpy as np

from geosyspy.services.gis_service import GisService
from geosyspy.utils.constants import *
from geosyspy.utils.http_client import *
from tests.test_helper import *

geometry = "POLYGON((-91.17523978603823 40.29787117039518,-91.17577285022956 40.29199489606421,-91.167613719932 40.29199489606421,-91.1673028670095 40.29867040193312,-91.17523978603823 40.29787117039518))"


class TestGisService:
    url = "https://testurl.com"
    http_client = HttpClient("client_id_123",
                        "client_secret_123456",
                        "username_123",
                        "password_123",
                        "preprod",
                        "na")
    service = GisService(base_url=url, http_client=http_client)

    @patch('geosyspy.utils.http_client.HttpClient.post')
    def test_get_municipio_id_from_geometry(self, post_response):
        post_response.return_value = mock_http_response_text_content("POST", load_data_from_textfile(
            "gis_layer_municipio_data_mock_http_response"))

        municipio_id = self.service.get_municipio_id_from_geometry(geometry=geometry)
        assert municipio_id == 121935