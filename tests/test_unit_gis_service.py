from unittest.mock import patch
from geosyspy.services.gis_service import GisService
from geosyspy.utils.http_client import HttpClient
from tests.test_helper import mock_http_response_text_content, load_data_from_textfile

GEOMETRY = "POLYGON((-91.17523978603823 40.29787117039518,-91.17577285022956 40.29199489606421,-91.167613719932 40.29199489606421,-91.1673028670095 40.29867040193312,-91.17523978603823 40.29787117039518))"
LATITUDE = -15.01402
LONGITUDE = -50.7717


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

        municipio_id = self.service.get_municipio_id_from_geometry(geometry=GEOMETRY)
        assert municipio_id == 121935
        
        
    @patch('geosyspy.utils.http_client.HttpClient.get')
    def test_get_farm_info_from_location(self, get_response):
        get_response.return_value = mock_http_response_text_content("GET", load_data_from_textfile(
            "get_farm_info_from_location_data_mock_http_response"))

        result = self.service.get_farm_info_from_location(latitude=LATITUDE, longitude=LONGITUDE)
        assert result is not None
        assert result[0]["properties"]["NOM_MUNICIPIO"] == "Araguapaz"