from unittest.mock import patch

from geosyspy.services.master_data_management_service import MasterDataManagementService
from geosyspy.utils.http_client import *
from tests.test_helper import *

geometry = "POLYGON((-91.17523978603823 40.29787117039518,-91.17577285022956 40.29199489606421,-91.167613719932 40.29199489606421,-91.1673028670095 40.29867040193312,-91.17523978603823 40.29787117039518))"
sfids = ["g6ap335", "5nlm9e1"]


class TestMasterDataManagementService:
    url = "https://testurl.com"
    http_client = HttpClient(
        "client_id_123",
        "client_secret_123456",
        "username_123",
        "password_123",
        "preprod",
        "na",
    )
    service = MasterDataManagementService(base_url=url, http_client=http_client)

    @patch("geosyspy.utils.http_client.HttpClient.post")
    def test_create_season_field_id(self, post_response):
        post_response.return_value = mock_http_response_text_content(
            "POST",
            load_data_from_textfile(
                "master_data_management_post_extract_id_mock_http_response"
            ),
        )

        response = self.service.create_season_field_id(polygon=geometry)
        assert response.status_code == 200

    @patch("geosyspy.utils.http_client.HttpClient.post")
    def test_extract_season_field_id(self, post_response):
        post_response.return_value = mock_http_response_text_content(
            "POST",
            load_data_from_textfile(
                "master_data_management_post_extract_id_mock_http_response"
            ),
            status_code=201,
        )

        response = self.service.extract_season_field_id(polygon=geometry)
        assert response == "ajqxm3v"

    @patch("geosyspy.utils.http_client.HttpClient.get")
    def test_extract_season_field_id(self, get_response):
        get_response.return_value = mock_http_response_text_content(
            "GET",
            load_data_from_textfile(
                "master_data_management_get_unique_id_mock_http_response"
            ),
            status_code=201,
        )

        response = self.service.get_season_field_unique_id(
            season_field_id="fakeSeasonFieldId"
        )
        assert response == "4XcGhZvA1OjpO3gUwYM61e"

    @patch("geosyspy.utils.http_client.HttpClient.get")
    def test_retrieve_season_fields_in_polygon(self, get_response):
        get_response.return_value = mock_http_response_text_content(
            "GET",
            load_data_from_textfile(
                "master_data_management_retrieve_sfids_mock_http_response"
            ),
            status_code=201,
        )

        response = self.service.retrieve_season_fields_in_polygon(polygon=geometry)
        assert response.status_code == 200

    @patch("geosyspy.utils.http_client.HttpClient.get")
    def test_get_season_fields(self, get_response):
        get_response.return_value = mock_http_response_text_content(
            "GET",
            load_data_from_textfile(
                "master_data_management_retrieve_sfids_mock_http_response"
            ),
            status_code=201,
        )

        response = self.service.get_season_fields(sfids)
        assert len(response) == 20

    @patch("geosyspy.utils.http_client.HttpClient.get")
    def test_get_profile(self, get_response):
        get_response.return_value = mock_http_response_text_content(
            "GET",
            load_data_from_textfile(
                "master_data_management_post_profile_mock_http_response"
            ),
            status_code=201,
        )

        response = self.service.get_profile()
        assert "id" in response

    @patch("geosyspy.utils.http_client.HttpClient.get")
    def test_get_profile_fields(self, get_response):
        get_response.return_value = mock_http_response_text_content(
            "GET",
            load_data_from_textfile(
                "master_data_management_post_profile_mock_http_response"
            ),
            status_code=201,
        )

        response = self.service.get_profile(fields="unitProfileUnitCategories")
        assert "id" in response
