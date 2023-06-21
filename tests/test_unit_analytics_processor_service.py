from unittest.mock import patch

from geosyspy.services.analytics_processor_service import AnalyticsProcessorService
from geosyspy.utils.constants import *
from geosyspy.utils.http_client import *
from tests.test_helper import *

geometry = "POLYGON((-91.17523978603823 40.29787117039518,-91.17577285022956 40.29199489606421,-91.167613719932 40.29199489606421,-91.1673028670095 40.29867040193312,-91.17523978603823 40.29787117039518))"


class TestAnalyticsProcessorService:
    url = "https://testurl.com"
    http_client = HttpClient("client_id_123",
                        "client_secret_123456",
                        "username_123",
                        "password_123",
                        "preprod",
                        "na")
    service = AnalyticsProcessorService(base_url=url, http_client=http_client)

    @patch('geosyspy.utils.http_client.HttpClient.get')
    def test_get_task_status(self, get_response):
        get_response.return_value = mock_http_response_text_content("GET", load_data_from_textfile(
            "processor_event_data_mock_http_response"))

        task_status = self.service.wait_and_check_task_status("task_id")
        assert task_status == "Ended"

    @patch('geosyspy.utils.http_client.HttpClient.post')
    def test_get_launch_planted_area_processor(self, post_response):
        post_response.return_value = mock_http_response_text_content("POST", load_data_from_textfile(
            "launch_processor_data_mock_http_response"))

        task_id = self.service.launch_planted_area_processor(start_date='2020-01-01', end_date='2021-01-01', seasonfield_id= 'seasonfieldFakeId')
        assert task_id == "cb58faaf8a5640e4913d16bfde3f5bbf"

    @patch('geosyspy.utils.http_client.HttpClient.post')
    def test_get_launch_zarc_processor(self, post_response):
        post_response.return_value = mock_http_response_text_content("POST", load_data_from_textfile(
            "launch_processor_data_mock_http_response"))

        task_id = self.service.launch_zarc_processor(start_date_emergence ='2020-01-01',
                                                     municipio=123,
                                                     soil_type=ZarcSoilType.SOIL_TYPE_1.value,
                                                     nb_days_sowing_emergence=50,
                                                     crop="Corn",
                                                     end_date_emergence='2021-01-01',
                                                     cycle= ZarcCycleType.CYCLE_TYPE_1.value,
                                                     seasonfield_id='seasonFieldFakeId')
        assert task_id == "cb58faaf8a5640e4913d16bfde3f5bbf"

    @patch('geosyspy.utils.http_client.HttpClient.post')
    def test_get_launch_greenness_processor(self, post_response):
        post_response.return_value = mock_http_response_text_content("POST", load_data_from_textfile(
            "launch_processor_data_mock_http_response"))

        task_id = self.service.launch_greenness_processor(start_date='2020-01-01',
                                                          crop="Corn",
                                                          end_date='2021-01-01',
                                                          sowing_date='2020-01-01',
                                                          geometry=geometry,
                                                          seasonfield_id='seasonFieldFakeId')
        assert task_id == "cb58faaf8a5640e4913d16bfde3f5bbf"

    @patch('geosyspy.utils.http_client.HttpClient.post')
    def test_get_launch_emergence_processor(self, post_response):
        post_response.return_value = mock_http_response_text_content("POST", load_data_from_textfile(
            "launch_processor_data_mock_http_response"))

        task_id = self.service.launch_emergence_processor(season_start_day='2020-01-01',
                                                          crop="Corn",
                                                          year=2021,
                                                          emergence_type=Emergence.EMERGENCE_IN_SEASON,
                                                          season_duration=110,
                                                          season_start_month=10,
                                                          geometry=geometry,
                                                          seasonfield_id='seasonFieldFakeId')
        assert task_id == "cb58faaf8a5640e4913d16bfde3f5bbf"

    @patch('geosyspy.utils.http_client.HttpClient.post')
    def test_get_launch_harvest_processor(self, post_response):
        post_response.return_value = mock_http_response_text_content("POST", load_data_from_textfile(
            "launch_processor_data_mock_http_response"))

        task_id = self.service.launch_harvest_processor(season_start_day='2020-01-01',
                                                        year=2021,
                                                        crop ='Corn',
                                                        season_duration=110,
                                                        season_start_month=10,
                                                        harvest_type=Harvest.HARVEST_HISTORICAL,
                                                        geometry=geometry,
                                                        seasonfield_id='seasonFieldFakeId')
        assert task_id == "cb58faaf8a5640e4913d16bfde3f5bbf"

    @patch('geosyspy.utils.http_client.HttpClient.post')
    def test_get_launch_harvest_readiness_processor(self, post_response):
        post_response.return_value = mock_http_response_text_content("POST", load_data_from_textfile(
            "launch_processor_data_mock_http_response"))

        task_id = self.service.launch_harvest_readiness_processor(start_date='2020-01-01',
                                                                  end_date='2020-01-01',
                                                                  sowing_date='2020-01-01',
                                                                  crop ='Corn',
                                                                  geometry=geometry,
                                                                  seasonfield_id='seasonFieldFakeId')
        assert task_id == "cb58faaf8a5640e4913d16bfde3f5bbf"

    @patch('geosyspy.utils.http_client.HttpClient.post')
    def test_get_launch_brazil_in_season_crop_processor(self, post_response):
        post_response.return_value = mock_http_response_text_content("POST", load_data_from_textfile(
            "launch_processor_data_mock_http_response"))

        task_id = self.service.launch_brazil_in_season_crop_id_processor(start_date='2020-01-01',
                                                                         end_date='2020-01-01',
                                                                         geometry=geometry,
                                                                         season=2021,
                                                                         seasonfield_id='seasonFieldFakeId')
        assert task_id == "cb58faaf8a5640e4913d16bfde3f5bbf"

    @patch('geosyspy.utils.http_client.HttpClient.post')
    def test_get_launch_potential_score_processor(self, post_response):
        post_response.return_value = mock_http_response_text_content("POST", load_data_from_textfile(
            "launch_processor_data_mock_http_response"))

        task_id = self.service.launch_potential_score_processor(end_date='2021-01-01',
                                                                season_start_month=10,
                                                                season_duration=120,
                                                                season_start_day=30,
                                                                sowing_date='2020-01-01',
                                                                crop='Corn',
                                                                nb_historical_years=5,
                                                                geometry=geometry,
                                                                seasonfield_id='seasonFieldFakeId')
        assert task_id == "cb58faaf8a5640e4913d16bfde3f5bbf"