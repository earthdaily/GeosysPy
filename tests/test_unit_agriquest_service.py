import datetime
import json
from unittest.mock import patch

from geosyspy.services.agriquest_service import AgriquestService
from geosyspy.utils.http_client import *
from tests.test_helper import *
from geosyspy.utils.constants import *

geometry = "POLYGON((-91.17523978603823 40.29787117039518,-91.17577285022956 40.29199489606421,-91.167613719932 40.29199489606421,-91.1673028670095 40.29867040193312,-91.17523978603823 40.29787117039518))"


class TestAgriquestService:
    url = "https://testurl.com"
    http_client = HttpClient("client_id_123",
                             "client_secret_123456",
                             "username_123",
                             "password_123",
                             "preprod",
                             "na")
    service = AgriquestService(base_url=url, http_client=http_client)

    def test_is_block_for_france(self):
        is_france_block = self.service.is_block_for_france(block_code=AgriquestBlocks.FRA_DEPARTEMENTS)
        assert is_france_block == True

        is_france_block = self.service.is_block_for_france(block_code=AgriquestBlocks.AMU_NORTH_AMERICA)
        assert is_france_block == False

    def test_weather_indicators_builder(self):
        start_date = datetime.datetime.now() - datetime.timedelta(days=1)
        end_date = start_date + datetime.timedelta(days=7, hours=3, minutes=30)

        indicators = self.service.weather_indicators_builder(start_date=start_date.date(), end_date = end_date.date(), isFrance = True)
        assert set(indicators) == set([3,4,5])

        indicators = self.service.weather_indicators_builder(start_date=start_date.date(), end_date=end_date.date(),
                                                             isFrance=False)
        assert set(indicators) == set([2, 4, 5])

    @patch('geosyspy.utils.http_client.HttpClient.post')
    def test_get_agriquest_weather_block_data(self, get_response):
        get_response.return_value = mock_http_response_text_content("POST", load_data_from_textfile(
            "agriquest_weather_data_mock_http_response"))
        start_date = "2022-05-01"
        end_date = "2023-04-28"

        dataset = self.service.get_agriquest_block_weather_data(
            start_date=start_date,
            end_date=end_date,
            block_code=AgriquestBlocks.FRA_DEPARTEMENTS,
            weather_type=AgriquestWeatherType.CUMULATIVE_PRECIPITATION,
            indicator_list=[3,4,5]
        )
        assert dataset.keys()[0] == "AMU"
        assert len(dataset["AMU"]) == 97

    @patch('geosyspy.utils.http_client.HttpClient.post')
    def test_get_agriquest_ndvi_block_data(self, get_response):
        get_response.return_value = mock_http_response_text_content("POST", load_data_from_textfile(
            "agriquest_ndvi_data_mock_http_response"))
        date = "2023-06-05"
        dataset = self.service.get_agriquest_block_ndvi_data(
            date=date,
            commodity=AgriquestCommodityCode.ALL_VEGETATION,
            block_code=AgriquestBlocks.AMU_NORTH_AMERICA,
            indicator_list=[1]
        )
        assert dataset.keys()[0] == "AMU"
        assert dataset.keys()[-1] == "NDVI"
