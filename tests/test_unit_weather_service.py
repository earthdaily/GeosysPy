from unittest.mock import patch
import datetime
from geosyspy.services.weather_service import WeatherService
from geosyspy.utils.http_client import *
from tests.test_helper import *
from geosyspy.utils.constants import *

geometry = "POLYGON((-91.17523978603823 40.29787117039518,-91.17577285022956 40.29199489606421,-91.167613719932 40.29199489606421,-91.1673028670095 40.29867040193312,-91.17523978603823 40.29787117039518))"

class TestWeatherService:
    url = "https://testurl.com"
    http_client = HttpClient("client_id_123",
                        "client_secret_123456",
                        "username_123",
                        "password_123",
                        "preprod",
                        "na")
    service = WeatherService(base_url=url, http_client=http_client)

    @patch('geosyspy.utils.http_client.HttpClient.get')
    def test_get_weather(self, get_response):
        get_response.return_value = mock_http_response_text_content("GET", load_data_from_textfile(
            "weather_data_mock_http_response"))
        start_date = datetime.datetime.now()
        end_date = start_date + datetime.timedelta(days=7, hours=3, minutes=30)

        data = self.service.get_weather(polygon=geometry, start_date=start_date, end_date=end_date,
                                        weather_type=WeatherTypeCollection.WEATHER_FORECAST_DAILY,
                                        fields=['precipitation','temperature'])

        assert data.index.__len__() == 6
        assert data['precipitation.cumulative'][0] == 0.22834645669291338
        assert data['temperature.standard'][0] == 71.47399998282076


