from datetime import datetime
from unittest.mock import patch
from geosyspy import Geosys
from dotenv import load_dotenv
import datetime as dt
import numpy as np
from geosyspy.utils.constants import *
from tests.test_helper import *

# read .env file
load_dotenv()

API_CLIENT_ID = os.getenv("API_CLIENT_ID")
API_CLIENT_SECRET = os.getenv("API_CLIENT_SECRET")
API_USERNAME = os.getenv("API_USERNAME")
API_PASSWORD = os.getenv("API_PASSWORD")

# polygon with two pixels : mh11v4i225j4612, mh11v4i226j4612
POLYGON = "POLYGON((-91.29152885756007 40.39177489815265,-91.28403789132507 40.391776131485386,-91.28386736508233 " \
          "40.389390758655935,-91.29143832829979 40.38874592864832,-91.29152885756007 40.39177489815265))"

class TestGeosys:
    client = Geosys(API_CLIENT_ID,
                    API_CLIENT_SECRET,
                    API_USERNAME,
                    API_PASSWORD,
                    Env.PREPROD,
                    Region.NA
                    )

    def test_authenticate(self):
        credentials = self.client.http_client.get_access_token();
        assert {"access_token", "expires_in", "token_type", "scope", "expires_at",
                "refresh_token"}.issubset(set(credentials.keys()))
        assert credentials['access_token'] is not None
        assert credentials['refresh_token'] is not None
        assert credentials['expires_at'] > datetime.today().timestamp()


    @patch('geosyspy.utils.http_client.HttpClient.get')
    def test_get_time_series_weather_historical_daily(self, get_response):
        get_response.return_value = mock_http_response_text_content("GET", load_data_from_textfile(
            "time_series_weather_historical_daily_mock_http_response"))
        start_date = dt.datetime.strptime("2021-01-01", "%Y-%m-%d")
        end_date = dt.datetime.strptime("2022-01-01", "%Y-%m-%d")
        indicators = [

            "Precipitation",
            "Temperature",
            "Temperature.Ground",
            "Temperature.Standard",
            "Temperature.StandardMax",
            "Location",
            "Date",
        ]

        df = self.client.get_time_series(
            POLYGON,
            start_date,
            end_date,
            WeatherTypeCollection.WEATHER_HISTORICAL_DAILY,
            indicators,
        )

        assert {"precipitation.cumulative", "temperature.standard", "temperature.standardMax",
                "Location"}.issubset(set(df.columns))
        assert df.index.name == "date"
        assert df["weatherType"].iloc[1] == "HISTORICAL_DAILY"

    @patch('geosyspy.utils.http_client.HttpClient.get')
    def test_get_metrics(self, get_response):

        fake_master_data_management_response = mock_http_response_text_content("GET", load_data_from_textfile(
            "master_data_management_get_unique_id_mock_http_response"))
        fake_analytics_fabric_response = mock_http_response_text_content("GET", load_data_from_textfile(
            "metrics_lai_radar_mock_http_response"))
        get_response.side_effect= [fake_master_data_management_response, fake_analytics_fabric_response]


        lai_radar_polygon = "POLYGON((-52.72591542 -18.7395779,-52.72604885 -18.73951122,-52.72603114 -18.73908689,-52.71556835 -18.72490316,-52.71391916 -18.72612966,-52.71362802 -18.72623726,-52.71086473 -18.72804231,-52.72083542 -18.74173696,-52.72118937 -18.74159174,-52.72139229 -18.7418552,-52.72600257 -18.73969719,-52.72591542 -18.7395779))"
        schema_id = "LAI_RADAR"
        start_date = dt.datetime.strptime("2023-01-02", "%Y-%m-%d")
        end_date = dt.datetime.strptime("2023-05-02", "%Y-%m-%d")
        df = self.client.get_metrics(lai_radar_polygon, schema_id, start_date, end_date)

        assert {"Values.RVI", "Values.LAI", "Schema.Id"}.issubset(set(df.columns))
        assert {"2023-01-02T00:00:00Z", "2023-01-03T00:00:00Z", "2023-01-14T00:00:00Z", "2023-02-25T00:00:00Z",
                "2023-03-26T00:00:00Z", "2023-04-27T00:00:00Z", "2023-05-02T00:00:00Z"}.issubset(set(df.index))
        assert df.index.name == "date"

    @patch('geosyspy.utils.http_client.HttpClient.get')
    def test_get_time_series_weather_forecast_daily(self, get_response):
        get_response.return_value = mock_http_response_text_content("GET", load_data_from_textfile(
            "time_series_weather_forecast_daily_mock_http_response"))
        start_date = dt.datetime.strptime("2021-01-01", "%Y-%m-%d")
        end_date = dt.datetime.strptime("2022-01-01", "%Y-%m-%d")
        indicators = [
            "Precipitation",
            "Temperature.Standard",
            "Temperature.StandardMax",
            "Date",
            "Location",
            "WeatherType"
        ]

        df = self.client.get_time_series(
            POLYGON,
            start_date,
            end_date,
            WeatherTypeCollection.WEATHER_HISTORICAL_DAILY,
            indicators,
        )

        assert {'weatherType', 'precipitation.cumulative', 'precipitation.probabilities', 'temperature.standard',
                'temperature.standardMax', 'Location'}.issubset(set(df.columns))
        assert df.index.name == "date"
        assert df["weatherType"].iloc[1] == "FORECAST_DAILY"

    @patch('geosyspy.utils.http_client.HttpClient.get')
    def test_get_satellite_image_time_series(self, get_response):
        fake_get_tiff_zip_response =  mock_http_response_binary_content("GET", load_binary_data_from_zipfile("Refletance_map_mock.tiff.zip"))
        fake_image_time_series_response =  mock_http_response_text_content("GET", load_data_from_textfile(
           "satellite_image_time_series_landsat8_mock_http_response"))
        get_response.side_effect= [fake_image_time_series_response, fake_get_tiff_zip_response, fake_get_tiff_zip_response]
        start_date = dt.datetime.strptime("2022-05-01", "%Y-%m-%d")
        end_date = dt.datetime.strptime("2023-04-28", "%Y-%m-%d")
        dataset = self.client.get_satellite_image_time_series(
            POLYGON,
            start_date,
            end_date,
            collections=[SatelliteImageryCollection.SENTINEL_2, SatelliteImageryCollection.LANDSAT_8],
            indicators=["Reflectance"],
        )
        assert dict(dataset.dims) == {'band': 4, 'y': 80, 'x': 81, 'time': 2}


    @patch('geosyspy.utils.http_client.HttpClient.post')
    def test_get_agriquest_weather_block_data(self, get_response):
        get_response.return_value =  mock_http_response_text_content("POST", load_data_from_textfile(
           "agriquest_weather_data_mock_http_response"))
        start_date = "2022-05-01"
        end_date = "2023-04-28"
        dataset = self.client.get_agriquest_weather_block_data(
            start_date=start_date,
            end_date=end_date,
            block_code=AgriquestBlocks.FRA_DEPARTEMENTS,
            weather_type=AgriquestWeatherType.CUMULATIVE_PRECIPITATION
        )
        assert dataset.keys()[0] == "AMU"
        assert len(dataset["AMU"]) == 97

    @patch('geosyspy.utils.http_client.HttpClient.post')
    def test_get_agriquest_ndvi_block_data(self, get_response):
        get_response.return_value =  mock_http_response_text_content("POST", load_data_from_textfile(
           "agriquest_ndvi_data_mock_http_response"))
        date = "2023-06-05"
        dataset = self.client.get_agriquest_ndvi_block_data(
            day_of_measure=date,
            commodity_code=AgriquestCommodityCode.ALL_VEGETATION,
            block_code=AgriquestBlocks.AMU_NORTH_AMERICA,
        )
        assert dataset.keys()[0] == "AMU"
        assert dataset.keys()[-1] == "NDVI"
