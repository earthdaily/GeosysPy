import datetime as dt
import numpy as np
from unittest.mock import patch

from geosyspy.services.vegetation_time_series_service import VegetationTimeSeriesService
from geosyspy.utils.http_client import *
from tests.test_helper import *


class TestVegetationTimeSeriesService:
    url = "https://testurl.com"
    http_client = HttpClient("client_id_123",
                             "client_secret_123456",
                             "username_123",
                             "password_123",
                             "preprod",
                             "na")
    service = VegetationTimeSeriesService(base_url=url, http_client=http_client)

    @patch('geosyspy.utils.http_client.HttpClient.get')
    def test_get_time_series_modis_ndvi(self, get_response):
        get_response.return_value = mock_http_response_text_content("GET", load_data_from_textfile(
            "time_series_modis_ndvi_mock_http_response"))
        start_date = dt.datetime.strptime("2020-01-01", "%Y-%m-%d")
        end_date = dt.datetime.strptime("2020-01-07", "%Y-%m-%d")
        df = self.service.get_modis_time_series(
            "fakeSeasonFieldId", start_date, end_date, ["NDVI"]
        )

        assert df.index.name == "date"
        assert "value" in df.columns
        assert "index" in df.columns
        assert len(df.index) == 7
        date_range = list(map(lambda x: x.strftime("%Y-%m-%d"), df.index))
        assert {"2020-01-01", "2020-01-02", "2020-01-03", "2020-01-04", "2020-01-05", "2020-01-06",
                "2020-01-07"}.issubset(set(date_range))

    @patch('geosyspy.utils.http_client.HttpClient.get')
    def test_get_satellite_image_time_series_modis_ndvi(self, get_response):
        get_response.return_value = mock_http_response_text_content("GET", load_data_from_textfile(
            "satellite_image_time_series_modis_ndvi_mock_http_response"))
        start_date = dt.datetime.strptime("2020-01-01", "%Y-%m-%d")
        end_date = dt.datetime.strptime("2020-01-07", "%Y-%m-%d")

        df = self.service.get_time_series_by_pixel(
            "fakeSeasonFieldId", start_date, end_date, ["NDVI"]
        )
        assert df.index.name == "date"
        assert {"value", "index", "pixel.id"}.issubset(set(df.columns))
        assert np.all((df["index"].values == "NDVI"))
        assert len(df.index) == 14

        assert {"2020-01-01", "2020-01-02", "2020-01-03", "2020-01-04", "2020-01-05", "2020-01-06",
                "2020-01-07"}.issubset(set(df.index))

        assert {"mh11v4i225j4612", "mh11v4i226j4612"}.issubset(set(df["pixel.id"]))


