import datetime as dt
import numpy as np
from unittest.mock import patch

from geosyspy.services.map_product_service import MapProductService
from geosyspy.utils.http_client import *
from tests.test_helper import *
from geosyspy.utils.constants import *


class TestMapProductService:
    url = "https://testurl.com"
    http_client = HttpClient("client_id_123",
                             "client_secret_123456",
                             "username_123",
                             "password_123",
                             "preprod",
                             "na")
    priority_queue = "realtime"

    service = MapProductService(base_url=url, http_client=http_client,priority_queue=priority_queue)

    @patch('geosyspy.utils.http_client.HttpClient.get')
    def test_get_satellite_coverage(self, get_response):
        get_response.return_value = mock_http_response_text_content("GET", load_data_from_textfile(
            "satellite_coverage_image_references_mock_http_response"))
        start_date = dt.datetime.strptime("2022-01-01", "%Y-%m-%d")
        end_date = dt.datetime.strptime("2023-01-01", "%Y-%m-%d")
        info = self.service.get_satellite_coverage(
            "fakeSeasonFieldId", start_date, end_date, "NDVI", [SatelliteImageryCollection.SENTINEL_2]
        )

        assert {"coverageType", "image.id", "image.availableBands", "image.sensor",
                "image.spatialResolution", "image.date", "seasonField.id"}.issubset(set(info.columns))



