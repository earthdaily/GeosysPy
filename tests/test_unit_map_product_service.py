import datetime as dt
from unittest.mock import patch

import numpy as np

from geosyspy.services.map_product_service import MapProductService
from geosyspy.utils.constants import *
from geosyspy.utils.http_client import *
from tests.test_helper import *


class TestMapProductService:
    url = "https://testurl.com"
    http_client = HttpClient(
        "client_id_123",
        "client_secret_123456",
        "username_123",
        "password_123",
        "preprod",
        "na",
    )
    priority_queue = "realtime"

    service = MapProductService(
        base_url=url, http_client=http_client, priority_queue=priority_queue
    )

    @patch("geosyspy.utils.http_client.HttpClient.post")
    def test_get_satellite_coverage(self, get_response):
        get_response.return_value = mock_http_response_text_content(
            "GET",
            load_data_from_textfile(
                "satellite_coverage_image_references_mock_http_response"
            ),
        )
        start_date = dt.datetime.strptime("2022-01-01", "%Y-%m-%d")
        end_date = dt.datetime.strptime("2023-01-01", "%Y-%m-%d")
        info = self.service.get_satellite_coverage(
            "fakeSeasonFieldId",
            None,
            start_date,
            end_date,
            "NDVI",
            [SatelliteImageryCollection.SENTINEL_2],
        )

        assert {
            "coveragePercent",
            "image.id",
            "image.availableBands",
            "image.sensor",
            "image.spatialResolution",
            "image.date",
            "seasonField.id",
        }.issubset(set(info.columns))

    @patch("geosyspy.utils.http_client.HttpClient.post")
    def test_get_difference_map(self, get_response):
        get_response.return_value = mock_http_response_text_content(
            "GET",
            load_data_from_textfile(
                "satellite_coverage_image_references_mock_http_response"
            ),
        )
        image_id_earliest = "sentinel-2-l2a%7CS2B_13SGC_20230520_0_L2A"
        image_id_latest = "sentinel-2-l2a%7CS2B_13SGC_20230530_0_L2A"
        field_id = "bgbrzez"
        response = self.service.get_zipped_tiff_difference_map(
            field_id, image_id_earliest, image_id_latest
        )

        assert response.status_code == 200, "Expected status code to be 200"

        # Assert that the content type is 'image/tiff'
        assert (
            response.headers["Content-Type"] == "image/tiff+zip"
        ), "Expected content type to be 'image/tiff+zip'"

        # Assert that the content is not empty
        assert response.content, "Expected non-empty response content"
