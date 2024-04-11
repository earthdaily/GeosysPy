""" map product service class """

import logging
from datetime import datetime
from urllib.parse import urljoin
from typing import Optional
from requests import HTTPError
import pandas as pd
from geosyspy.utils.constants import (
    GeosysApiEndpoints,
    SatelliteImageryCollection,
    PRIORITY_HEADERS,
)

from geosyspy.utils.http_client import HttpClient


class MapProductService:

    def __init__(self, base_url: str, http_client: HttpClient, priority_queue: str):
        self.base_url: str = base_url
        self.http_client: HttpClient = http_client
        self.priority_queue: str = priority_queue
        self.logger = logging.getLogger(__name__)

    def get_satellite_coverage(
        self,
        season_field_id: str,
        start_date: datetime,
        end_date: datetime,
        indicator,
        sensors_collection: Optional[list[SatelliteImageryCollection]] = [
            SatelliteImageryCollection.SENTINEL_2,
            SatelliteImageryCollection.LANDSAT_8,
        ],
    ):
        """
        Retrieves satellite coverage for a given season field within the specified time range.

        Args:
            season_field_id (str): The identifier for the season field.
            start_date (datetime): The start date of the time range for satellite coverage.
            end_date (datetime): The end date of the time range for satellite coverage.
            indicator: The indicator for which satellite imagery is requested.
            sensors_collection (Optional[list[SatelliteImageryCollection]], optional):
                A list of satellite imagery collections to consider.
                Defaults to [SatelliteImageryCollection.SENTINEL_2,
                SatelliteImageryCollection.LANDSAT_8].

        Returns:
            DataFrame

        """

        self.logger.info("Calling APIs for coverage")
        start_date: str = start_date.strftime("%Y-%m-%d")
        end_date: str = end_date.strftime("%Y-%m-%d")

        if (
            indicator == ""
            or indicator.upper() == "REFLECTANCE"
            or indicator.upper() == "NDVI"
        ):
            map_type = "INSEASON_NDVI"
        else:
            map_type = f"INSEASON_{indicator.upper()}"

        if sensors_collection is not None:
            sensors: list[str] = [elem.value for elem in sensors_collection]
            parameters = f"?maps.type={map_type}&Image.Sensor=$in:{'|'.join(sensors)}&CoverageType=CLEAR&$limit=None&$filter=Image.Date >= '{start_date}' and Image.Date <= '{end_date}'"
        else:
            parameters = f"?maps.type={map_type}&CoverageType=CLEAR&$limit=None&$filter=Image.Date >= '{start_date}' and Image.Date <= '{end_date}'"

        flm_url: str = urljoin(
            self.base_url,
            GeosysApiEndpoints.FLM_CATALOG_IMAGERY.value.format(season_field_id)
            + parameters,
        )
        response = self.http_client.get(
            flm_url,
            {"X-Geosys-Task-Code": PRIORITY_HEADERS[self.priority_queue]},
        )

        if response.status_code == 200:
            df = pd.json_normalize(response.json())
            if df.empty:
                return df
            else:
                return df[
                    [
                        "coverageType",
                        "maps",
                        "image.id",
                        "image.availableBands",
                        "image.sensor",
                        "image.soilMaterial",
                        "image.spatialResolution",
                        "image.weather",
                        "image.date",
                        "seasonField.id",
                    ]
                ]
        else:
            self.logger.info(response.status_code)

    def get_zipped_tiff(self, field_id: str, image_id: str, indicator: str = ""):
        """
        Retrieves a zipped TIFF image file for a specified field and image identifier.

        Args:
            field_id (str): The identifier for the field.
            image_id (str): The identifier for the image.
            indicator (str, optional): The indicator type. Defaults to "".

        Returns:
            requests.Response: The response object containing the zipped TIFF file.

        Raises:
            HTTPError: If the server returns a non-200 status code.
        """
        parameters = f"/{image_id}/reflectance-map/TOC/image.tiff.zip"

        if indicator != "" and indicator.upper() != "REFLECTANCE":
            parameters = f"/{image_id}/base-reference-map/INSEASON_{indicator.upper()}/image.tiff.zip?resolution=Sensor"

        download_tiff_url: str = urljoin(
            self.base_url,
            GeosysApiEndpoints.FLM_COVERAGE.value.format(field_id) + parameters,
        )

        response_zipped_tiff = self.http_client.get(
            download_tiff_url,
            {"X-Geosys-Task-Code": PRIORITY_HEADERS[self.priority_queue]},
        )
        if response_zipped_tiff.status_code != 200:
            raise HTTPError(
                "Unable to download tiff.zip file. Server error: "
                + str(response_zipped_tiff.status_code)
            )
        return response_zipped_tiff
