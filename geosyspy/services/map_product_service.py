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
        coveragePercent: int = 80,
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
            parameters = f"?maps.type={map_type}&Image.Sensor=$in:{'|'.join(sensors)}&CoverageType={coveragePercent}&$limit=None&$filter=Image.Date >= '{start_date}' and Image.Date <= '{end_date}'"
        else:
            parameters = f"?maps.type={map_type}&coveragePercent={coveragePercent}&$limit=None&$filter=Image.Date >= '{start_date}' and Image.Date <= '{end_date}'"

        fields = f"&$fields=coveragePercent,maps,image.id,image.sensor,image.availableBands,coveragePercent,image.spatialResolution,image.date,seasonField.id"
        flm_url: str = urljoin(
            self.base_url,
            GeosysApiEndpoints.FLM_CATALOG_IMAGERY.value.format(season_field_id)
            + parameters + fields,
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
                        "coveragePercent",
                        "maps",
                        "image.id",
                        "image.availableBands",
                        "image.sensor",                        
                        "image.spatialResolution",
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
    def get_satellite_coverage_post(self, polygon: str,
                               start_date: datetime,
                               end_date: datetime,
                            #    indicator,
                               sensors_collection: list[SatelliteImageryCollection] = [
                                   SatelliteImageryCollection.SENTINEL_2,
                                   SatelliteImageryCollection.LANDSAT_8]
                               ):

        self.logger.info("Calling APIs for coverage")
        start_date: str = start_date.strftime("%Y-%m-%d")
        end_date: str = end_date.strftime("%Y-%m-%d")
        sensors: list[str] = [elem.value for elem in sensors_collection]

        parameters = f"?Image.Sensor=$in:{'|'.join(sensors)}&coveragePercent=$gte:20&$filter=Image.Date >= '{start_date}' and Image.Date <= '{end_date}'&$limit=None&mask=Auto"
        payload = {
            "seasonFields": [
                {
                "geometry": polygon
                }
            ]
        }

        flm_url: str = urljoin(
            self.base_url,
            GeosysApiEndpoints.FLM_CATALOG_IMAGERY_POST.value + parameters,
        )
        response = self.http_client.post(
            flm_url,
            payload,
            {"X-Geosys-Task-Code": PRIORITY_HEADERS[self.priority_queue]},
        )

        if response.status_code == 200:
            df = pd.json_normalize(response.json())
            if df.empty:
                return df
            else:
                return df[
                    [
                        "coveragePercent",
                        "maps",
                        "image.id",
                        "image.availableBands",
                        "image.sensor",                        
                        "image.spatialResolution",
                        "image.date",
                        "seasonField.id",
                    ]
                ]
        else:
            self.logger.info(response.status_code)

    def get_zipped_tiff(self, field_id: str,
                          image_id: str,
                          indicator: str = ""):
        parameters = f"/{image_id}/reflectance-map/TOC/image.tiff.zip"

        if indicator != "" and indicator.upper() != "REFLECTANCE":
            parameters = f"/{image_id}/base-reference-map/INSEASON_{indicator.upper()}/image.tiff.zip?resolution=Sensor"

        download_tiff_url: str = urljoin(
            self.base_url,
            GeosysApiEndpoints.FLM_BASE_REFERENCE_MAP.value.format(field_id) + parameters,
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

    def get_product(self, field_id: str, image_id: str, indicator: str, image: str = None):
        """
        Retrieves image product for a given season field and image reference from MP API.

        Args:
            season_field_id (str): The identifier for the season field.
            image_id (str): The image reference from the satellite coverage.
            indicator: The indicator for which product is requested.

        Returns:
            DataFrame

        """
        parameters = f"/{image_id}/base-reference-map/{indicator}"

        if image is not None:
            parameters += f"/image{image}"

        get_product_url: str = urljoin(
            self.base_url,
            GeosysApiEndpoints.FLM_BASE_REFERENCE_MAP.value.format(field_id) + parameters
        )
        response_product = self.http_client.get(
            get_product_url,
            {"X-Geosys-Task-Code": PRIORITY_HEADERS[self.priority_queue]},
        )

        if response_product.status_code != 200:
            raise HTTPError(
                "Unable to retrieve product. Server error: "
                + str(response_product.status_code)
            )
        
        if image is not None:
            with open("output" + image, "wb") as file:
                file.write(response_product.content)
            return "Image stocked locally"
        else:
            df = pd.json_normalize(response_product.json())
            return df
