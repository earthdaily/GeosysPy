""" map product service class """

import logging
from datetime import datetime
from typing import Optional
from urllib.parse import urljoin

import pandas as pd
from requests import HTTPError

from geosyspy.utils.constants import (
    PRIORITY_HEADERS,
    GeosysApiEndpoints,
    SatelliteImageryCollection,
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
        polygon: str,
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
            map_type = "NDVI"
        else:
            map_type = f"{indicator.upper()}"

        if sensors_collection is not None:
            sensors: list[str] = [elem.value for elem in sensors_collection]
            parameters = f"?maps.type={map_type}&Image.Sensor=$in:{'|'.join(sensors)}&coveragePercent={coveragePercent}&mask=Auto&$filter=Image.Date>='{start_date}' and Image.Date<='{end_date}'"
        else:
            parameters = f"?maps.type={map_type}&coveragePercent={coveragePercent}&mask=Auto&$filter=Image.Date>='{start_date}' and Image.Date<='{end_date}'"

        fields = f"&$fields=coveragePercent,maps,image.id,image.sensor,image.availableBands,image.spatialResolution,image.date,seasonField.id"
        flm_url: str = urljoin(
            self.base_url,
            GeosysApiEndpoints.FLM_CATALOG_IMAGERY_POST.value + parameters + fields,
        )

        if not season_field_id:
            payload = {"seasonFields": [{"geometry": polygon}]}
        else:
            payload = {"seasonFields": [{"id": season_field_id}]}

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

    def get_zipped_tiff(
        self, field_id: str, field_geometry: str, image_id: str, indicator: str
    ):

        if indicator != "" and indicator.upper() != "REFLECTANCE":
            parameters = f"/{indicator.upper()}/image.tiff.zip?resolution=Sensor"
            download_tiff_url: str = urljoin(
                self.base_url,
                GeosysApiEndpoints.FLM_BASE_REFERENCE_MAP_POST.value + parameters,
            )
        else:
            parameters = f"/TOC/image.tiff.zip?resolution=Sensor"
            download_tiff_url: str = urljoin(
                self.base_url, GeosysApiEndpoints.FLM_REFLECTANCE_MAP.value + parameters
            )

        if not field_id or field_id == "":
            payload = {
                "image": {"id": image_id},
                "seasonField": {"geometry": field_geometry},
            }
        else:
            payload = {"image": {"id": image_id}, "seasonField": {"id": field_id}}

        response_zipped_tiff = self.http_client.post(
            download_tiff_url,
            payload,
            {"X-Geosys-Task-Code": PRIORITY_HEADERS[self.priority_queue]},
        )
        if response_zipped_tiff.status_code != 200:
            raise HTTPError(
                "Unable to download tiff.zip file. Server error: "
                + str(response_zipped_tiff.status_code)
            )
        return response_zipped_tiff

    def get_product(
        self, field_id: str, image_id: str, indicator: str, image: str = None
    ):
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
            GeosysApiEndpoints.FLM_BASE_REFERENCE_MAP.value.format(field_id)
            + parameters,
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

    def get_zipped_tiff_difference_map(
        self,
        field_id: str,
        field_geometry: str,
        image_id_earliest: str,
        image_id_latest: str,
    ):
        """
        Retrieves tiff resulting of a difference between 2 in-season images for a given season field from MP API.

        Args:
            field_id (str): The identifier for the season field.
            field_geometry (str): The geometry of the season field
            image_id_earliest (str): The earliest image reference from the satellite coverage.
            image_id_latest (str): The latest image reference from the satellite coverage.

        Returns:
            zipped tiff
        """

        parameters = f"/DIFFERENCE_NDVI/image.tiff.zip"
        download_tiff_url: str = urljoin(
            self.base_url, GeosysApiEndpoints.FLM_DIFFERENCE_MAP.value + parameters
        )

        if not field_id or field_id == "":
            payload = {
                "earliestImage": {"id": image_id_earliest},
                "latestImage": {"id": image_id_latest},
                "seasonField": {"geometry": field_geometry},
            }
        else:
            payload = {
                "earliestImage": {"id": image_id_earliest},
                "latestImage": {"id": image_id_latest},
                "seasonField": {"id": field_id},
            }

        response_zipped_tiff = self.http_client.post(
            download_tiff_url,
            payload,
            {"X-Geosys-Task-Code": PRIORITY_HEADERS[self.priority_queue]},
        )

        if response_zipped_tiff.status_code != 200:
            raise HTTPError(
                "Unable to download tiff.zip file. Server error: "
                + str(response_zipped_tiff.status_code)
            )
        return response_zipped_tiff
