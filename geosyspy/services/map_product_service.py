import json
import logging
import pandas as pd
from datetime import datetime
from urllib.parse import urljoin
from geosyspy.utils.constants import *
from geosyspy.utils.http_client import *
from requests import HTTPError
from typing import Optional

class MapProductService:

    def __init__(self, base_url: str, http_client: HttpClient, priority_queue: str):
        self.base_url: str = base_url
        self.http_client: HttpClient = http_client
        self.priority_queue: str = priority_queue
        self.logger = logging.getLogger(__name__)

    def get_satellite_coverage(self, season_field_id: str,
                               start_date: datetime,
                               end_date: datetime,
                               indicator,
                               sensors_collection: Optional[list[SatelliteImageryCollection]]= [
                                   SatelliteImageryCollection.SENTINEL_2,
                                   SatelliteImageryCollection.LANDSAT_8]
                               ):

        self.logger.info("Calling APIs for coverage")
        start_date: str = start_date.strftime("%Y-%m-%d")
        end_date: str = end_date.strftime("%Y-%m-%d")
            

        if indicator == "" or indicator.upper() == "REFLECTANCE" or indicator.upper() == "NDVI":
            mapType = "INSEASON_NDVI"
        else:
            mapType = f"INSEASON_{indicator.upper()}"
        
        if sensors_collection is not None:
            sensors: list[str] = [elem.value for elem in sensors_collection]
            parameters = f"?maps.type={mapType}&Image.Sensor=$in:{'|'.join(sensors)}&CoverageType=CLEAR&$limit=None&$filter=Image.Date >= '{start_date}' and Image.Date <= '{end_date}'"
        else:
            parameters = f"?maps.type={mapType}&CoverageType=CLEAR&$limit=None&$filter=Image.Date >= '{start_date}' and Image.Date <= '{end_date}'"

        flm_url: str = urljoin(
            self.base_url,
            GeosysApiEndpoints.FLM_CATALOG_IMAGERY.value.format(season_field_id) + parameters,
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

    def get_zipped_tiff(self, field_id: str,
                          image_id: str,
                          indicator: str = ""):
        parameters = f"/{image_id}/reflectance-map/TOC/image.tiff.zip"

        if indicator != "" and indicator.upper() != "REFLECTANCE":
            parameters = f"/{image_id}/base-reference-map/INSEASON_{indicator.upper()}/image.tiff.zip?resolution=Sensor"

        download_tiff_url: str = urljoin(
            self.base_url, GeosysApiEndpoints.FLM_COVERAGE.value.format(field_id) + parameters
        )

        response_zipped_tiff = self.http_client.get(
            download_tiff_url,
            {"X-Geosys-Task-Code": PRIORITY_HEADERS[self.priority_queue]},
        )
        if response_zipped_tiff.status_code != 200:
            raise HTTPError("Unable to download tiff.zip file. Server error: " + str(response_zipped_tiff.status_code))
        return response_zipped_tiff