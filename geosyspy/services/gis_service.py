""" gis service class"""

from urllib.parse import urljoin
import logging
from geosyspy.utils.http_client import HttpClient


class GisService:

    def __init__(self, base_url: str, http_client: HttpClient):
        self.base_url: str = base_url
        self.http_client: HttpClient = http_client
        self.logger = logging.getLogger(__name__)

    def get_municipio_id_from_geometry(self, geometry: str):
        """
        method to call Gis Api to retrieve Municipio id from a geometry

            Args:
                 geometry (str): the geometry (WKT or GeosJson) to retrieve the municipio Id
            Returns:
                the internal id of the municipio
        """

        payload = {"properties": ["id"], "features": [geometry]}
        parameters: str = "/layerservices/api/v1/layers/BRAZIL_MUNICIPIOS/intersect"
        gis_url: str = urljoin(self.base_url, parameters)
        response = self.http_client.post(gis_url, payload, verify_ssl=False)
        if response.status_code == 200:
            dict_response = response.json()

            try:
                # extract & return municipio id from response
                municipio_id = dict_response[0][0]["properties"]["id"]
                if isinstance(municipio_id, int):
                    return municipio_id
                self.logger.warning("No municipio id found for this geometry")
                return 0
            except Exception:
                return 0

        else:
            self.logger.info(response.status_code)
            raise ValueError("No municipio id found for this geometry")

    def get_farm_info_from_location(self, latitude: str, longitude: str):
        """
        method to call Gis Api to retrieve Municipio id from a geometry

            Args:
                 latitude (str): the latitude of the location
                 longitude (str): the longitude of the location
            Returns:
                the farm boundary & informations
        """

        parameters: str = (
            f"/layerservices/api/v1/layers/BR_CAR_PROPERTIES/feature?LOCATION={latitude},{longitude}&format=wkt"
        )
        gis_url: str = urljoin(self.base_url, parameters)
        response = self.http_client.get(gis_url, verify_ssl=False)
        if response.status_code == 200:
            return response.json()

        self.logger.error(response.status_code)
        raise ValueError(f"No farm found for the location ({latitude}, {longitude}")
