from urllib.parse import urljoin
import logging
from geosyspy.utils.http_client import *


class GisService:

    def __init__(self, base_url: str, http_client: HttpClient):
        self.base_url: str = base_url
        self.http_client: HttpClient = http_client

    def get_municipio_id_from_geometry(self,
                                       geometry:str
                                       ):
        """
            method to call Gis Api to retrieve Municipio id from a geometry

                Args:
                     geometry (str): the geometry (WKT or GeosJson) to retrieve the municipio Id
                Returns:
                    the internal id of the municipio
        """

        payload = {
            "properties": ["id"],
            "features": [geometry]
        }
        parameters: str = f"/layerservices/api/v1/layers/BRAZIL_MUNICIPIOS/intersect"
        gis_url: str = urljoin(self.base_url, parameters)
        response = self.http_client.post(gis_url, payload)
        if response.status_code == 200:
            dict_response = response.json()

            try:
                # extract & return municipio id from response
                municipio_id = dict_response[0][0]["properties"]["id"]
                if isinstance(municipio_id, int):
                    return municipio_id
                else:
                    logging.WARN("No municipio id found for this geometry")
                    return 0
            except:
                return 0

        else:
            logging.info(response.status_code)
            raise ValueError(
                f"No municipio id found for this geometry"
            )