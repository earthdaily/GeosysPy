""" Mastaer data managenement service class"""
import logging
from urllib.parse import urljoin
from typing import List
from datetime import datetime

from geosyspy.utils.constants import GeosysApiEndpoints, SEASON_FIELD_ID_REGEX
from geosyspy.utils.helper import Helper
from geosyspy.utils.http_client import HttpClient



class MasterDataManagementService:

    def __init__(self, base_url: str, http_client: HttpClient):
        self.base_url: str = base_url
        self.http_client: HttpClient = http_client
        self.logger = logging.getLogger(__name__)

    def create_season_field_id(self, polygon: str) -> object:
        """Posts the payload below to the master data management endpoint.

        This method returns a season field id. The season field id is required
        to request other APIs endpoints.

        Args:
            polygon : A string representing a polygon.

        Returns:
            A response object.

        """

        year = datetime.now().year
        payload = {
            "Geometry": polygon,
            "Crop": {"Id": "CORN"},
            "SowingDate": f"{year}-01-01",
        }
        mdm_url: str = urljoin(
            self.base_url,
            GeosysApiEndpoints.MASTER_DATA_MANAGEMENT_ENDPOINT.value + "/seasonfields",
        )
        return self.http_client.post(mdm_url, payload)

    def extract_season_field_id(self, polygon: str) -> str:
        """Extracts the season field id from the response object.

        Args:
            polygon : A string representing a polygon.

        Returns:
            A string representing the season field id.

        Raises:
            ValueError: The response status code is not as expected.
        """

        response = self.create_season_field_id(polygon)
        dict_response = response.json()

        if (
            response.status_code == 400
            and "sowingDate" in dict_response["errors"]["body"]
        ):

            text: str = dict_response["errors"]["body"]["sowingDate"][0]["message"]
            return Helper.get_matched_str_from_pattern(SEASON_FIELD_ID_REGEX, text)

        if response.status_code == 201:
            return dict_response["id"]
        raise ValueError(
            f"Cannot handle HTTP response : {str(response.status_code)} : {str(response.json())}"
        )

    def get_season_field_unique_id(self, season_field_id: str) -> str:
        """Extracts the season field unique id from the response object.

        Args:
            season_field_id : A string representing the seasonfield legacy na id

        Returns:
            A string representing the season field unique id.

        Raises:
            ValueError: The response status code is not as expected.
        """

        mdm_url: str = urljoin(
            self.base_url,
            GeosysApiEndpoints.MASTER_DATA_MANAGEMENT_ENDPOINT.value
            + f"/seasonfields/{season_field_id}?$fields=externalids",
        )

        response = self.http_client.get(mdm_url)

        dict_response = response.json()

        # extract unique id from response:
        if response.status_code == 200:
            return dict_response["externalIds"]["id"]
        raise ValueError(
            f"Cannot handle HTTP response : {str(response.status_code)} : {str(response.json())}"
        )

    def check_season_field_exists(self, season_field_id: str) -> str:
        """Check if the seasonfield id exists.

        Args:
            season_field_id : A string representing the seasonfield legacy na id

        Returns:
            A bool value

        Raises:
            ValueError: The response status code is not as expected.
        """

        mdm_url: str = urljoin(
            self.base_url,
            GeosysApiEndpoints.MASTER_DATA_MANAGEMENT_ENDPOINT.value
            + f"/seasonfields/{season_field_id}",
        )

        response = self.http_client.get(mdm_url)

        # dict_response = response.json()

        # extract unique id from response:
        if response.status_code == 200:
            return True
        return False

    def get_available_crops_code(self) -> List[str]:
        """Extracts the list of available crops for the connected user

        Args:

        Returns:
            A list of string representing the available crop codes

        Raises:
            ValueError: The response status code is not as expected.
        """
        mdm_url: str = urljoin(
            self.base_url,
            GeosysApiEndpoints.MASTER_DATA_MANAGEMENT_ENDPOINT.value
            + "/crops?$fields=code&$limit=none",
        )

        response = self.http_client.get(mdm_url)

        dict_response = response.json()

        if response.status_code == 200:
            return dict_response
        raise ValueError(
            f"Cannot handle HTTP response : {str(response.status_code)} : {str(response.json())}"
        )

    def get_permission_codes(self) -> List[str]:
        """Extracts the list of available permissions for the connected user

        Args:

        Returns:
            A list of string representing the available permissions codes

        Raises:
            ValueError: The response status code is not as expected.
        """
        mdm_url: str = urljoin(
            self.base_url,
            GeosysApiEndpoints.MASTER_DATA_MANAGEMENT_ENDPOINT.value
            + "/profile?$fields=permissions&$limit=none",
        )

        response = self.http_client.get(mdm_url)

        dict_response = response.json()

        if response.status_code == 200:
            return dict_response
        raise ValueError(
            f"Cannot handle HTTP response : {str(response.status_code)} : {str(response.json())}"
        )
