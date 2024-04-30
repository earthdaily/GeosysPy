"""Agriquest Service class"""
import json
import logging
from datetime import datetime
from typing import List
from urllib.parse import urljoin
import pandas as pd
from geosyspy.utils.constants import GeosysApiEndpoints, AgriquestBlocks, AgriquestCommodityCode, AgriquestFranceBlockCode, AgriquestWeatherType
from geosyspy.utils.http_client import HttpClient


class AgriquestService:

    def __init__(self, base_url: str, http_client: HttpClient):
        self.base_url: str = base_url
        self.http_client: HttpClient = http_client
        self.logger = logging.getLogger(__name__)

    def weather_indicators_builder(self, start_date, end_date, is_france):
        """build weather indicators list from 2 dates

            Args:
                start_date (datetime) : the start date used for the request
                end_date (datetime) : the end date used for the request
                is_france (boolean) : resuqest mad for France or not

            Returns:
                A list of int (weather indicators)
                - 2 : ENS observed data
                - 3 : Arome Weather Observed data (Meteo France, France only)
                - 4 : ECMWF Weather  Forecast Data
                - 5 : GFS Weather Forecast Data

            """

        today = datetime.now().date()
        result = []

        # Forecast indicators
        if end_date > today:
            result.extend([4, 5])

        if start_date < today:
            if not is_france:
                result.append(2)
            else:
                result.append(3)

        return result

    def is_block_for_france(self, block_code: AgriquestBlocks):
        """
        method to check if the services block code is dedicated to France

            Args:
                 block_code (AgriquestBlocks): the block code to check
            Returns:
                boolean
        """
        if (block_code.value in [item.value for item in AgriquestFranceBlockCode]):
            return True
        return False

    def get_agriquest_block_weather_data(self,
                                         start_date: str,
                                         end_date: str,
                                         block_code: AgriquestBlocks,
                                         indicator_list: [int],
                                         weather_type: AgriquestWeatherType
                                         ):
        """
            method to call Weather AgriQuest Api and build a panda DataFrame

                Args:
                     start_date (str),
                     end_date (str),
                     block_code (AgriquestBlocks): the AQ block to check ,
                     indicator_list ([int]): list of weather indicator types,
                     weather_type(AgriquestWeatherType): type of weather data to retrieve
                Returns:
                    Panda DataFrame representing the value corresponding to the weather_type of each AMU of the block,
                    on the specified period provided in parameter.
        """

        payload = {
            "analyticName": weather_type.name,
            "commodityId": AgriquestCommodityCode.ALL_VEGETATION.value,
            "startDate": start_date,
            "endDate": end_date,
            "idPixelType": 1,
            "idBlock": block_code.value,
            "indicatorTypeIds": indicator_list
        }
        parameters: str = f"/{weather_type.value}/export-map/year-of-interest"
        aq_url: str = urljoin(self.base_url, GeosysApiEndpoints.AGRIQUEST_ENDPOINT.value + parameters)
        response = self.http_client.post(aq_url, payload)
        if response.status_code == 200:
            dict_response = response.json()
            df = pd.read_json(json.dumps(dict_response))

            # Ignore first line
            df = df.iloc[1:]

            # first line as column header
            df.columns = df.iloc[0]

            # Ignore first line after using columns header
            df = df.iloc[1:]
            if "Name" in df.columns:
                df = df.rename(columns={"Name": "AMU"})
            elif "Nom" in df.columns:
                df = df.rename(columns={"Nom": "AMU"})
            return df
        else:
            self.logger.info(response.status_code)


    def get_agriquest_block_ndvi_data(self,
                                      date: str,
                                      block_code: AgriquestBlocks,
                                      commodity: AgriquestCommodityCode,
                                      indicator_list: List[int]
                                      ):
        """
            method to call year-of-interest AgriQuest Api and build a panda DataFrame

                Args:
                     date (str),
                     block_code (AgriquestBlocks): the AQ block to check ,
                     indicator_list ([int]): list of indicator types
                Returns:
                    Panda DataFrame representing the value corresponding to NDVI index of each AMU of the block,
                    on the specified date (dayOfMeasure) provided in parameter.
        """

        payload = {
            "analyticName": "NDVI",
            "commodityId": commodity.value,
            "dayOfMeasure": date,
            "idPixelType": 1,
            "idBlock": block_code.value,
            "indicatorTypeIds": indicator_list
        }
        parameters: str = "/vegetation-vigor-index/export-map/year-of-interest"
        aq_url: str = urljoin(self.base_url, GeosysApiEndpoints.AGRIQUEST_ENDPOINT.value + parameters)
        response = self.http_client.post(aq_url, payload)
        if response.status_code == 200:
            dict_response = response.json()
            df = pd.read_json(json.dumps(dict_response))

            # Ignore first line
            df = df.iloc[1:]

            # first line as column header
            df.columns = df.iloc[0]

            # Ignore first line after using columns header
            df = df.iloc[1:]
            if "Name" in df.columns:
                df = df.rename(columns={"Name": "AMU", "Value": "NDVI"})
            elif "Nom" in df.columns:
                df = df.rename(columns={"Nom": "AMU", "Valeur": "NDVI"})

            return df
        else:
            self.logger.info(response.status_code)
