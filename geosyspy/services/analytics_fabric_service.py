"""Analytics Fabric service class"""
import logging
from datetime import datetime
from urllib.parse import urljoin
from typing import Optional
import pandas as pd
from geosyspy.utils.constants import GeosysApiEndpoints
from geosyspy.utils.http_client import HttpClient


class AnalyticsFabricService:

    def __init__(self, base_url: str, http_client: HttpClient):
        self.base_url: str = base_url
        self.http_client: HttpClient = http_client
        self.logger = logging.getLogger(__name__)


    @staticmethod
    def build_timestamp_query_parameters(start_date: Optional[datetime] = None,
                                         end_date: Optional[datetime] = None):
        """ Build Timestamp parameter to provide in AF api calls
        Args:
            start_date : A datetime object representing the start date of the date interval the user wants to filter on.
            end_date : A datetime object representing the final date of the date interval the user wants to filter on.
        Returns:
            A string query params to include in api call
        """

        if start_date is None and end_date is None:
            return ""
        if start_date is not None and end_date is None:
            return f'&Timestamp=$gte:{start_date}'
        elif start_date is None and end_date is not None:
            return f'&Timestamp:$lte:{end_date}'
        else:
            return f'&Timestamp=$between:{start_date}|{end_date}'

    def create_schema_id(self, schema_id: str,
                        schema: dict):
        """Create a schema in Analytics Fabrics

        Args:
            schema_id: The schema id to create
            schema: Dict representing the schema {'property_name': 'property_type'}

        Returns:
            A http response object.
        """
        properties = []
        for prop_name, datatype in schema.items():
            prop = {
                "Name": prop_name,
                "Datatype": datatype,
                "UnitCategory": None,
                "IsPartOfKey": False,
                "IsOptional": False,
            }
            properties.append(prop)

        payload = {
            "Id": schema_id,
            "Properties": properties,
            "Metadata": {"OnAggregationCompleted": "Off"},
        }
        af_url: str = urljoin(
            self.base_url,
            GeosysApiEndpoints.ANALYTICS_FABRIC_SCHEMA_ENDPOINT.value,
        )
        response = self.http_client.post(af_url, payload)
        dict_response = response.json()
        if response.status_code == 201 :
            return response.content
        elif response.status_code == 400 and "This schema already exists." in str(dict_response["Errors"]["Body"]["Id"]):
            self.logger.info(f"The schema {schema_id} already exists.")
        else:
            self.logger.info(response.status_code)

    def get_metrics(self, season_field_id: str,
                    schema_id: str,
                    start_date: Optional[datetime] = None,
                    end_date: Optional[datetime] = None):
        """Returns metrics from Analytics Fabrics in a pandas dataframe.
        Filters on date:
        if start_date is None: <= end_date
        if end_date is None: >= start_date
        if start_date & end_date not None:  between start_date & end_date
        if start_date & end_date both None: no filter on dates


        Args:
            season_field_id : A string representing the seasonfield id
            start_date : A datetime object representing the start date of the date interval the user wants to filter on.
            end_date : A datetime object representing the final date of the date interval the user wants to filter on.
            schema_id : A string representing a schema existing in Analytics Fabrics

        Returns:
            df : A Pandas DataFrame containing several columns with metrics

        """
        self.logger.info("Calling APIs for metrics")
        if start_date is not None:
            start_date: str = start_date.strftime("%Y-%m-%d")
        if end_date is not None:
            end_date: str = end_date.strftime("%Y-%m-%d")

        timestamp_params = self.build_timestamp_query_parameters(start_date, end_date);
        parameters: str = f'?%24filter=Entity.TypedId==\'SeasonField:{season_field_id}\'' \
                          f'{timestamp_params}' \
                          f'&Schema.Id={schema_id}' \
                          f'&%24limit=None'

        af_url: str = urljoin(
            self.base_url,
            GeosysApiEndpoints.ANALYTICS_FABRIC_ENDPOINT.value + parameters,
        )
        response = self.http_client.get(af_url)

        if response.status_code == 200:
            df = pd.json_normalize(response.json())
            if df.empty:
                if start_date is not None and end_date is not None:
                    date_msg =f"between:{start_date} and {end_date} "
                elif start_date is None and end_date is not None:
                    date_msg =f"<= {end_date} "
                elif start_date is not None and end_date is None:
                    date_msg = f">= {start_date} "
                self.logger.info(f"No metrics found in Analytic Fabric with "
                             f"SchemaId: {schema_id}, "
                             f"SeasonField:{season_field_id} "
                             f"{date_msg} ")
                return df
            df.drop("Entity.TypedId", inplace=True, axis=1)
            df.rename(
                columns={"Timestamp": "date"},
                inplace=True,
            )
            df = df.sort_values(by="date")
            df.set_index("date", inplace=True)
            return df
        else:
            self.logger.error("Issue in get_metrics. Status Code: "+str(response.status_code) +
                              " Error:" + str(response.json()))

    def get_lastest_metrics(self, season_field_id: str,
                            schema_id: str):
        """Returns latest metrics from Analytics Fabrics in a pandas dataframe.

        Args:
            season_field_id : A string representing the seasonfield id
            schema_id : A string representing a schema existing in Analytics Fabrics

        Returns:
            df : A Pandas DataFrame containing several columns with metrics

        """
        self.logger.info("Calling APIs for Latest metrics")

        parameters: str = f'?%24filter=Entity.TypedId==\'SeasonField:{season_field_id}\'' \
                          f'&Schema.Id={schema_id}' \
                          f'&%24limit=1' \
                          f'&$sort=-Timestamp'

        af_url: str = urljoin(
            self.base_url,
            GeosysApiEndpoints.ANALYTICS_FABRIC_LATEST_ENDPOINT.value + parameters,
        )
        response = self.http_client.get(af_url)

        if response.status_code == 200:
            df = pd.json_normalize(response.json())
            if df.empty:
                self.logger.info(f"No Latest metrics found in Analytic Fabric with "
                             f"SchemaId: {schema_id}, "
                             f"SeasonField:{season_field_id} ")
                return df
            df.drop("Entity.TypedId", inplace=True, axis=1)
            df.rename(
                columns={"Timestamp": "date"},
                inplace=True,
            )
            df = df.sort_values(by="date")
            df.set_index("date", inplace=True)
            return df
        else:
            self.logger.error("Issue in get_latests_metrics. Status Code: "+str(response.status_code)
                              + " Error:" + str(response.json()))

    def push_metrics(self, season_field_id: str,
                     schema_id: str,
                     values: dict):
        """Push metrics in Analytics Fabrics

        Args:
            season_field_id : A string representing the seasonFieldId.
            schema_id : The schema on which to save
            values : Dict representing values to push

        Returns:
            A response object.
        """
        payload = []
        for value in values:
            prop = {
                "Entity": {
                    "TypedId": f"SeasonField:{season_field_id}@ID"
                },
                "Schema": {"Id": schema_id, "Version": 1},
            }
            prop = dict(prop, **value)
            payload.append(prop)

        af_url: str = urljoin(
            self.base_url,
            GeosysApiEndpoints.ANALYTICS_FABRIC_ENDPOINT.value,
        )
        response = self.http_client.patch(af_url, payload)
        if response.status_code == 200:
            return response.status_code
        else:
            self.logger.error("Issue in push_metrics. Status Code: "+str(response.status_code)
                              + " Error:" + str(response.json()))
