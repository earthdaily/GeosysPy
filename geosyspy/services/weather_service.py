import logging
import pandas as pd
from datetime import datetime
from urllib.parse import urljoin

from shapely import wkt

from geosyspy.utils.constants import *
from geosyspy.utils.http_client import *


class WeatherService:

    def __init__(self, base_url: str, http_client: HttpClient):
        self.base_url: str = base_url
        self.http_client: HttpClient = http_client

    def get_weather(self, polygon: str,
                      start_date: datetime,
                      end_date: datetime,
                      weather_type: WeatherTypeCollection,
                      fields: [str]):
        """Returns the weather data as a pandas dataframe.

        Args:
            polygon : A string representing a polygon.
            start_date : A datetime object representing the start date of the date interval the user wants to filter on.
            end_date : A datetime object representing the final date of the date interval the user wants to filter on.
            weather_type : A list representing the weather type collection ["HISTORICAL_DAILY", "FORECAST_DAILY", "FORECAST_HOURLY"]
            fields : A list of strings representing the fields to select (eg: Precipitation, Temperature)

        Returns:
            The image's numpy array.

        """

        if weather_type not in WeatherTypeCollection:
            raise ValueError(f"weather_type should be either {[item.value for item in WeatherTypeCollection]}")
        weather_type = weather_type.value
        if "Date" not in fields:
            fields.append("Date")

        start_date: str = start_date.strftime("%Y-%m-%d")
        end_date: str = end_date.strftime("%Y-%m-%d")
        polygon_wkt = wkt.loads(polygon)
        weather_fields: str = ",".join(fields)
        parameters: str = f"?%24offset=0&%24limit=None&%24count=false&Location={polygon_wkt.centroid.wkt}&Date=%24between%3A{start_date}T00%3A00%3A00.0000000Z%7C{end_date}T00%3A00%3A00.0000000Z&Provider=GLOBAL1&WeatherType={weather_type}&$fields={weather_fields}"
        weather_url: str = urljoin(self.base_url, GeosysApiEndpoints.WEATHER_ENDPOINT.value + parameters)

        response = self.http_client.get(weather_url)

        if response.status_code == 200:
            df = pd.json_normalize(response.json())
            if df.empty:
                return df
            else:
                df.set_index("date", inplace=True)
                df["Location"] = polygon_wkt.centroid.wkt
                return df.sort_index()
        else:
            logging.error(response.status_code)
            raise ValueError(response.content)