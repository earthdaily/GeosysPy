import json
import logging
import pandas as pd
from datetime import datetime
from urllib.parse import urljoin
from geosyspy.utils.constants import *
from geosyspy.utils.http_client import *


class VegetationTimeSeriesService:

    def __init__(self, base_url: str, http_client: HttpClient):
        self.base_url: str = base_url
        self.http_client: HttpClient = http_client
        self.logger = logging.getLogger(__name__)

    def get_modis_time_series(self, season_field_id:str,
                              start_date:datetime,
                              end_date:datetime,
                              indicator:str):
        """Returns a pandas DataFrame.

        This method returns a time series of 'indicator' within the range
        'start_date' -> 'end_date' as a pandas DataFrame :

                     | index     | value |

            date     | indicator |   1   |
        __________________________________
         start-date  | indicator |   2   |

            ...      |    ...    |  ...  |

         end-date    | indicator |   8   |

        Args:
            season_field_id : A string representing the season_field_id.
            start_date : A datetime object representing the start date of the date interval the user wants to filter on.
            end_date : A datetime object representing the final date of the date interval the user wants to filter on.
            indicator : A string representing the indicator whose time series the user wants.

        Returns:
            df : A Pandas DataFrame containing two columns : index and value, and an index called 'date'.

        """

        self.logger.info("Calling APIs for aggregated time series")
        start_date: str = start_date.strftime("%Y-%m-%d")
        end_date: str = end_date.strftime("%Y-%m-%d")
        parameters: str = f"/values?$offset=0&$limit=None&$count=false&SeasonField.Id={season_field_id}&index={indicator}&$filter=Date >= '{start_date}' and Date <= '{end_date}'"
        vts_url: str = urljoin(self.base_url, GeosysApiEndpoints.VTS_ENDPOINT.value + parameters)
        response = self.http_client.get(vts_url)

        if response.status_code == 200:
            dict_response = response.json()
            df = pd.read_json(json.dumps(dict_response))
            df.set_index("date", inplace=True)
            return df
        else:
            self.logger.info(response.status_code)


    def get_time_series_by_pixel(self, season_field_id: str,
                                   start_date: datetime,
                                   end_date: datetime,
                                   indicator: str) -> pd.DataFrame:
        """Returns a pandas DataFrame.

            This method returns a time series of 'indicator' by pixel within the range 'start_date' -> 'end_date'
            as well as the pixel's coordinates X,Y in the MODIS's sinusoidal projection as a pandas DataFrame :



                            | index     | value | pixel.id | X | Y |
                            _______________________________________|
                date        | indicator |       |          |   |   |
                ___________________________________________________|

                start-date  | indicator |   2   |    1     |   |   |

                ...         | ...       |  ...  |   ...    |   |   |

                end-date    | indicator |   8   |   1000   |   |   |

            Args:
                polygon : A string representing a polygon.
                start_date : A datetime object representing the start date of the date interval the user wants to filter on.
                end_date : A datetime object representing the final date of the date interval the user wants to filter on.
                indicator : A string representing the indicator whose time series the user wants.

            Returns:
                df : A Pandas DataFrame containing five columns : index, value, pixel.id, X, Y and an index called 'date'.

            """

        self.logger.info("Calling APIs for time series by the pixel")
        start_date: str = start_date.strftime("%Y-%m-%d")
        end_date: str = end_date.strftime("%Y-%m-%d")
        parameters: str = f"/values?$offset=0&$limit=None&$count=false&SeasonField.Id={season_field_id}&index={indicator}&$filter=Date >= '{start_date}' and Date <= '{end_date}'"
        vts_url: str = urljoin(self.base_url, GeosysApiEndpoints.VTS_BY_PIXEL_ENDPOINT.value + parameters)
        response = self.http_client.get(vts_url)

        if response.status_code == 200:
            return self._extracted_from_get_time_series_by_pixel_50(response)
        else:
            self.logger.info(response.status_code)

    # TODO Rename this here and in `get_time_series_by_pixel`
    def _extracted_from_get_time_series_by_pixel_50(self, response):
        df = pd.json_normalize(response.json())
        df.set_index("date", inplace=True)

        # Extracts h, v, i and j from the pixel dataframe
        self.logger.info("Computing X and Y coordinates per pixel... ")
        df["h"] = df["pixel.id"].str.extract(r"h(.*)v").astype(int)
        df["v"] = df["pixel.id"].str.extract(r"v(.*)i").astype(int)
        df["i"] = df["pixel.id"].str.extract(r"i(.*)j").astype(int)
        df["j"] = df["pixel.id"].str.extract(r"j(.*)$").astype(int)

        # PSX/PSY : size in meters of one pixel
        # MODIS_GRID_LENGTH : theoretical length of the modis grid in meters
        # MODIS_GRID_HEIGHT : theoretical height of the modis grid in meters
        PSX = 231.65635826
        MODIS_GRID_LENGTH = 4800 * PSX * 36
        # XUL/YUL : The coordinates of the top left corner of the tile h,v's top left pixel
        #  X/Y : the coordinates of the top left corner of the i,j pixel
        df["XUL"] = (df["h"] + 1) * 4800 * PSX - MODIS_GRID_LENGTH / 2
        PSY = -231.65635826
        MODIS_GRID_HEIGHT = 4800 * PSY * 18

        df["YUL"] = (df["v"] + 1) * 4800 * PSY + MODIS_GRID_HEIGHT / 2
        df["X"] = df["i"] * PSX + df["XUL"]
        df["Y"] = df["j"] * PSY + df["YUL"]
        self.logger.info("Done ! ")
        return df[["index", "value", "pixel.id", "X", "Y"]]