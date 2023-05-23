from datetime import datetime
import json
from urllib.parse import urljoin
import pandas as pd
import logging
import io
import zipfile
from rasterio.io import MemoryFile
from requests import HTTPError
from shapely import wkt
from pathlib import Path
from geosyspy import image_reference
import xarray as xr
import rasterio
import numpy as np

from geosyspy.utils.helper import *
from geosyspy.utils.constants import *
from geosyspy.utils.http_client import *
from geosyspy.utils.geosys_platform_urls import *

class Geosys:
    def __init__(self, client_id: str,
                 client_secret: str,
                 username: str,
                 password: str,
                 enum_env: Env,
                 enum_region: Region,
                 priority_queue: str = "realtime",
                 ):
        """Initializes a Geosys instance with the required credentials
        to connect to the GEOSYS API.
        """
        self.region: str = enum_region.value
        self.env: str = enum_env.value
        self.base_url: str = GEOSYS_API_URLS[enum_region.value][enum_env.value]
        self.priority_queue: str = priority_queue
        self.http_client: HttpClient = HttpClient(client_id, client_secret, username, password, enum_env.value,
                                                  enum_region.value)

    """Geosys is the main client class to access all the Geosys APIs capabilities.

    `client = Geosys(api_client_id, api_client_secret, api_username, api_password, env, region)`

    Parameters:
        enum_env: 'Env.PROD' or 'Env.PREPROD'
        enum_region: 'Region.NA' or 'Region.EU'
        priority_queue: 'realtime' or 'bulk'
    """


    def __create_season_field_id(self, polygon: str) -> object:
        """Posts the payload below to the master data management endpoint.

        This method returns a season field id. The season field id is required
        to request other APIs endpoints.

        Args:
            polygon : A string representing a polygon.

        Returns:
            A response object.

        """
        payload = {
            "Geometry": polygon,
            "Crop": {"Id": "CORN"},
            "SowingDate": "2022-01-01",
        }
        mdm_url: str = urljoin(self.base_url, GeosysApiEndpoints.MASTER_DATA_MANAGEMENT_ENDPOINT.value)
        return self.http_client.post(mdm_url, payload)

    def __extract_season_field_id(self, polygon: str) -> str:
        """Extracts the season field id from the response object.

        Args:
            polygon : A string representing a polygon.

        Returns:
            A string rerpesenting the season field id.

        Raises:
            ValueError: The response status code is not as expected.
        """

        response = self.__create_season_field_id(polygon)
        dict_response = response.json()

        if (
                response.status_code == 400
                and "sowingDate" in dict_response["errors"]["body"]
        ):

            text: str = dict_response["errors"]["body"]["sowingDate"][0]["message"]
            return Helper.get_matched_str_from_pattern(SEASON_FIELD_ID_REGEX, text)

        elif response.status_code == 201:
            return dict_response["id"]
        else:
            raise ValueError(
                f"Cannot handle HTTP response : {str(response.status_code)} : {str(response.json())}"
            )

    def get_time_series(self, polygon: str,
                        start_date: datetime,
                        end_date: datetime,
                        collection: enumerate,
                        indicators: [str]) -> pd.DataFrame:
        """Retrieve a time series of the indicator for the aggregated polygon on the collection targeted.

        Args:
            polygon : The polygon
            start_date : The start date of the time series
            end_date : The end date of the time series
            collection : The collection targeted
            indicators : The indicators to retrieve on the collection

        Returns:
            (dataframe): A pandas dataframe for the time series

        Raises:
            ValueError: The collection doesn't exist
        """
        if collection in WeatherTypeCollection:
            return self.__get_weather(
                polygon,
                start_date,
                end_date,
                collection,
                indicators,
            )
        elif collection in LR_SATELLITE_COLLECTION:
            return self.__get_modis_time_series(
                polygon, start_date, end_date, indicators[0]
            )
        else:
            raise ValueError(f"{collection} collection doesn't exist")

    def get_satellite_image_time_series(self, polygon: str,
                                        start_date: datetime,
                                        end_date: datetime,
                                        collections: list[SatelliteImageryCollection],
                                        indicators: [str]
                                        ):
        """Retrieve a pixel-by-pixel time series of the indicator on the collection targeted.

        Args:
            polygon : The polygon
            start_date : The start date of the time series
            end_date : The end date of the time series
            collections : The Satellite Imagery Collection targeted
            indicators : The indicators to retrieve on the collections

        Returns:
            ('dataframe or xarray'): Either a pandas dataframe or a xarray for the time series
        """
        if not collections:
            raise ValueError(
                "The argument collections is empty. It must be a list of SatelliteImageryCollection objects"
            )
        elif all([isinstance(elem, SatelliteImageryCollection) for elem in collections]):
            if set(collections).issubset(set(LR_SATELLITE_COLLECTION)):
                return self.__get_time_series_by_pixel(
                    polygon, start_date, end_date, indicators[0]
                )
            elif set(collections).issubset(set(MR_SATELLITE_COLLECTION)):
                return self.__get_images_as_dataset(
                    polygon, start_date, end_date, collections, indicators[0]
                )
        else:
            raise TypeError(
                f"Argument collections must be a list of SatelliteImageryCollection objects"
            )

    def __get_modis_time_series(self, polygon: str,
                                start_date: datetime,
                                end_date: datetime,
                                indicator: str) -> pd.DataFrame:
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
            polygon : A string representing a polygon.
            start_date : A datetime object representing the start date of the date interval the user wants to filter on.
            end_date : A datetime object representing the final date of the date interval the user wants to filter on.
            indicator : A string representing the indicator whose time series the user wants.

        Returns:
            df : A Pandas DataFrame containing two columns : index and value, and an index called 'date'.

        """
        logging.info("Calling APIs for aggregated time series")
        season_field_id: str = self.__extract_season_field_id(polygon)
        start_date: str = start_date.strftime("%Y-%m-%d")
        end_date: str = end_date.strftime("%Y-%m-%d")
        parameters: str = f"/values?$offset=0&$limit=9999&$count=false&SeasonField.Id={season_field_id}&index={indicator}&$filter=Date >= '{start_date}' and Date <= '{end_date}'"
        vts_url: str = urljoin(self.base_url, GeosysApiEndpoints.VTS_ENDPOINT.value + parameters)
        response = self.http_client.get(vts_url)

        if response.status_code == 200:
            dict_response = response.json()
            df = pd.read_json(json.dumps(dict_response))
            df.set_index("date", inplace=True)
            return df
        else:
            logging.info(response.status_code)

    def __get_time_series_by_pixel(self, polygon: str,
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

        logging.info("Calling APIs for time series by the pixel")
        season_field_id: str = self.__extract_season_field_id(polygon)
        start_date: str = start_date.strftime("%Y-%m-%d")
        end_date: str = end_date.strftime("%Y-%m-%d")
        parameters: str = f"/values?$offset=0&$limit=9999&$count=false&SeasonField.Id={season_field_id}&index={indicator}&$filter=Date >= '{start_date}' and Date <= '{end_date}'"
        vts_url: str = urljoin(self.base_url, GeosysApiEndpoints.VTS_BY_PIXEL_ENDPOINT.value + parameters)
        # PSX/PSY : size in meters of one pixel
        # MODIS_GRID_LENGTH : theoretical length of the modis grid in meters
        # MODIS_GRID_HEIGHT : theoretical height of the modis grid in meters
        PSX = 231.65635826
        PSY = -231.65635826
        MODIS_GRID_LENGTH = 4800 * PSX * 36
        MODIS_GRID_HEIGHT = 4800 * PSY * 18

        response = self.http_client.get(vts_url)

        if response.status_code == 200:
            df = pd.json_normalize(response.json())
            df.set_index("date", inplace=True)

            # Extracts h, v, i and j from the pixel dataframe
            logging.info("Computing X and Y coordinates per pixel... ")
            df["h"] = df["pixel.id"].str.extract(r"h(.*)v").astype(int)
            df["v"] = df["pixel.id"].str.extract(r"v(.*)i").astype(int)
            df["i"] = df["pixel.id"].str.extract(r"i(.*)j").astype(int)
            df["j"] = df["pixel.id"].str.extract(r"j(.*)$").astype(int)

            # XUL/YUL : The coordinates of the top left corner of the tile h,v's top left pixel
            #  X/Y : the coordinates of the top left corner of the i,j pixel
            df["XUL"] = (df["h"] + 1) * 4800 * PSX - MODIS_GRID_LENGTH / 2
            df["YUL"] = (df["v"] + 1) * 4800 * PSY + MODIS_GRID_HEIGHT / 2
            df["X"] = df["i"] * PSX + df["XUL"]
            df["Y"] = df["j"] * PSY + df["YUL"]
            logging.info("Done ! ")
            return df[["index", "value", "pixel.id", "X", "Y"]]
        else:
            logging.info(response.status_code)

    def get_satellite_coverage_image_references(self, polygon: str,
                                                start_date: datetime,
                                                end_date: datetime,
                                                collections: list[SatelliteImageryCollection] = [
                                                    SatelliteImageryCollection.SENTINEL_2,
                                                    SatelliteImageryCollection.LANDSAT_8]
                                                ) -> tuple:
        """Retrieves a list of images that covers a polygon on a specific date range.
        The return is a tuple: a dataframe with all the images covering the polygon, and
                    a dictionary images_references. Key= a tuple (image_date, image_sensor).
                    Value = an object image_reference, to use with the method `download_image()`

        Args:
            polygon: The polygon
            start_date: The start date of the time series
            end_date: The end date of the time series
            collections: The sensors to check the coverage on

        Returns:
            (tuple): images list and image references for downloading
        """

        df = self.__get_satellite_coverage(polygon, start_date, end_date, "", collections)
        images_references = {}

        for i, image in df.iterrows():
            images_references[
                (image["image.date"], image["image.sensor"])
            ] = image_reference.ImageReference(
                image["image.id"],
                image["image.date"],
                image["image.sensor"],
                image["seasonField.id"],
            )

        return df, images_references

    def __get_satellite_coverage(self, polygon: str,
                                 start_date: datetime,
                                 end_date: datetime,
                                 indicator,
                                 sensors_collection: list[SatelliteImageryCollection] = [
                                     SatelliteImageryCollection.SENTINEL_2,
                                     SatelliteImageryCollection.LANDSAT_8]
                                 ):
        logging.info("Calling APIs for coverage")
        season_field_id: str = self.__extract_season_field_id(polygon)
        start_date: str = start_date.strftime("%Y-%m-%d")
        end_date: str = end_date.strftime("%Y-%m-%d")
        sensors: list[str] = [elem.value for elem in sensors_collection]

        if indicator == "" or indicator.upper() == "REFLECTANCE" or indicator.upper() == "NDVI":
            mapType = "INSEASON_NDVI"
        else:
            mapType = f"INSEASON_{indicator.upper()}"

        parameters = f"?maps.type={mapType}&Image.Sensor=$in:{'|'.join(sensors)}&CoverageType=CLEAR&$limit=9999&$filter=Image.Date >= '{start_date}' and Image.Date <= '{end_date}'"

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
            logging.info(response.status_code)

    def __get_zipped_tiff(self, field_id: str,
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

    def download_image(self, image_reference,
                       path: str = ""):
        """Downloads a satellite image locally

        Args:
            image_reference (ImageReference): An ImageReference object representing the image to download
            path (str): the path to download the image to
        """

        response_zipped_tiff = self.__get_zipped_tiff(
            image_reference.season_field_id, image_reference.image_id
        )
        if path == "":
            path = Path.cwd() / f"image_{image_reference.image_id}_tiff.zip"
        with open(path, "wb") as f:
            logging.info(f"writing to {path}")
            f.write(response_zipped_tiff.content)

    def __get_images_as_dataset(self, polygon: str,
                                start_date: datetime,
                                end_date: datetime,
                                collections: list[SatelliteImageryCollection],
                                indicator: str) -> 'np.ndarray[Any , np.dtype[np.float64]]':
        """Returns all the 'sensors_list' images covering 'polygon' between
        'start_date' and 'end_date' as a xarray dataset.

        Args:
            polygon : A string representing the polygon that the images will be covering.
            start_date : The date from which the method will start looking for images.
            end_date : The date at which the method will stop looking images.
            collections : A list of Satellite Imagery Collection.
            indicator : A string representing the indicator whose time series the user wants.

        Returns:
            The image's numpy array.

        """

        def get_coordinates_by_pixel(raster):
            """Returns the coordinates in meters in the raster's CRS
            from its pixels' grid coordinates."""

            img = raster.read()
            band1 = img[0]
            height = band1.shape[0]
            width = band1.shape[1]
            cols, rows = np.meshgrid(np.arange(width), np.arange(height))
            xs, ys = rasterio.transform.xy(raster.transform, rows, cols)
            lons = np.array(xs)
            lats = np.array(ys)
            lst_lats = [lat[0] for lat in lats]
            lst_lons = list(lons[0])
            return {"y": lst_lats, "x": lst_lons}

        # Selects the covering images in the provided date range
        # and sorts them by resolution, from the highest to the lowest.
        # Keeps only the first image if two are found on the same date.
        df_coverage = self.__get_satellite_coverage(
            polygon, start_date, end_date, indicator, collections
        )

        # Return empty dataset if no coverage on the polygon between start_date, end_date
        if df_coverage.empty:
            return xr.Dataset()

        df_coverage["image.date"] = pd.to_datetime(
            df_coverage["image.date"], infer_datetime_format=True
        )
        df_coverage = df_coverage.sort_values(
            by=["image.spatialResolution", "image.date"], ascending=[True, True]
        ).drop_duplicates(subset="image.date", keep="first")

        # Creates a dictionary that contains a zip archive containing the tif file
        # for each image id and some additional data (bands, sensor...)
        dict_archives = {}
        for i, row in df_coverage.iterrows():
            if indicator.upper() != "REFLECTANCE":
                bands = [indicator]
            else:
                bands = row["image.availableBands"]
            dict_archives[row["image.id"]] = {
                "byte_archive": self.__get_zipped_tiff(
                    row["seasonField.id"], row["image.id"], indicator
                ).content,
                "bands": bands,
                "date": row["image.date"],
                "sensor": row["image.sensor"],
            }

        # Extracts the tif files from  the zip archives in memory
        # and transforms them into a list of xarray DataArrays.
        # A list of all the raster's crs is also created in order
        # to merge this data in the final xarray Dataset later on.
        list_xarr = []
        list_crs = []
        first_img_id = df_coverage.iloc[0]["image.id"]
        for img_id, dict_data in dict_archives.items():
            with zipfile.ZipFile(io.BytesIO(dict_data["byte_archive"]), "r") as archive:
                images_in_bytes = [archive.read(file) for file in archive.namelist() if file.endswith('.tif')]
                for image in images_in_bytes:
                    with MemoryFile(image) as memfile:
                        with memfile.open() as raster:
                            dict_coords = get_coordinates_by_pixel(raster)
                            xarr = xr.DataArray(
                                raster.read(masked=True),
                                dims=["band", "y", "x"],
                                coords={
                                    "band": dict_data["bands"],
                                    "y": dict_coords["y"],
                                    "x": dict_coords["x"],
                                    "time": dict_data["date"],
                                },
                            )

                            if img_id == first_img_id:
                                len_y = len(dict_coords["y"])
                                len_x = len(dict_coords["x"])
                                print(
                                    f"The highest resolution's image grid size is {(len_x, len_y)}"
                                )
                            else:
                                logging.info(
                                    f"interpolating {img_id} to {first_img_id}'s grid"
                                )
                                xarr = xarr.interp(
                                    x=list_xarr[0].coords["x"].data,
                                    y=list_xarr[0].coords["y"].data,
                                    method="linear",
                                )
                            list_xarr.append(xarr)
                            list_crs.append(raster.crs.to_string())

        # Adds the img's raster's crs to the initial dataframe
        df_coverage["crs"] = list_crs

        # Concatenates all the DataArrays in list_xarr in order
        # to create one final DataArray with an additional dimension
        # 'time'. This final DataArray is then transformed into
        # a xarray Dataset containing one data variable "reflectance".

        final_xarr = xr.concat(list_xarr, "time")
        dataset = xr.Dataset(data_vars={indicator.lower(): final_xarr})

        # Adds additional metadata to the dataset.
        dataset = dataset.assign_coords(
            **{
                k: ("time", np.array(v))
                for k, v in df_coverage[
                    [
                        "image.id",
                        "image.sensor",
                        "image.soilMaterial",
                        "image.spatialResolution",
                        "image.weather",
                        "crs",
                    ]
                ]
                .to_dict(orient="list")
                .items()
            }
        )
        return dataset

    def __get_weather(self, polygon: str,
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
        parameters: str = f"?%24offset=0&%24limit=9999&%24count=false&Location={polygon_wkt.centroid.wkt}&Date=%24between%3A{start_date}T00%3A00%3A00.0000000Z%7C{end_date}T00%3A00%3A00.0000000Z&Provider=GLOBAL1&WeatherType={weather_type}&$fields={weather_fields}"
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
        if response.status_code == 201:
            return response.content
        else:
            logging.info(response.status_code)

    def _get_s3_path(self, task_id: str):

        endpoint: str = GeosysApiEndpoints.MRTS_PROCESSOR_EVENTS_ENDPOINT.value + "/" + task_id
        response = self.http_client.get(endpoint)
        if response.ok:
            dict_resp = json.loads(response.content)
            customer_code: str = dict_resp["customerCode"].lower().replace("_", "-")
            user_id: str = dict_resp["userId"]
            task_id = dict_resp["taskId"]
            return "s3://geosys-" + customer_code + "/" + user_id + "/mrts/" + task_id
        else:
            logging.info(response.status_code)

    def get_mr_time_series(self, start_date: str, end_date: str, list_sensors, denoiser: bool, smoother: str, eoc: bool,
                           func: str, index: str, raw_data: bool, polygon: str):
        payload = {
            "parametersProfile": {
                "code": "mrts_default",
                "version": 1
            },
            "parameters": {
                "start_date": start_date,
                "end_date": end_date,
                "sensors": list_sensors,
                "denoiser": denoiser,
                "smoother": smoother,
                "eoc": eoc,
                "aggregation": func,
                "index": index,
                "raw_data": raw_data
            },
            "data": [
                {"wkt": polygon}
            ]
        }

        response = self.http_client.post(GeosysApiEndpoints.MRTS_PROCESSOR_ENDPOINT.value, payload)

        if response.ok:
            task_id = json.loads(response.content)["taskId"]
            return self._get_s3_path(task_id)
        else:
            logging.info(response.status_code)

    def get_metrics(self, polygon: str,
                    schema_id: str,
                    start_date: datetime,
                    end_date: datetime):
        """Returns metrics from Analytics Fabrics in a pandas dataframe.

        Args:
            polygon : A string representing a polygon.
            start_date : A datetime object representing the start date of the date interval the user wants to filter on.
            end_date : A datetime object representing the final date of the date interval the user wants to filter on.
            schema_id : A string representing a schema existing in Analytics Fabrics

        Returns:
            df : A Pandas DataFrame containing several's columns with metrics

        """
        season_field_id: str = self.__extract_season_field_id(polygon)
        logging.info("Calling APIs for metrics")
        start_date: str = start_date.strftime("%Y-%m-%d")
        end_date: str = end_date.strftime("%Y-%m-%d")
        parameters: str = f'?%24limit=9999&Timestamp=$between:{start_date}|{end_date}&$filter=Entity.ExternalTypedIds.Contains("SeasonField:{season_field_id}@LEGACY_ID_{self.region.upper()}")&$filter=Schema.Id=={schema_id}'
        af_url: str = urljoin(
            self.base_url,
            GeosysApiEndpoints.ANALYTICS_FABRIC_ENDPOINT.value + parameters,
        )
        response = self.http_client.get(af_url)

        if response.status_code == 200:
            df = pd.json_normalize(response.json())
            if df.empty:
                logging.info(f"No metrics found in Analytic Fabric with "
                             f"SchemaId: {schema_id}, "
                             f"SeasonField:{season_field_id}@LEGACY_ID_{self.region.upper()} "
                             f"between:{start_date} and{end_date} ")
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
            logging.info(response.status_code)

    def push_metrics(self, polygon: str,
                     schema_id: str,
                     values: dict):
        """Push metrics in Analytics Fabrics

        Args:
            polygon : A string representing the polygon.
            schema_id : The schema on which to save
            values : Dict representing values to push

        Returns:
            A response object.
        """
        season_field_id: str = self.__extract_season_field_id(polygon)
        payload = []
        for value in values:
            prop = {
                "Entity": {
                    "TypedId": f"SeasonField:{season_field_id}@LEGACY_ID_{self.region.upper()}"
                },
                "Schema": {"Id": schema_id, "Version": 1},
            }
            prop = dict(prop, **value)
            payload.append(prop)

        af_url: str = urljoin(
            self.base_url,
            GeosysApiEndpoints.ANALYTICS_FABRIC_SCHEMA_ENDPOINT.value,
        )
        response = self.http_client.patch(af_url, payload)
        if response.status_code == 200:
            return response.status_code
        else:
            logging.info(response.status_code)
