import io
import zipfile
from pathlib import Path

import numpy as np
import rasterio
import xarray as xr
from rasterio.io import MemoryFile

from geosyspy import image_reference
from geosyspy.services.agriquest_service import *
from geosyspy.services.analytics_fabric_service import *
from geosyspy.services.analytics_processor_service import *
from geosyspy.services.gis_service import *
from geosyspy.services.map_product_service import *
from geosyspy.services.master_data_management_service import *
from geosyspy.services.vegetation_time_series_service import *
from geosyspy.services.weather_service import *
from geosyspy.utils.geosys_platform_urls import *


class Geosys:
    """Geosys is the main client class to access all the Geosys APIs capabilities.

    `client = Geosys(api_client_id, api_client_secret, api_username, api_password, env, region)`

    Parameters:
        enum_env: 'Env.PROD' or 'Env.PREPROD'
        enum_region: 'Region.NA' or 'Region.EU'
        priority_queue: 'realtime' or 'bulk'
    """

    def __init__(self, client_id: str,
                 client_secret: str,
                 username: str,
                 password: str,
                 enum_env: Env,
                 enum_region: Region,
                 priority_queue: str = "realtime",
                 ):
        self.region: str = enum_region.value
        self.env: str = enum_env.value
        self.base_url: str = GEOSYS_API_URLS[enum_region.value][enum_env.value]
        self.gis_url: str = GIS_API_URLS[enum_region.value][enum_env.value]
        self.priority_queue: str = priority_queue
        self.http_client: HttpClient = HttpClient(client_id, client_secret, username, password, enum_env.value,
                                                  enum_region.value)
        self.__master_data_management_service = MasterDataManagementService(self.base_url, self.http_client)
        self.__analytics_fabric_service = AnalyticsFabricService(self.base_url, self.http_client)
        self.__analytics_processor_service = AnalyticsProcessorService(self.base_url, self.http_client)
        self.__agriquest_service = AgriquestService(self.base_url, self.http_client)
        self.__weather_service = WeatherService(self.base_url, self.http_client)
        self.__gis_service = GisService(self.gis_url, self.http_client)
        self.__vts_service = VegetationTimeSeriesService(self.base_url, self.http_client)
        self.__map_product_service = MapProductService(self.base_url, self.http_client, self.priority_queue)

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
            return self.__weather_service.get_weather(
                polygon,
                start_date,
                end_date,
                collection,
                indicators,
            )
        elif collection in LR_SATELLITE_COLLECTION:
            # extract seasonfield id from geometry
            season_field_id: str = self.__master_data_management_service.extract_season_field_id(polygon)
            return self.__vts_service.get_modis_time_series(
                season_field_id, start_date, end_date, indicators[0]
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
            # extract seasonfield id from geometry
            season_field_id: str = self.__master_data_management_service.extract_season_field_id(polygon)

            if set(collections).issubset(set(LR_SATELLITE_COLLECTION)):
                return self.__vts_service.get_time_series_by_pixel(
                    season_field_id, start_date, end_date, indicators[0]
                )
            elif set(collections).issubset(set(MR_SATELLITE_COLLECTION)):
                return self.__get_images_as_dataset(
                    season_field_id, start_date, end_date, collections, indicators[0]
                )
        else:
            raise TypeError(
                f"Argument collections must be a list of SatelliteImageryCollection objects"
            )

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
        # extract seasonfield id from geometry
        season_field_id: str = self.__master_data_management_service.extract_season_field_id(polygon)

        df = self.__map_product_service.get_satellite_coverage(season_field_id, start_date, end_date, "", collections)
        images_references = {}
        if df is not None:
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

    def download_image(self, image_reference,
                       path: str = ""):
        """Downloads a satellite image locally

        Args:
            image_reference (ImageReference): An ImageReference object representing the image to download
            path (str): the path to download the image to
        """

        response_zipped_tiff = self.__map_product_service.get_zipped_tiff(
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
        df_coverage = self.__map_product_service.get_satellite_coverage(
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
                "byte_archive": self.__map_product_service.get_zipped_tiff(
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

    ###########################################
    #           ANALYTICS FABRIC              #
    ###########################################

    def create_schema_id(self, schema_id: str,
                         schema: dict):
        """Create a schema in Analytics Fabrics

        Args:
            schema_id: The schema id to create
            schema: Dict representing the schema {'property_name': 'property_type'}

        Returns:
            A http response object.
        """
        return self.__analytics_fabric_service.create_schema_id(schema_id=schema_id, schema=schema)

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
            df : A Pandas DataFrame containing severals columns with metrics

        """

        season_field_id: str = self.__master_data_management_service.extract_season_field_id(polygon)
        season_field_unique_id: str = self.__master_data_management_service.get_season_field_unique_id(season_field_id)

        return self.__analytics_fabric_service.get_metrics(season_field_unique_id, schema_id, start_date, end_date)

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
        season_field_id: str = self.__master_data_management_service.extract_season_field_id(polygon)

        return self.__analytics_fabric_service.push_metrics(season_field_id, schema_id, values)

    ###########################################
    #           MASTER DATA MANAGEMENT        #
    ###########################################

    def get_available_crops(self):
        """Build the list of available crop codes for the connected user in an enum

        Returns:
            crop_enum: an Enum containing all available crop codes of the connected user
        """
        # get crop code list
        result = self.__master_data_management_service.get_available_crops_code()

        # build an enum with all available crop codes for the connected user
        crop_enum = Enum('CropEnum',
                         {crop['code'] if not crop['code'][0].isdigit() else '_' + crop['code']: crop['code'] for crop
                          in result})

        return crop_enum

    ###########################################
    #           AGRIQUEST                     #
    ###########################################
    def get_agriquest_weather_block_data(self,
                                         start_date: str,
                                         end_date: str,
                                         block_code: AgriquestBlocks,
                                         weather_type: AgriquestWeatherType
                                         ):
        """Retrieve data on all AMU of an AgriquestBlock for the specified weather indicator.

               Args:
                   start_date (str): The start date to retrieve data (format: 'YYYY-MM-dd')
                   end_date (str): The end date to retrieve data (format: 'YYYY-MM-dd')
                   block_code (AgriquestBlocks): The AgriquestBlock name (Enum)
                   weather_type (AgriquestWeatherType) : The Agriquest weather indicator to retrieve (Enum)

               Returns:
                   result ('dataframe'):  pandas dataframe
               """
        # date convert
        start_datetime = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_datetime = datetime.strptime(end_date, "%Y-%m-%d").date()

        # check if the block is dedicated to France
        isFrance = self.__agriquest_service.is_block_for_france(block_code)

        # build the weather indicator list
        weather_indicators = self.__agriquest_service.weather_indicators_builder(start_datetime, end_datetime, isFrance)

        # call the weather endpoint to retrieve data
        result = self.__agriquest_service.get_agriquest_block_weather_data(start_date=start_date, end_date=end_date,
                                                                           block_code=block_code,
                                                                           indicator_list=weather_indicators,
                                                                           weather_type=weather_type)

        return result

    def get_agriquest_ndvi_block_data(self,
                                      day_of_measure: str,
                                      block_code: AgriquestBlocks,
                                      commodity_code: AgriquestCommodityCode
                                      ):
        """Retrieve data on all AMU of an AgriquestBlock for NDVI index

               Args:
                   day_of_measure (str) : The date of measure (format: 'YYYY-MM-dd')
                   block_code (AgriquestBlocks) : The AgriquestBlock name (Enum)
                   commodity_code (AgriquestCommodityCode) : The commodity code (Enum)
               Returns:
                   result ('dataframe'):  pandas dataframe result
               """

        # call the weather endpoint to retrieve data, indicator of NDVI = 1
        result = self.__agriquest_service.get_agriquest_block_ndvi_data(date=day_of_measure, block_code=block_code,
                                                                        commodity=commodity_code, indicator_list=[1])

        return result

    ###########################################
    #           ANALYTICS PROCESSOR           #
    ###########################################

    def get_mr_time_series(self,
                           polygon,
                           start_date: str = "2010-01-01",
                           end_date=None,
                           list_sensors=["micasense", "sequoia", "m4c", "sentinel_2",
                                         "landsat_8", "landsat_9", "cbers4", "kazstsat",
                                         "alsat_1b", "huanjing_2", "deimos", "gaofen_1", "gaofen_6",
                                         "resourcesat2", "dmc_2", "landsat_5", "landsat_7",
                                         "spot", "rapideye_3a", "rapideye_1b"],
                           denoiser: bool = True,
                           smoother: str = "ww",
                           eoc: bool = True,
                           aggregation: str = "mean",
                           index: str = "ndvi",
                           raw_data: bool = False
                           ):

        """Retrieve mr time series on the collection targeted.

        Args:
            start_date : The start date of the time series
            end_date : The end date of the time series
            list_sensors : The Satellite Imagery Collection targeted
            denoiser : A boolean value indicating whether a denoising operation should be applied or not.
            smoother : The type or name of the smoothing technique or algorithm to be used.
            eoc : A boolean value indicating whether the "end of curve" detection should be performed.
            func : The type or name of the function to be applied to the data.
            index : The type or name of the index used for data manipulation or referencing
            raw_data : A boolean value indicating whether the data is in its raw/unprocessed form.
            polygon : A string representing a polygon.

        Returns:
            string : s3 bucket path
        """
        task_id = self.__analytics_processor_service.launch_mr_time_series_processor(
            start_date=start_date,
            end_date=end_date,
            polygon=polygon,
            raw_data=raw_data,
            denoiser=denoiser,
            smoother=smoother,
            aggregation=aggregation,
            list_sensors=list_sensors,
            index=index,
            eoc=eoc,


        )

        # check the task status to continue or not the process
        self.__analytics_processor_service.wait_and_check_task_status(task_id)

        return self.__analytics_processor_service.get_s3_path_from_task_and_processor(task_id, processor_name="mrts")

    def get_harvest_analytics(self,
                              season_duration: int,
                              season_start_day: int,
                              season_start_month: int,
                              crop: Enum,
                              year: int,
                              geometry: str,
                              harvest_type: Harvest):
        """launch a harvest analytics processor and get the metrics in a panda dataframe object

            Args:
                season_duration (int): the duration of the season in days,
                season_start_day (int): the start day value (1 - 31),
                season_start_month (int): the start month value (1 - 12),
                crop (Enum): the geosys crop code,
                year (int): the year value,
                geometry (str): the geometry to calculate the analytic (WKT or GeoJSON),
                harvest_type (Harvest): the type of Harvest analytics (INSEASON/HISTORICAL)

            Returns:
                A Pandas DataFrame containing several columns with metrics
        """
        # validate and convert the geometry to WKT
        geometry = Helper.convert_to_wkt(geometry)

        if geometry is None:
            raise ValueError("The geometry is not a valid WKT of GeoJson")

        # Create seasonfield from geometry and extract uniqueId
        sfd_public_id = self.__master_data_management_service.extract_season_field_id(geometry)
        sf_unique_id = self.__master_data_management_service.get_season_field_unique_id(sfd_public_id)

        task_id = self.__analytics_processor_service.launch_harvest_processor(
            season_duration=season_duration,
            season_start_day=season_start_day,
            season_start_month=season_start_month,
            seasonfield_id=sf_unique_id,
            geometry=geometry,
            crop=crop.value,
            year=year,
            harvest_type=harvest_type
        )

        logging.info(f"Task Id: {task_id}")

        # check the task status to continue or not the process
        self.__analytics_processor_service.wait_and_check_task_status(task_id)

        # Analytics Schema
        if harvest_type == Harvest.HARVEST_IN_SEASON:
            schema = "INSEASON_HARVEST"
        else:
            schema = "HISTORICAL_HARVEST"

        # if task successfully completed, get metrics from analytics fabric
        return self.__analytics_fabric_service.get_lastest_metrics(sf_unique_id, schema)

    def get_emergence_analytics(self,
                                season_duration: int,
                                season_start_day: int,
                                season_start_month: int,
                                crop: Enum,
                                year: int,
                                geometry: str,
                                emergence_type: Emergence):
        """launch an emergence analytics processor and get the metrics in a panda dataframe object

            Args:
                season_duration (int): the duration of the season in days,
                season_start_day (int): the start day value (1 - 31),
                season_start_month (int): the start month value (1 - 12),
                crop (Enum): the crop code,
                year (int): the year value,
                geometry (str): the geometry to calculate the analytic (WKT or GeoJSON),
                emergence_type (Emergence): the type of Emergence analytics (INSEASON/HISTORICAL/DELAY)

            Returns:
                A Pandas DataFrame containing several columns with metrics
        """
        # validate and convert the geometry to WKT
        geometry = Helper.convert_to_wkt(geometry)

        if geometry is None:
            raise ValueError("The geometry is not a valid WKT of GeoJson")

        # Create seasonfield from geometry and extract uniqueId
        sfd_public_id = self.__master_data_management_service.extract_season_field_id(geometry)
        sf_unique_id = self.__master_data_management_service.get_season_field_unique_id(sfd_public_id)

        task_id = self.__analytics_processor_service.launch_emergence_processor(
            season_duration=season_duration,
            season_start_day=season_start_day,
            season_start_month=season_start_month,
            seasonfield_id=sf_unique_id,
            geometry=geometry,
            crop=crop.value,
            year=year,
            emergence_type=emergence_type
        )

        # check the task status to continue or not the process
        self.__analytics_processor_service.wait_and_check_task_status(task_id)

        # Analytics Schema
        if emergence_type == Emergence.EMERGENCE_IN_SEASON:
            schema = "INSEASON_EMERGENCE"
        elif emergence_type == Emergence.EMERGENCE_HISTORICAL:
            schema = "HISTORICAL_EMERGENCE"
        else:
            schema = "EMERGENCE_DELAY"

        # if task successfully completed, get metrics from analytics fabric
        return self.__analytics_fabric_service.get_lastest_metrics(sf_unique_id, schema)

    def get_brazil_crop_id_analytics(self,
                                     start_date: str,
                                     end_date: str,
                                     season: CropIdSeason,
                                     geometry: str):
        """launch a brazil-in-season-crop-id analytics processor and get the metrics in a panda dataframe object

            Args:
                start_date (str) : the start date used for the request (format YYYY-MM-dd)
                end_date (str) : the end date used for the request (format YYYY-MM-dd)
                season (CropIdSeason): the season name,
                geometry (str): the geometry to calculate the analytic (WKT or GeoJSON)

            Returns:
                A Pandas DataFrame containing several columns with metrics
        """
        # validate and convert the geometry to WKT
        geometry = Helper.convert_to_wkt(geometry)

        if geometry is None:
            raise ValueError("The geometry is not a valid WKT of GeoJson")

        # Create seasonfield from geometry and extract uniqueId
        sfd_public_id = self.__master_data_management_service.extract_season_field_id(geometry)
        sf_unique_id = self.__master_data_management_service.get_season_field_unique_id(sfd_public_id)

        task_id = self.__analytics_processor_service.launch_brazil_in_season_crop_id_processor(
            start_date=start_date,
            end_date=end_date,
            seasonfield_id=sf_unique_id,
            geometry=geometry,
            season=season.value
        )

        # check the task status to continue or not the process
        self.__analytics_processor_service.wait_and_check_task_status(task_id)

        # Analytics Schema
        schema = "CROP_IDENTIFICATION"

        # if task successfully completed, get metrics from analytics fabric
        return self.__analytics_fabric_service.get_lastest_metrics(sf_unique_id, schema)

    def get_potential_score_analytics(self,
                                      end_date: str,
                                      nb_historical_years: int,
                                      season_duration: int,
                                      season_start_day: int,
                                      season_start_month: int,
                                      sowing_date: str,
                                      crop: Enum,
                                      geometry: str):
        """launch a potential score analytics processor and get the metrics in a panda dataframe object

            Args:
                season_duration (int): the duration of the season in days,
                season_start_day (int): the start day value (1 - 31),
                season_start_month (int): the start month value (1 - 12),
                crop (Enum): the crop code,
                end_date (str): end date used to calculate potential score
                sowing_date (str): sowing date of the filed used to calculate potential score
                nb_historical_years (int): number of historical years data to calculate potential score
                geometry (str): the geometry to calculate the analytic (WKT or GeoJSON)

            Returns:
                A Pandas DataFrame containing several columns with metrics
        """
        # validate and convert the geometry to WKT
        geometry = Helper.convert_to_wkt(geometry)

        if geometry is None:
            raise ValueError("The geometry is not a valid WKT of GeoJson")

        # Create seasonfield from geometry and extract uniqueId
        sfd_public_id = self.__master_data_management_service.extract_season_field_id(geometry)
        sf_unique_id = self.__master_data_management_service.get_season_field_unique_id(sfd_public_id)

        task_id = self.__analytics_processor_service.launch_potential_score_processor(
            end_date=end_date,
            nb_historical_years=nb_historical_years,
            sowing_date=sowing_date,
            season_duration=season_duration,
            season_start_day=season_start_day,
            season_start_month=season_start_month,
            seasonfield_id=sf_unique_id,
            geometry=geometry,
            crop=crop.value
        )

        # check the task status to continue or not the process
        self.__analytics_processor_service.wait_and_check_task_status(task_id)

        # Analytics Schema
        schema = "POTENTIAL_SCORE"

        # if task successfully completed, get metrics from analytics fabric
        return self.__analytics_fabric_service.get_lastest_metrics(sf_unique_id, schema)

    def get_greenness_analytics(self,
                                start_date: str,
                                end_date: str,
                                sowing_date: str,
                                crop: Enum,
                                geometry: str):
        """launch a greenness analytics processor and get the metrics in a panda dataframe object

                    Args:
                        start_date (str) : the start date used for the request (format YYYY-MM-dd)
                        end_date (str) : the end date used for the request (format YYYY-MM-dd)
                        sowing_date(str): sowing date of the field used to calculate potential score
                        crop (Enum): the crop code,
                        geometry (str): the geometry to calculate the analytic (WKT or GeoJSON)

                    Returns:
                        A Pandas DataFrame containing several columns with metrics
        """
        # validate and convert the geometry to WKT
        geometry = Helper.convert_to_wkt(geometry)

        if geometry is None:
            raise ValueError("The geometry is not a valid WKT of GeoJson")

        # Create seasonfield from geometry and extract uniqueId
        sfd_public_id = self.__master_data_management_service.extract_season_field_id(geometry)
        sf_unique_id = self.__master_data_management_service.get_season_field_unique_id(sfd_public_id)

        task_id = self.__analytics_processor_service.launch_greenness_processor(
            start_date=start_date,
            end_date=end_date,
            sowing_date=sowing_date,
            seasonfield_id=sf_unique_id,
            geometry=geometry,
            crop=crop.value
        )

        # check the task status to continue or not the process
        self.__analytics_processor_service.wait_and_check_task_status(task_id)

        # Analytics Schema
        schema = "GREENNESS"

        # if task successfully completed, get metrics from analytics fabric
        return self.__analytics_fabric_service.get_lastest_metrics(sf_unique_id, schema)

    def get_harvest_readiness_analytics(self,
                                        start_date: str,
                                        end_date: str,
                                        sowing_date: str,
                                        crop: Enum,
                                        geometry: str):
        """launch a harvest readiness analytics processor and get the metrics in a panda dataframe object

            Args:
                start_date (str) : the start date used for the request (format YYYY-MM-dd)
                end_date (str) : the end date used for the request (format YYYY-MM-dd)
                sowing_date(str): sowing date of the field used to calculate potential score
                crop (Enum): the crop code,
                geometry (str): the geometry to calculate the analytic (WKT or GeoJSON)

            Returns:
                A Pandas DataFrame containing several columns with metrics
        """
        # validate and convert the geometry to WKT
        geometry = Helper.convert_to_wkt(geometry)

        if geometry is None:
            raise ValueError("The geometry is not a valid WKT of GeoJson")

        # Create seasonfield from geometry and extract uniqueId
        sfd_public_id = self.__master_data_management_service.extract_season_field_id(geometry)
        sf_unique_id = self.__master_data_management_service.get_season_field_unique_id(sfd_public_id)

        task_id = self.__analytics_processor_service.launch_harvest_readiness_processor(
            start_date=start_date,
            end_date=end_date,
            sowing_date=sowing_date,
            seasonfield_id=sf_unique_id,
            geometry=geometry,
            crop=crop.value
        )

        # check the task status to continue or not the process
        self.__analytics_processor_service.wait_and_check_task_status(task_id)

        # Analytics Schema
        schema = "HARVEST_READINESS"

        # if task successfully completed, get metrics from analytics fabric
        return self.__analytics_fabric_service.get_lastest_metrics(sf_unique_id, schema)

    def get_planted_area_analytics(self,
                                   start_date: str,
                                   end_date: str,
                                   geometry: str):
        """launch a planted area analytics processor and get the metrics in a panda dataframe object

                    Args:
                        start_date (str) : the start date used for the request (format YYYY-MM-dd)
                        end_date (str) : the end date used for the request (format YYYY-MM-dd)
                        geometry (str): the geometry to calculate the analytic (WKT or GeoJSON),
                    Returns:
                        A Pandas DataFrame containing several columns with metrics
        """
        # validate and convert the geometry to WKT
        geometry = Helper.convert_to_wkt(geometry)

        if geometry is None:
            raise ValueError("The geometry is not a valid WKT of GeoJson")

        # Create seasonfield from geometry and extract uniqueId
        sfd_public_id = self.__master_data_management_service.extract_season_field_id(geometry)
        sf_unique_id = self.__master_data_management_service.get_season_field_unique_id(sfd_public_id)

        task_id = self.__analytics_processor_service.launch_planted_area_processor(start_date, end_date, sf_unique_id)
        # check the task status to continue or not the process
        self.__analytics_processor_service.wait_and_check_task_status(task_id)

        schema = "PLANTED_AREA"

        # if task successfully completed, get latests metrics from analytics fabric
        return self.__analytics_fabric_service.get_lastest_metrics(sf_unique_id, schema)

    def get_zarc_analytics(self,
                           start_date_emergence: str,
                           end_date_emergence: str,
                           nb_days_sowing_emergence: int,
                           crop: Enum,
                           soil_type: ZarcSoilType,
                           cycle: ZarcCycleType,
                           geometry: str):
        """launch a zarc analytics processor and get the metrics in a panda dataframe object

                        Args:
                            start_date_emergence (str) : the emergence start date used for the request (format YYYY-MM-dd)
                            end_date_emergence (str) : the emergence end date used for the request (format YYYY-MM-dd)
                            nb_days_sowing_emergence (int): the number of days for sowing emergence
                            crop (Enum): the zarc crop code,
                            soil_type (ZarcSoilType): the zarc soil type (1/2/3),
                            cycle (ZarcCycleType): the zarc cycle type (1/2/3),
                            geometry (str): the geometry to calculate the analytic (WKT or GeoJSON),
                        Returns:
                            A Pandas DataFrame containing several columns with metrics
        """
        # validate and convert the geometry to WKT
        geometry = Helper.convert_to_wkt(geometry)

        if geometry is None:
            raise ValueError("The geometry is not a valid WKT of GeoJson")

        # get municipio id from geometry
        municipio_id = self.__gis_service.get_municipio_id_from_geometry(geometry)

        if municipio_id == 0:
            raise ValueError(f"No municipio id found for this geometry")

        # Create seasonfield from geometry and extract uniqueId
        sfd_public_id = self.__master_data_management_service.extract_season_field_id(geometry)
        sf_unique_id = self.__master_data_management_service.get_season_field_unique_id(sfd_public_id)

        task_id = self.__analytics_processor_service.launch_zarc_processor(
            start_date_emergence=start_date_emergence,
            end_date_emergence=end_date_emergence,
            crop=crop.value,
            cycle=cycle.value,
            soil_type=soil_type.value,
            municipio=municipio_id,
            nb_days_sowing_emergence=nb_days_sowing_emergence,
            seasonfield_id=sf_unique_id
        )

        # check the task status to continue or not the process
        self.__analytics_processor_service.wait_and_check_task_status(task_id)

        # Analytics Schema
        schema = "ZARC"

        # if task successfully completed, get metrics from analytics fabric
        return self.__analytics_fabric_service.get_lastest_metrics(sf_unique_id, schema)
