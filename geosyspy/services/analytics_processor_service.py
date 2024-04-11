"""Analytics Processor service class"""
import json
import logging
import datetime
from urllib.parse import urljoin
import retrying
from geosyspy.utils.constants import GeosysApiEndpoints, Harvest, Emergence
from geosyspy.services.service_constants import ProcessorConfiguration
from geosyspy.utils.http_client import HttpClient



class AnalyticsProcessorService:

    def __init__(self, base_url: str, http_client: HttpClient):
        self.base_url: str = base_url
        self.http_client: HttpClient = http_client
        self.logger = logging.getLogger(__name__)

    @retrying.retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_attempt_number=50, retry_on_exception=lambda exc: isinstance(exc, KeyError))
    def wait_and_check_task_status(self, task_id: str):
        """Check task status until it is ended for a specific analytics processor run

        Args:
            task_id (str) : A string representing a task id

        Returns:
            task_status (str): the status of the task

        """

        events_endpoint: str = urljoin(self.base_url,
                                       GeosysApiEndpoints.PROCESSOR_EVENTS_ENDPOINT.value + "/" + task_id)

        while True:
            response = self.http_client.get(events_endpoint)
            if response.ok:
                dict_resp = json.loads(response.content)
                task_status = dict_resp["status"]
            else:
                self.logger.info(response.status_code)
                return "Failed"

            if task_status == "Ended":
                break  # the task is completed.
            elif task_status == "Running":
                self.logger.info("Retry -- Task still running")
                raise KeyError("Task still running")  # raise exception to retry
            else:
                raise Exception(f"Task Status: {task_status}, Content: {response.content}" )

        return task_status

    def get_s3_path_from_task_and_processor(self, task_id: str,
                                            processor_name: str):
        """Returns S3 path related to task_id

        Args:
            task_id : A string representing a task id
            processor_name: the processor name
        Returns:
            path : uri

        """

        events_endpoint: str = urljoin(self.base_url,
                                            GeosysApiEndpoints.PROCESSOR_EVENTS_ENDPOINT.value + "/" + task_id)

        response = self.http_client.get(events_endpoint)
        if response.ok:
            dict_resp = json.loads(response.content)
            customer_code: str = dict_resp["customerCode"].lower().replace("_", "-")
            user_id: str = dict_resp["userId"]
            task_id = dict_resp["taskId"]
            return f"s3://geosys-{customer_code}/{user_id}/{processor_name}/{task_id}"
        
        self.logger.info(response.status_code)
        raise ValueError(response.content)

    def launch_mr_time_series_processor(self, polygon,
                                        start_date: str,
                                        end_date,
                                        list_sensors,
                                        denoiser: bool,
                                        smoother: str,
                                        eoc: bool,
                                        aggregation: str,
                                        index: str,
                                        raw_data: bool):
        """launch a MRTS analytics processor and get the task id in result

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
                taskId (str)
        """

        if end_date is None:
            end_date = datetime.datetime.today().strftime("%Y-%m-%d")
        payload = {
            "parametersProfile": {
                "code": ProcessorConfiguration.MRTS.value['profile'],
                "version": 1
            },
            "parameters": {
                "start_date": start_date,
                "end_date": end_date,
                "sensors": list_sensors,
                "denoiser": denoiser,
                "smoother": smoother,
                "eoc": eoc,
                "aggregation": aggregation,
                "index": index,
                "raw_data": raw_data
            },
            "data": [
                {"wkt": polygon}
            ]
        }

        processor_endpoint: str = urljoin(self.base_url,
                                          GeosysApiEndpoints.LAUNCH_PROCESSOR_ENDPOINT.value.format(
                                              ProcessorConfiguration.MRTS.value['api_processor_path']))

        response = self.http_client.post(processor_endpoint, payload)

        if response.ok:
            task_id = json.loads(response.content)["taskId"]
            return task_id
        self.logger.info(response.status_code)
        raise ValueError(response.content)

    def launch_planted_area_processor(self,
                                      start_date: str,
                                      end_date: str,
                                      seasonfield_id: str):
        """launch a planted area analytics processor and get the task id in result

            Args:
                start_date (str) : the start date used for the request (format YYYY-MM-dd)
                end_date (str) : the end date used for the request (format YYYY-MM-dd)
                seasonfield_id (sfd) : seasonField geosys uniqueId

            Returns:
                taskId (str)
        """

        payload = {
            "parametersProfileCode": ProcessorConfiguration.PLANTED_AREA.value['profile'],
            "data":
                [
                    {
                        "id": seasonfield_id + "@ID",
                        "start_date": start_date,
                        "end_date": end_date
                    }
                ]
        }

        processor_endpoint: str = urljoin(self.base_url,
                                          GeosysApiEndpoints.LAUNCH_PROCESSOR_ENDPOINT.value.format(ProcessorConfiguration.PLANTED_AREA.value['api_processor_path']))

        response = self.http_client.post(processor_endpoint, payload)

        if response.ok:
            task_id = json.loads(response.content)["taskId"]
            return task_id
        self.logger.info(response.status_code)
        raise ValueError(response.content)

    def launch_harvest_processor(self,
                                 season_duration: int,
                                 season_start_day: int,
                                 season_start_month: int,
                                 crop: str,
                                 year: int,
                                 seasonfield_id: str,
                                 geometry: str,
                                 harvest_type: Harvest):
        """launch a harvest analytics processor and get the task id in result

            Args:
                season_duration (int): the duration of the season in days,
                season_start_day (int): the start day value (1 - 31),
                season_start_month (int): the start month value (1 - 12),
                crop (str): the geosys crop code,
                year (int): the year value,
                seasonfield_id (sfd) : seasonField geosys uniqueId
                geometry (str): the geometry to calcultate the analytic (WKT or GeoJSON),
                harvest_type (Harvest): the type of Harvest analytics (INSEASON/HISTORICAL)

            Returns:
                taskId (str)
        """

        # build payload for api call
        payload = {
            "parametersProfile": ProcessorConfiguration[harvest_type.name].value['profile'],
            "parameters": {
                "season_duration": season_duration,
                "season_start_day": season_start_day,
                "season_start_month": season_start_month
            },
            "data":
                [
                    {
                        "id": "SeasonField:" + seasonfield_id + "@ID",
                        "crop": crop,
                        "year": year,
                        "geom": geometry
                    }
                ]
        }

        processor_endpoint: str = urljoin(self.base_url,
                                          GeosysApiEndpoints.LAUNCH_PROCESSOR_ENDPOINT.value.format(ProcessorConfiguration[harvest_type.name].value['api_processor_path']))

        response = self.http_client.post(processor_endpoint, payload)

        if response.ok:
            task_id = json.loads(response.content)["taskId"]
            return task_id        
        self.logger.info(response.status_code)
        raise ValueError(response.content)

    def launch_emergence_processor(self,
                                   season_duration: int,
                                   season_start_day: int,
                                   season_start_month: int,
                                   crop: str,
                                   year: int,
                                   seasonfield_id: str,
                                   geometry: str,
                                   emergence_type: Emergence):
        """launch an emergence analytics processor and get the task id in result

            Args:
                season_duration (int): the duration of the season in days,
                season_start_day (int): the start day value (1 - 31),
                season_start_month (int): the start month value (1 - 12),
                crop (str): the geosys crop code,
                year (int): the year value,
                seasonfield_id (sfd) : seasonField geosys uniqueId
                geometry (str): the geometry to calculate the analytic (WKT or GeoJSON),
                emergence_type (Emergence): the type of Emergence analytics (INSEASON/HISTORICAL/DELAY)

            Returns:
                taskId (str)
        """

        # build payload for api call
        payload = {
            "parametersProfile": ProcessorConfiguration[emergence_type.name].value['profile'],
            "parameters": {
                "season_duration": season_duration,
                "season_start_day": season_start_day,
                "season_start_month": season_start_month
            },
            "data":
                [
                    {
                        "id": "SeasonField:" + seasonfield_id + "@ID",
                        "crop": crop,
                        "year": year,
                        "geom": geometry
                    }
                ]
        }

        if emergence_type == Emergence.EMERGENCE_DELAY:
            payload["parameters"]["emergence_delay"] = True

        processor_endpoint: str = urljoin(self.base_url,
                                          GeosysApiEndpoints.LAUNCH_PROCESSOR_ENDPOINT.value.format( ProcessorConfiguration[emergence_type.name].value['api_processor_path']))

        response = self.http_client.post(processor_endpoint, payload)

        if response.ok:
            task_id = json.loads(response.content)["taskId"]
            return task_id
        self.logger.info(response.status_code)
        raise ValueError(response.content)

    def launch_potential_score_processor(self,
                                         season_duration: int,
                                         season_start_day: int,
                                         season_start_month: int,
                                         crop: str,
                                         end_date: str,
                                         sowing_date: str,
                                         nb_historical_years: int,
                                         seasonfield_id: str,
                                         geometry: str):
        """launch a potential score analytics processor and get the task id in result

            Args:
                season_duration (int): the duration of the season in days,
                season_start_day (int): the start day value (1 - 31),
                season_start_month (int): the start month value (1 - 12),
                crop (str): the geosys crop code,
                end_date (str): end date used to calculate potential score
                sowing_date (str): sowing date of the filed used to calculate potential score
                nb_historical_years (int): number of historical years data to calculate potential score
                seasonfield_id (sfd) : seasonField geosys uniqueId
                geometry (str): the geometry to calculate the analytic (WKT or GeoJSON)

            Returns:
                taskId (str)
        """

        # build payload for api call
        payload = {
            "parametersProfile":  ProcessorConfiguration.POTENTIAL_SCORE.value['profile'],
            "parameters": {
                "end_date" : end_date,
                "nb_historical_years" : nb_historical_years,
                "season_duration": season_duration,
                "season_start_day": season_start_day,
                "season_start_month": season_start_month
            },
            "data":
                [
                    {
                        "id": "SeasonField:" + seasonfield_id + "@ID",
                        "crop": crop,
                        "sowing_date": sowing_date,
                        "geom": geometry
                    }
                ]
        }

        processor_endpoint: str = urljoin(self.base_url,
                                          GeosysApiEndpoints.LAUNCH_PROCESSOR_ENDPOINT.value.format(ProcessorConfiguration.POTENTIAL_SCORE.value['api_processor_path']))

        response = self.http_client.post(processor_endpoint, payload)

        if response.ok:
            task_id = json.loads(response.content)["taskId"]
            return task_id
        self.logger.info(response.status_code)
        raise ValueError(response.content)

    def launch_brazil_in_season_crop_id_processor(self,
                                                  start_date: str,
                                                  end_date: str,
                                                  season: str,
                                                  seasonfield_id: str,
                                                  geometry: str):
        """launch a brazil-in-season-crop-id analytics processor and get the task id in result

            Args:
                start_date (str) : the start date used for the request (format YYYY-MM-dd)
                end_date (str) : the end date used for the request (format YYYY-MM-dd)

                season (str): the season name,
                seasonfield_id (sfd) : seasonField geosys uniqueId
                geometry (str): the geometry to calculate the analytic (WKT or GeoJSON),

            Returns:
                taskId (str)
        """

        # build payload for api call
        payload = {
            "parametersProfile": ProcessorConfiguration.BRAZIL_IN_SEASON_CROP_ID.value['profile'],
            "parameters": {
                "start_date": start_date,
                "end_date": end_date,
                "season": season
            },
            "data":
                [
                    {
                        "id": "SeasonField:" + seasonfield_id + "@ID",
                        "geom": geometry
                    }
                ]
        }

        processor_endpoint: str = urljoin(self.base_url,
                                          GeosysApiEndpoints.LAUNCH_PROCESSOR_ENDPOINT.value.format(ProcessorConfiguration.BRAZIL_IN_SEASON_CROP_ID.value['api_processor_path']))

        response = self.http_client.post(processor_endpoint, payload)

        if response.ok:
            task_id = json.loads(response.content)["taskId"]
            return task_id
        self.logger.info(response.status_code)
        raise ValueError(response.content)

    def launch_greenness_processor(self,
                                   start_date: str,
                                   end_date: str,
                                   sowing_date: str,
                                   crop: str,
                                   seasonfield_id: str,
                                   geometry: str):
        """launch a greenness analytics processor and get the task id in result

            Args:
                start_date (str) : the start date used for the request (format YYYY-MM-dd)
                end_date (str) : the end date used for the request (format YYYY-MM-dd)
                sowing_date(str): sowing date of the field used to calculate potential score
                crop (str): the EDA crop code,
                seasonfield_id (sfd) : seasonField geosys uniqueId
                geometry (str): the geometry to calculate the analytic (WKT or GeoJSON),

            Returns:
                taskId (str)
        """

        # build payload for api call
        payload = {
            "parametersProfile": ProcessorConfiguration.GREENNESS.value['profile'],
            "parameters": {
                "start_date": start_date,
                "end_date": end_date,
            },
            "data":
                [
                    {
                        "id": "SeasonField:" + seasonfield_id + "@ID",
                        "crop": crop,
                        "sowing_date": sowing_date,
                        "geom": geometry
                    }
                ]
        }

        processor_endpoint: str = urljoin(self.base_url,
                                          GeosysApiEndpoints.LAUNCH_PROCESSOR_ENDPOINT.value.format(ProcessorConfiguration.GREENNESS.value['api_processor_path']))

        response = self.http_client.post(processor_endpoint, payload)

        if response.ok:
            task_id = json.loads(response.content)["taskId"]
            return task_id
        self.logger.info(response.status_code)
        raise ValueError(response.content)


    def launch_harvest_readiness_processor(self,
                                   start_date: str,
                                   end_date: str,
                                   sowing_date: str,
                                   crop: str,
                                   seasonfield_id: str,
                                   geometry: str):
        """launch a harvest readiness analytics processor and get the task id in result

            Args:
                start_date (str) : the start date used for the request (format YYYY-MM-dd)
                end_date (str) : the end date used for the request (format YYYY-MM-dd)
                sowing_date(str): sowing date of the field used to calculate potential score
                crop (str): the EDA crop code,
                seasonfield_id (sfd) : seasonField geosys uniqueId
                geometry (str): the geometry to calculate the analytic (WKT or GeoJSON),

            Returns:
                taskId (str)
        """

        # build payload for api call
        payload = {
            "parametersProfile": ProcessorConfiguration.HARVEST_READINESS.value['profile'],
            "parameters": {
                "start_date": start_date,
                "end_date": end_date,
            },
            "data":
                [
                    {
                        "id": "SeasonField:" + seasonfield_id + "@ID",
                        "crop": crop,
                        "sowing_date": sowing_date,
                        "geom": geometry
                    }
                ]
        }

        processor_endpoint: str = urljoin(self.base_url,
                                          GeosysApiEndpoints.LAUNCH_PROCESSOR_ENDPOINT.value.format(ProcessorConfiguration.HARVEST_READINESS.value['api_processor_path']))

        response = self.http_client.post(processor_endpoint, payload)

        if response.ok:
            task_id = json.loads(response.content)["taskId"]
            return task_id
        self.logger.info(response.status_code)
        raise ValueError(response.content)

    def launch_zarc_processor(self,
                              start_date_emergence: str,
                              end_date_emergence: str,
                              nb_days_sowing_emergence: int,
                              crop: str,
                              municipio: int,
                              soil_type: str,
                              cycle: str,
                              seasonfield_id:str):
        """launch a zarc analytics processor and get the task id in result

            Args:
                start_date_emergence (str) : the emergence start date used for the request (format YYYY-MM-dd)
                end_date_emergence (str) : the emergence end date used for the request (format YYYY-MM-dd)
                nb_days_sowing_emergence (int): the number of days for sowing emergence
                crop (str): the zarc crop code,
                municipio (int): the municipio id,
                soil_type (str): the zarc soil type,
                cycle (str): the zarc cycle value,
                seasonfield_id (sfd) : seasonField geosys uniqueId

            Returns:
                taskId (str)
        """

        # build payload for api call
        payload = {
            "parametersProfile": ProcessorConfiguration.ZARC.value['profile'],
            "data":
                [
                    {
                        "id": "SeasonField:" + seasonfield_id + "@ID",
                        "start_date_emergence": start_date_emergence,
                        "end_date_emergence": end_date_emergence,
                        "crop_zarc": crop,
                        "municipio_zarc": municipio,
                        "nb_days_sowing_emergence": nb_days_sowing_emergence,
                        "soil_type_zarc": soil_type,
                        "cycle_zarc": cycle
                    }
                ]
        }

        processor_endpoint: str = urljoin(self.base_url,
                                            GeosysApiEndpoints.LAUNCH_PROCESSOR_ENDPOINT.value.format(
                                                ProcessorConfiguration.ZARC.value['api_processor_path']))

        response = self.http_client.post(processor_endpoint, payload)

        if response.ok:
            task_id = json.loads(response.content)["taskId"]
            return task_id
        self.logger.info(response.status_code)
        raise ValueError(response.content)
