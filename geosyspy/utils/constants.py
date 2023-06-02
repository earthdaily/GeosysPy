from enum import Enum


class SatelliteImageryCollection(Enum):
    """
    Available imagery collections
    """
    MODIS = "MODIS"
    SENTINEL_2 = "SENTINEL_2"
    LANDSAT_8 = "LANDSAT_8"
    LANDSAT_9 = "LANDSAT_9"


class WeatherTypeCollection(Enum):
    """
    Available weather collections
    """
    WEATHER_FORECAST_DAILY = "FORECAST_DAILY"
    WEATHER_FORECAST_HOURLY = "FORECAST_HOURLY"
    WEATHER_HISTORICAL_DAILY = "HISTORICAL_DAILY"


class Env(Enum):
    """
    Environment to target (PROD, PREPROD)
    """
    PROD = "prod"
    PREPROD = "preprod"


class Region(Enum):
    """
    Region to target (NA, EU)
    """
    NA = "na"
    EU = "eu"


class GeosysApiEndpoints(Enum):
    """
    Available Geosys APIs Endpoints
    """
    MASTER_DATA_MANAGEMENT_ENDPOINT = "master-data-management/v6/seasonfields"
    VTS_ENDPOINT = "vegetation-time-series/v1/season-fields"
    VTS_BY_PIXEL_ENDPOINT = "vegetation-time-series/v1/season-fields/pixels"
    FLM_CATALOG_IMAGERY = "field-level-maps/v4/season-fields/{}/catalog-imagery"
    FLM_COVERAGE = "field-level-maps/v4/season-fields/{}/coverage"
    WEATHER_ENDPOINT = "Weather/v1/weather"
    ANALYTICS_FABRIC_ENDPOINT = "analytics/metrics"
    ANALYTICS_FABRIC_SCHEMA_ENDPOINT = "analytics/schemas"
    MRTS_PROCESSOR_EVENTS_ENDPOINT = "analytics-pipeline/v1/processors/events"
    MRTS_PROCESSOR_ENDPOINT = "analytics-pipeline/v1/processors/mrts/launch"

LR_SATELLITE_COLLECTION = [SatelliteImageryCollection.MODIS]
MR_SATELLITE_COLLECTION = [SatelliteImageryCollection.LANDSAT_8, SatelliteImageryCollection.LANDSAT_9,
                           SatelliteImageryCollection.SENTINEL_2]

PRIORITY_HEADERS = {"bulk": "Geosys_API_Bulk", "realtime": ""}
SEASON_FIELD_ID_REGEX = r"\sId:\s(\w+),"