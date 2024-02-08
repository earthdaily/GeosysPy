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
    Region to target (NA)
    """
    NA = "na"


class Harvest(Enum):
    """
    Type of Harvest query used for Harvest analytics processor
    """
    HARVEST_IN_SEASON = "IN_SEASON"
    HARVEST_HISTORICAL = "HISTORICAL"

class Emergence(Enum):
    """
    Type of Emergence query used for Emergence analytics processor
    """
    EMERGENCE_IN_SEASON = "IN_SEASON"
    EMERGENCE_HISTORICAL = "HISTORICAL"
    EMERGENCE_DELAY = "DELAY"


class AgriquestCommodityCode(Enum):
    """
    Available AgriQuest Commodity values
    """
    ALL_VEGETATION = 33
    ALL_CROPS = 35


class AgriquestFranceBlockCode(Enum):
    """
    Available AgriQuest Block codes dedicated to France
    """
    FRA_CANTONS = 216
    FRA_COMMUNES = 135
    FRA_DEPARTEMENTS = 226


class AgriquestBlocks(Enum):
    """
    Available AgriQuest Block codes
    """
    FIRST_LEVEL = 129
    AMU_AUSTRALIA_LEVEL_1 = 205
    AMU_AUSTRALIA_LEVEL_2 = 206
    AMU_CHINA = 202
    AMU_EUROPE_RUSSIA = 197
    AMU_INDIA = 204
    AMU_MEXICO = 212
    AMU_NORTH_AMERICA = 207
    AMU_SOUTH_AFRICA = 213
    BM_REGIONS = 139
    CAR = 140
    COUNTY = 141
    FRA_CANTONS = 216
    FRA_COMMUNES = 135
    FRA_DEPARTEMENTS = 226
    MESOREGION = 131
    NORTH_AFRICA_AMU = 125
    RAION = 127
    SERBIA = 132
    SOUTH_AMERICA_MUNICIPIOS_2020 = 267
    SOUTH_AMERICA_AMU = 115
    SPAIN_COMARCAS = 136
    US_ASD = 130
    WESTERN_AFRICA_AMU = 122


class AgriquestWeatherType(Enum):
    """
    Available AgriQuest Weather types
    """
    CUMULATIVE_PRECIPITATION = "cumulative-precipitation"
    MIN_TEMPERATURE = "min-temperature"
    AVERAGE_TEMPERATURE = "average-temperature"
    MAX_TEMPERATURE = "max-temperature"
    MAX_WIND_SPEED = "max-wind-speed"
    RELATIVE_HUMIDITY = "relative-humidity"
    SNOW_DEPTH = "snow-depth"
    SOIL_MOISTURE = "soil-moisture"
    SOLAR_RADIATION = "solar-radiation"

class ZarcSoilType(Enum):
    """
    Available Soil Type values for analytics processor Zarc
    """
    SOIL_TYPE_1 = "1"
    SOIL_TYPE_2 = "2"
    SOIL_TYPE_3 = "3"
    NONE = None

class CropIdSeason(Enum):
    """
    Available season values  for analytics processor Zarc
    """
    SEASON_1="SEASON_1"
    SEASON_2="SEASON_2"


class ZarcCycleType(Enum):
    """
    Available season values  for analytics processor Zarc
    """
    CYCLE_TYPE_1 = "1"
    CYCLE_TYPE_2 = "2"
    CYCLE_TYPE_3 = "3"
    NONE = None


class GeosysApiEndpoints(Enum):
    """
    Available Geosys APIs Endpoints
    """
    MASTER_DATA_MANAGEMENT_ENDPOINT = "master-data-management/v6"
    VTS_ENDPOINT = "vegetation-time-series/v1/season-fields"
    VTS_BY_PIXEL_ENDPOINT = "vegetation-time-series/v1/season-fields/pixels"
    FLM_CATALOG_IMAGERY = "field-level-maps/v4/season-fields/{}/catalog-imagery"
    FLM_COVERAGE = "field-level-maps/v4/season-fields/{}/coverage"
    WEATHER_ENDPOINT = "Weather/v1/weather"
    ANALYTICS_FABRIC_ENDPOINT = "analytics/metrics"
    ANALYTICS_FABRIC_LATEST_ENDPOINT = "analytics/metrics-latest"
    ANALYTICS_FABRIC_SCHEMA_ENDPOINT = "analytics/schemas"
    AGRIQUEST_ENDPOINT = "agriquest/Geosys.Agriquest.CropMonitoring.WebApi/v0/api"
    # Analytics processor
    PROCESSOR_EVENTS_ENDPOINT = "analytics-pipeline/v1/processors/events"
    LAUNCH_PROCESSOR_ENDPOINT = "analytics-pipeline/v1/processors/{}/launch"


LR_SATELLITE_COLLECTION = [SatelliteImageryCollection.MODIS]
MR_SATELLITE_COLLECTION = [SatelliteImageryCollection.LANDSAT_8, SatelliteImageryCollection.LANDSAT_9,
                           SatelliteImageryCollection.SENTINEL_2]

PRIORITY_HEADERS = {"bulk": "Geosys_API_Bulk", "realtime": ""}
SEASON_FIELD_ID_REGEX = r"\sId:\s(\w+),"
