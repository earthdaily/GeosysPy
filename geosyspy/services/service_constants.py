from enum import Enum


class ProcessorConfiguration(Enum):
    """
    Available different Geosys Api Processor key path & profile values
    """
    PLANTED_AREA = {"profile":"planted_area_default", "api_processor_path":"planted_area"}
    POTENTIAL_SCORE = {"profile":"potential_score_default", "api_processor_path":"potential_score"}
    BRAZIL_IN_SEASON_CROP_ID = {"profile":"brazil_in_season_crop_id_default", "api_processor_path":"brazil_in_season_crop_id"}
    GREENNESS = {"profile":"greenness_default", "api_processor_path":"greenness"}
    HARVEST_READINESS = {"profile":"harvest_readiness_default", "api_processor_path":"harvest_readiness"}
    ZARC = {"profile":"zarc_default", "api_processor_path":"zarc"}
    EMERGENCE_IN_SEASON = {"profile":"inseason-emergence_default", "api_processor_path":"emergence-date"}
    EMERGENCE_HISTORICAL = {"profile":"historical-emergence_default", "api_processor_path":"historical-emergence"}
    EMERGENCE_DELAY = {"profile": "inseason-emergence_default", "api_processor_path": "emergence-delay"}
    HARVEST_IN_SEASON = {"profile":"inseason-harvest_default", "api_processor_path":"inseason-harvest"}
    HARVEST_HISTORICAL = {"profile":"historical-harvest_default", "api_processor_path":"historical-harvest"}
    MRTS = {"profile":"mrts_default", "api_processor_path":"mrts"}

