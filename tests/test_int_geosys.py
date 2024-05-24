from datetime import datetime
import pytest
from dateutil.relativedelta import relativedelta

from geosyspy import Geosys
from dotenv import load_dotenv
import os
import datetime as dt
import numpy as np
from geosyspy.utils.constants import *

# read .env file
load_dotenv()

API_CLIENT_ID = os.getenv("API_CLIENT_ID")
API_CLIENT_SECRET = os.getenv("API_CLIENT_SECRET")
API_USERNAME = os.getenv("API_USERNAME")
API_PASSWORD = os.getenv("API_PASSWORD")

POLYGON = "POLYGON((-91.17523978603823 40.29787117039518,-91.17577285022956 40.29199489606421,-91.167613719932 40.29199489606421,-91.1673028670095 40.29867040193312,-91.17523978603823 40.29787117039518))"


class TestGeosys:
    client = Geosys(API_CLIENT_ID,
                    API_CLIENT_SECRET,
                    API_USERNAME,
                    API_PASSWORD,
                    Env.PREPROD,
                    Region.NA
                    )

    prod_client = Geosys(API_CLIENT_ID,
                         API_CLIENT_SECRET,
                         API_USERNAME,
                         API_PASSWORD,
                         Env.PROD,
                         Region.NA
                         )
    # get list of available crops
    crops = prod_client.get_available_crops()

    def test_authenticate(self):
        credentials = self.client.http_client.get_access_token();
        assert {"access_token", "expires_in", "token_type", "scope", "expires_at",
                "refresh_token"}.issubset(set(credentials.keys()))
        assert credentials['access_token'] is not None
        assert credentials['refresh_token'] is not None
        assert credentials['expires_at'] > datetime.today().timestamp()

    def test_get_time_series_modis_ndvi(self):
        start_date = dt.datetime.strptime("2020-01-01", "%Y-%m-%d")
        end_date = dt.datetime.strptime("2020-01-07", "%Y-%m-%d")

        df = self.client.get_time_series(
            start_date, end_date, SatelliteImageryCollection.MODIS, ["NDVI"], polygon=POLYGON
        )

        assert df.index.name == "date"
        assert "value" in df.columns
        assert "index" in df.columns
        assert len(df.index) == 7
        date_range = list(map(lambda x: x.strftime("%Y-%m-%d"), df.index))
        assert {"2020-01-01", "2020-01-02", "2020-01-03", "2020-01-04", "2020-01-05", "2020-01-06",
                "2020-01-07"}.issubset(set(date_range))

    def test_get_satellite_image_time_series_modis_ndvi(self):
        start_date = dt.datetime.strptime("2020-01-01", "%Y-%m-%d")
        end_date = dt.datetime.strptime("2020-01-07", "%Y-%m-%d")
        POLYGON = "POLYGON((-91.29152885756007 40.39177489815265,-91.28403789132507 40.391776131485386,-91.28386736508233 40.389390758655935,-91.29143832829979 40.38874592864832,-91.29152885756007 40.39177489815265))"
        df = self.client.get_satellite_image_time_series(
             start_date, end_date, [SatelliteImageryCollection.MODIS], ["NDVI"], polygon=POLYGON
        )
        assert df.index.name == "date"
        assert {"value", "index", "pixel.id"}.issubset(set(df.columns))
        assert np.all((df["index"].values == "NDVI"))
        assert len(df.index) == 14

        assert {"2020-01-01", "2020-01-02", "2020-01-03", "2020-01-04", "2020-01-05", "2020-01-06",
                "2020-01-07"}.issubset(set(df.index))

        assert {"mh11v4i225j4612", "mh11v4i226j4612"}.issubset(set(df["pixel.id"]))

    def test_get_satellite_coverage_image_references(self):
        end_date = dt.date.today()
        start_date = dt.date.today() + relativedelta(months=-12)
        info, images_references = self.client.get_satellite_coverage_image_references(
            start_date, end_date,
            collections=[SatelliteImageryCollection.SENTINEL_2, SatelliteImageryCollection.LANDSAT_8,
                         SatelliteImageryCollection.LANDSAT_9], polygon=POLYGON)

        assert {"coveragePercent", "image.id", "image.availableBands", "image.sensor",
                "image.spatialResolution", "image.date", "seasonField.id"}.issubset(set(info.columns))

        assert len(info) == len(images_references)
        for i, image_info in info.iterrows():
            assert (
                       image_info["image.date"],
                       image_info["image.sensor"],
                   ) in images_references

    def get_time_series_weather_historical_daily(self):
        start_date = dt.datetime.strptime("2021-01-01", "%Y-%m-%d")
        end_date = dt.datetime.strptime("2022-01-01", "%Y-%m-%d")
        indicators = [
            "Precipitation",
            "Temperature.Ground",
            "Temperature.Standard",
            "Temperature.StandardMax",
            "Date",
        ]

        df = self.client.get_time_series(            
            start_date,
            end_date,
            WeatherTypeCollection.WEATHER_HISTORICAL_DAILY,
            indicators,
            polygon=POLYGON
        )

        assert {"precipitation.cumulative", "precipitation.probabilities", "temperature.ground", "temperature.standard",
                "temperature.standardMax"}.issubset(set(df.columns))
        assert df.index.name == "date"

    # def test_get_metrics(self):
    #
    #     lai_radar_polygon = "POLYGON((-52.72591542 -18.7395779,-52.72604885 -18.73951122,-52.72603114 -18.73908689,-52.71556835 -18.72490316,-52.71391916 -18.72612966,-52.71362802 -18.72623726,-52.71086473 -18.72804231,-52.72083542 -18.74173696,-52.72118937 -18.74159174,-52.72139229 -18.7418552,-52.72600257 -18.73969719,-52.72591542 -18.7395779))"
    #     schema_id = "LAI_RADAR"
    #     start_date = dt.datetime.strptime("2022-01-24", "%Y-%m-%d")
    #     end_date = dt.datetime.strptime("2022-01-30", "%Y-%m-%d")
    #     df = self.client.get_metrics(lai_radar_polygon, schema_id, start_date, end_date)
    #
    #     assert set(
    #         [
    #             "Values.RVI",
    #             "Values.LAI",
    #             "Schema.Id",
    #         ]
    #     ).issubset(set(df.columns))
    #     assert set(
    #         [
    #             "2022-01-24T00:00:00Z",
    #             "2022-01-25T00:00:00Z",
    #             "2022-01-26T00:00:00Z",
    #             "2022-01-27T00:00:00Z",
    #             "2022-01-28T00:00:00Z",
    #             "2022-01-29T00:00:00Z",
    #             "2022-01-30T00:00:00Z",
    #         ]
    #     ).issubset(set(df.index))
    #     assert df.index.name == "date"

    def test_get_satellite_image_time_series(self):
        start_date = dt.datetime.strptime("2022-05-01", "%Y-%m-%d")
        end_date = dt.datetime.strptime("2023-04-28", "%Y-%m-%d")
        dataset = self.client.get_satellite_image_time_series(            
            start_date,
            end_date,
            collections=[SatelliteImageryCollection.SENTINEL_2, SatelliteImageryCollection.LANDSAT_8],
            indicators=["Reflectance"],
            polygon=POLYGON
        )
        assert dict(dataset.dims) == {'band': 7, 'y': 27, 'x': 26, 'time': 9}

    def test_get_agriquest_weather_time_series(self):
        start_date = "2022-05-01"
        end_date = "2023-04-28"
        dataset = self.client.get_agriquest_weather_block_data(
            start_date=start_date,
            end_date=end_date,
            block_code=AgriquestBlocks.FRA_DEPARTEMENTS,
            weather_type=AgriquestWeatherType.CUMULATIVE_PRECIPITATION
        )
        assert dataset.keys()[0] == "AMU"
        assert len(dataset["AMU"]) == 97

    def test_get_agriquest_ndvi_time_series(self):
        date = "2023-06-05"
        dataset = self.client.get_agriquest_ndvi_block_data(
            day_of_measure=date,
            commodity_code=AgriquestCommodityCode.ALL_VEGETATION,
            block_code=AgriquestBlocks.AMU_NORTH_AMERICA,
        )
        assert dataset.keys()[0] == "AMU"
        assert dataset.keys()[-1] == "NDVI"

    def test_get_harvest_analytics(self):
        dataset = self.prod_client.get_harvest_analytics(
            season_duration=215,
            season_start_day=1,
            season_start_month=4,
            crop=self.crops._2ND_CORN,
            year=2021,
            geometry="POLYGON ((-56.785919346530768 -21.208154463301554 ,  -56.79078750820733 -21.206043784434833 ,  -56.790973809206818 -21.206069651656232 ,  -56.791373799079636 -21.197107091323097 ,  -56.785129186971687 -21.196010916846863 ,  -56.781397554331065 -21.19535575112814 ,  -56.777108478217059 -21.202038412606473 ,  -56.778435977920665 -21.211398619037478 ,  -56.785919346530768 -21.208154463301554))",
            harvest_type=Harvest.HARVEST_HISTORICAL)

        assert dataset.keys()[0] == 'Values.harvest_year_1'
        assert dataset.keys()[-1] == 'Schema.Id'
        assert dataset.values[0][-1] == 'HISTORICAL_HARVEST'

    def test_get_emergence_analytics(self):
        dataset = self.prod_client.get_emergence_analytics(
            season_duration=215,
            season_start_day=1,
            season_start_month=4,
            crop=self.crops._2ND_CORN,
            year=2021,
            geometry="POLYGON ((-56.785919346530768 -21.208154463301554 ,  -56.79078750820733 -21.206043784434833 ,  -56.790973809206818 -21.206069651656232 ,  -56.791373799079636 -21.197107091323097 ,  -56.785129186971687 -21.196010916846863 ,  -56.781397554331065 -21.19535575112814 ,  -56.777108478217059 -21.202038412606473 ,  -56.778435977920665 -21.211398619037478 ,  -56.785919346530768 -21.208154463301554))",
            emergence_type=Emergence.EMERGENCE_IN_SEASON)

        assert dataset.keys()[0] == 'Values.EmergenceDate'
        assert dataset.keys()[-1] == 'Schema.Id'
        assert dataset.values[0][-1] == 'INSEASON_EMERGENCE'

    def test_get_potential_score_analytics(self):
        dataset = self.prod_client.get_potential_score_analytics(
            end_date="2022-03-06",
            nb_historical_years=5,
            season_duration=200,
            season_start_day=1,
            season_start_month=10,
            crop=self.crops.CORN,
            sowing_date="2021-10-01",
            geometry="POLYGON ((-54.26027778 -25.38777778, -54.26027778 -25.37444444, -54.26 -25.37416667, -54.25972222 -25.37444444, -54.25944444 -25.37444444, -54.25888889 -25.37472222, -54.258611110000004 -25.37472222, -54.25888889 -25.375, -54.25888889 -25.37555555, -54.258611110000004 -25.37611111, -54.258611110000004 -25.38194444, -54.25833333 -25.38416667, -54.25694444 -25.38361111, -54.25694444 -25.38416667, -54.2575 -25.38416667, -54.2575 -25.38444444, -54.25777778 -25.38416667, -54.25807016 -25.384158120000002, -54.25805556 -25.38444444, -54.258077300000004 -25.38472206, -54.2575 -25.38527778, -54.25694444 -25.385, -54.256388890000004 -25.38361111, -54.25472222 -25.38305555, -54.25472222 -25.3825, -54.254166670000004 -25.38194444, -54.25444444 -25.38166667, -54.25472222 -25.38166667, -54.25472222 -25.37944444, -54.25277778 -25.37944444, -54.25277778 -25.38583333, -54.25419223 -25.3861539, -54.2539067 -25.38589216, -54.25388889 -25.385, -54.25444444 -25.38555555, -54.2547871 -25.385820770000002, -54.25472222 -25.38611111, -54.26027778 -25.38777778))"
        )
        assert dataset.keys()[0] == 'Values.historical_potential_score'
        assert dataset.keys()[-1] == 'Schema.Id'
        assert dataset.values[0][-1] == 'POTENTIAL_SCORE'

    def test_get_greenness_analytics(self):
        dataset = self.prod_client.get_greenness_analytics(
            start_date="2022-01-15",
            end_date="2022-05-31",
            crop=self.crops.CORN,
            sowing_date="2022-01-15",
            geometry="POLYGON ((-54.26027778 -25.38777778, -54.26027778 -25.37444444, -54.26 -25.37416667, -54.25972222 -25.37444444, -54.25944444 -25.37444444, -54.25888889 -25.37472222, -54.258611110000004 -25.37472222, -54.25888889 -25.375, -54.25888889 -25.37555555, -54.258611110000004 -25.37611111, -54.258611110000004 -25.38194444, -54.25833333 -25.38416667, -54.25694444 -25.38361111, -54.25694444 -25.38416667, -54.2575 -25.38416667, -54.2575 -25.38444444, -54.25777778 -25.38416667, -54.25807016 -25.384158120000002, -54.25805556 -25.38444444, -54.258077300000004 -25.38472206, -54.2575 -25.38527778, -54.25694444 -25.385, -54.256388890000004 -25.38361111, -54.25472222 -25.38305555, -54.25472222 -25.3825, -54.254166670000004 -25.38194444, -54.25444444 -25.38166667, -54.25472222 -25.38166667, -54.25472222 -25.37944444, -54.25277778 -25.37944444, -54.25277778 -25.38583333, -54.25419223 -25.3861539, -54.2539067 -25.38589216, -54.25388889 -25.385, -54.25444444 -25.38555555, -54.2547871 -25.385820770000002, -54.25472222 -25.38611111, -54.26027778 -25.38777778))"
        )
        assert dataset.keys()[0] == "Values.peak_found"
        assert dataset.keys()[-1] == 'Schema.Id'
        assert dataset.values[0][-1] == 'GREENNESS'

    def test_get_harvest_readinesss_analytics(self):
        dataset = self.prod_client.get_harvest_readiness_analytics(
            start_date="2022-01-15",
            end_date="2022-05-31",
            crop=self.crops.CORN,
            sowing_date="2022-01-15",
            geometry="POLYGON ((-54.26027778 -25.38777778, -54.26027778 -25.37444444, -54.26 -25.37416667, -54.25972222 -25.37444444, -54.25944444 -25.37444444, -54.25888889 -25.37472222, -54.258611110000004 -25.37472222, -54.25888889 -25.375, -54.25888889 -25.37555555, -54.258611110000004 -25.37611111, -54.258611110000004 -25.38194444, -54.25833333 -25.38416667, -54.25694444 -25.38361111, -54.25694444 -25.38416667, -54.2575 -25.38416667, -54.2575 -25.38444444, -54.25777778 -25.38416667, -54.25807016 -25.384158120000002, -54.25805556 -25.38444444, -54.258077300000004 -25.38472206, -54.2575 -25.38527778, -54.25694444 -25.385, -54.256388890000004 -25.38361111, -54.25472222 -25.38305555, -54.25472222 -25.3825, -54.254166670000004 -25.38194444, -54.25444444 -25.38166667, -54.25472222 -25.38166667, -54.25472222 -25.37944444, -54.25277778 -25.37944444, -54.25277778 -25.38583333, -54.25419223 -25.3861539, -54.2539067 -25.38589216, -54.25388889 -25.385, -54.25444444 -25.38555555, -54.2547871 -25.385820770000002, -54.25472222 -25.38611111, -54.26027778 -25.38777778))"
        )
        assert dataset.keys()[0] == "Values.date"
        assert dataset.keys()[-1] == 'Schema.Id'
        assert dataset.values[0][-1] == 'HARVEST_READINESS'

    def test_get_planted_area_analytics(self):
        dataset = self.prod_client.get_planted_area_analytics(
            start_date="2022-01-15",
            end_date="2022-05-31",
            geometry="POLYGON ((-54.26027778 -25.38777778, -54.26027778 -25.37444444, -54.26 -25.37416667, -54.25972222 -25.37444444, -54.25944444 -25.37444444, -54.25888889 -25.37472222, -54.258611110000004 -25.37472222, -54.25888889 -25.375, -54.25888889 -25.37555555, -54.258611110000004 -25.37611111, -54.258611110000004 -25.38194444, -54.25833333 -25.38416667, -54.25694444 -25.38361111, -54.25694444 -25.38416667, -54.2575 -25.38416667, -54.2575 -25.38444444, -54.25777778 -25.38416667, -54.25807016 -25.384158120000002, -54.25805556 -25.38444444, -54.258077300000004 -25.38472206, -54.2575 -25.38527778, -54.25694444 -25.385, -54.256388890000004 -25.38361111, -54.25472222 -25.38305555, -54.25472222 -25.3825, -54.254166670000004 -25.38194444, -54.25444444 -25.38166667, -54.25472222 -25.38166667, -54.25472222 -25.37944444, -54.25277778 -25.37944444, -54.25277778 -25.38583333, -54.25419223 -25.3861539, -54.2539067 -25.38589216, -54.25388889 -25.385, -54.25444444 -25.38555555, -54.2547871 -25.385820770000002, -54.25472222 -25.38611111, -54.26027778 -25.38777778))"
        )
        assert dataset.keys()[0] == "Values.planted_area"
        assert dataset.keys()[-1] == 'Schema.Id'
        assert dataset.values[0][-1] == 'PLANTED_AREA'

    def test_get_brazil_crop_id_analytics(self):
        dataset = self.prod_client.get_brazil_crop_id_analytics(
            start_date="2020-10-01",
            end_date="2021-05-31",
            season=CropIdSeason.SEASON_1,
            geometry="POLYGON ((-54.26027778 -25.38777778, -54.26027778 -25.37444444, -54.26 -25.37416667, -54.25972222 -25.37444444, -54.25944444 -25.37444444, -54.25888889 -25.37472222, -54.258611110000004 -25.37472222, -54.25888889 -25.375, -54.25888889 -25.37555555, -54.258611110000004 -25.37611111, -54.258611110000004 -25.38194444, -54.25833333 -25.38416667, -54.25694444 -25.38361111, -54.25694444 -25.38416667, -54.2575 -25.38416667, -54.2575 -25.38444444, -54.25777778 -25.38416667, -54.25807016 -25.384158120000002, -54.25805556 -25.38444444, -54.258077300000004 -25.38472206, -54.2575 -25.38527778, -54.25694444 -25.385, -54.256388890000004 -25.38361111, -54.25472222 -25.38305555, -54.25472222 -25.3825, -54.254166670000004 -25.38194444, -54.25444444 -25.38166667, -54.25472222 -25.38166667, -54.25472222 -25.37944444, -54.25277778 -25.37944444, -54.25277778 -25.38583333, -54.25419223 -25.3861539, -54.2539067 -25.38589216, -54.25388889 -25.385, -54.25444444 -25.38555555, -54.2547871 -25.385820770000002, -54.25472222 -25.38611111, -54.26027778 -25.38777778))"
        )

        assert dataset.keys()[0] == "Values.crop_code"
        assert dataset.keys()[-1] == 'Schema.Id'
        assert dataset.values[0][-1] == 'CROP_IDENTIFICATION'

    @pytest.mark.skip(reason="soucis SSL dans github")
    def test_get_zarc_analytics(self):
        dataset = self.client.get_zarc_analytics(
            start_date_emergence="2022-01-15",
            end_date_emergence="2022-05-31",
            nb_days_sowing_emergence=20,
            crop=self.crops.CORN,
            soil_type=ZarcSoilType.NONE,
            cycle=ZarcCycleType.NONE,
            geometry="POLYGON ((-54.26027778 -25.38777778, -54.26027778 -25.37444444, -54.26 -25.37416667, -54.25972222 -25.37444444, -54.25944444 -25.37444444, -54.25888889 -25.37472222, -54.258611110000004 -25.37472222, -54.25888889 -25.375, -54.25888889 -25.37555555, -54.258611110000004 -25.37611111, -54.258611110000004 -25.38194444, -54.25833333 -25.38416667, -54.25694444 -25.38361111, -54.25694444 -25.38416667, -54.2575 -25.38416667, -54.2575 -25.38444444, -54.25777778 -25.38416667, -54.25807016 -25.384158120000002, -54.25805556 -25.38444444, -54.258077300000004 -25.38472206, -54.2575 -25.38527778, -54.25694444 -25.385, -54.256388890000004 -25.38361111, -54.25472222 -25.38305555, -54.25472222 -25.3825, -54.254166670000004 -25.38194444, -54.25444444 -25.38166667, -54.25472222 -25.38166667, -54.25472222 -25.37944444, -54.25277778 -25.37944444, -54.25277778 -25.38583333, -54.25419223 -25.3861539, -54.2539067 -25.38589216, -54.25388889 -25.385, -54.25444444 -25.38555555, -54.2547871 -25.385820770000002, -54.25472222 -25.38611111, -54.26027778 -25.38777778))"
        )

        assert dataset.keys()[0] == "Values.emergence_date"
        assert dataset.keys()[-1] == 'Schema.Id'
        assert dataset.values[0][-1] == 'ZARC'

    def test_get_mr_time_series(self):
        result:str = self.client.get_mr_time_series(
            start_date="2020-10-09",
            end_date="2022-10-09",
            list_sensors=["Sentinel_2", "Landsat_8"],
            denoiser=True,
            smoother="ww",
            eoc=True,
            aggregation="mean",
            index="ndvi",
            raw_data=True,
            polygon="POLYGON ((-0.49881816 46.27330504, -0.49231649 46.27320122, -0.49611449 46.26983426, -0.49821735 46.27094671, -0.49881816 46.27330504))"
        )

        assert result.startswith('s3://geosys-geosys-us/2tKecZgMyEP6EkddLxa1gV')
        assert '/mrts/' in result


    def test_get_farm_info_from_location(self):
        result = self.client.get_farm_info_from_location(
          latitude="-15.01402",
          longitude="-50.7717"
        )
        print(result)
        assert result[0].get('geometry') is not None
        
    def test_retrieve_sfid_from_geometry(self):
        result = self.client.get_sfid_from_geometry(geometry='POLYGON((-96.5130239465625 40.6059966855058,-96.37878474978515 40.6059966855058,-96.37878474978515 40.52044824466329,-96.5130239465625 40.52044824466329,-96.5130239465625 40.6059966855058))')
        assert len(result) == 20
