import datetime
import json
from unittest.mock import patch

from geosyspy.services.analytics_fabric_service import AnalyticsFabricService
from geosyspy.utils.http_client import *
from tests.test_helper import *

geometry = "POLYGON((-91.17523978603823 40.29787117039518,-91.17577285022956 40.29199489606421,-91.167613719932 40.29199489606421,-91.1673028670095 40.29867040193312,-91.17523978603823 40.29787117039518))"


class TestAnalyticsFabricService:
    url = "https://testurl.com"
    http_client = HttpClient("client_id_123",
                        "client_secret_123456",
                        "username_123",
                        "password_123",
                        "preprod",
                        "na")
    service = AnalyticsFabricService(base_url=url, http_client=http_client)

    def test_get_build_timestamp_query_parameters(self):
        start_date = datetime.datetime.now()
        end_date = start_date + datetime.timedelta(days=7, hours=3, minutes=30)

        timestamp = self.service.build_timestamp_query_parameters(end_date=end_date)
        assert timestamp == f'&Timestamp:$lte:{end_date}'

        timestamp = self.service.build_timestamp_query_parameters(start_date=start_date)
        assert timestamp == f'&Timestamp=$gte:{start_date}'

        timestamp = self.service.build_timestamp_query_parameters(start_date=start_date, end_date=end_date)
        assert timestamp == f'&Timestamp=$between:{start_date}|{end_date}'

        timestamp = self.service.build_timestamp_query_parameters()
        assert timestamp == ''

    @patch('geosyspy.utils.http_client.HttpClient.get')
    def test_get_metrics(self, get_response):
        get_response.return_value = mock_http_response_text_content("GET", load_data_from_textfile(
            "metrics_harvest_mock_http_response"))
        start_date = datetime.datetime.now()
        end_date = start_date + datetime.timedelta(days=7, hours=3, minutes=30)

        metrics = self.service.get_metrics(start_date=start_date, end_date=end_date, season_field_id= 'seasonfieldFakeId', schema_id='HISTORICAL_HARVEST')

        assert metrics['Schema.Id'][0] == 'HISTORICAL_HARVEST'
        assert metrics['Values.harvest_year_1'][0] == '2020-10-02'

    @patch('geosyspy.utils.http_client.HttpClient.get')
    def test_get_latest_metrics(self, get_response):
        get_response.return_value = mock_http_response_text_content("GET", load_data_from_textfile(
            "metrics_harvest_mock_http_response"))

        metrics = self.service.get_lastest_metrics(season_field_id= 'seasonfieldFakeId', schema_id='HISTORICAL_HARVEST')

        assert metrics['Schema.Id'][0] == 'HISTORICAL_HARVEST'
        assert metrics['Values.harvest_year_1'][0] == '2020-10-02'

    @patch('geosyspy.utils.http_client.HttpClient.patch')
    def test_push_metrics(self, patch_response):
        patch_response.return_value = mock_http_response_text_content("PATCH", load_data_from_textfile(
            "metrics_harvest_mock_http_response"))
        data = [{
            "Timestamp": "2022-01-01",
            "Values": {
                "NDVI": 0.5
            }
        }]

        result = self.service.push_metrics(season_field_id='seasonfieldFakeId', schema_id='HISTORICAL_HARVEST', values=data)

        assert result == 200

    @patch('geosyspy.utils.http_client.HttpClient.post')
    def test_create_schema(self, get_response):
        get_response.return_value = mock_http_response_text_content("POST", load_data_from_textfile(
            "create_schema_mock_http_response"), status_code=201)
        schema ={
            "Timestamp": '2021-01-01',
            "Values": {
                "NDVI": 0.5
            }
        }
        response = self.service.create_schema_id(schema_id='NEW_SCHEMA', schema = schema)
        data = json.loads(response.decode('utf-8'))
        assert data['Id'] == "NEW_SCHEMA"

