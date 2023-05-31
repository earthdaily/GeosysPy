from unittest.mock import patch
from geosyspy.utils.http_client import *



@patch('geosyspy.utils.http_client.HttpClient.get')
def test_get_request_from_api_should_success(get_response):
    get_response.return_value = "HTTP 200 OK"
    client = HttpClient("client_id_123",
                        "client_secret_123456",
                        "username_123",
                        "password_123",
                        "preprod",
                        "na")

    response = client.get(url_endpoint="http://geosys.com", headers={})
    assert client.get.call_count == 1
    assert response == "HTTP 200 OK"


@patch('geosyspy.utils.http_client.HttpClient.post')
def test_post_request_should_success(post_response):
    post_response.return_value = "HTTP 201 OK"
    client = HttpClient("client_id_123",
                        "client_secret_123456",
                        "username_123",
                        "password_123",
                        "preprod",
                        "na")
    payload = {
        "Geometry": "polygon",
        "Crop": {"Id": "CORN"},
        "SowingDate": "2022-01-01",
    }
    response = client.post(url_endpoint="http://geosys.com", payload=payload, headers={})
    assert client.post.call_count == 1
    assert response == "HTTP 201 OK"


@patch('geosyspy.utils.http_client.HttpClient.patch')
def test_patch_request_should_success(patch_response):
    patch_response.return_value = "HTTP 200 OK"
    client = HttpClient("client_id_123",
                        "client_secret_123456",
                        "username_123",
                        "password_123",
                        "preprod",
                        "na")
    payload = {
        "Geometry": "new polygon",
        "Crop": {"Id": "CORN"},
        "SowingDate": "2022-01-01",
    }
    response = client.patch(url_endpoint="http://geosys.com", payload=payload)
    assert client.patch.call_count == 1
    assert response == "HTTP 200 OK"
