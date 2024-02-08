import logging
import requests_mock
import requests
import os


def load_data_from_textfile(file_name):
    # valid path for all os system
    file_path = os.path.join(os.path.dirname(__file__), f"resources/{file_name}").replace("\\", "/")
    if os.path.isfile(file_path):
        with open(file_path, 'r') as data:
            return data.read()
    else:
        logging.error(f"Unable to read file: {file_name}")


def load_binary_data_from_zipfile(file_name):
    # valid path for all os system
    file_path = os.path.join(os.path.dirname(__file__), f"resources/{file_name}").replace("\\", "/")
    if os.path.isfile(file_path):
        with open(file_path, 'rb') as data:
            return data.read()
    else:
        logging.error(f"Unable to read zip file: {file_name}")


def mock_http_response_text_content(method, content=None, status_code=200):
    if method == "GET":
        with requests_mock.Mocker() as m:
            m.get('http://geosys.com', text=content)
            return requests.get('http://geosys.com')
    if method == "POST":
        with requests_mock.Mocker() as m:
            m.post('http://geosys.com', text=content, status_code= status_code)
            return requests.post('http://geosys.com' )
    if method == "PATCH":
        with requests_mock.Mocker() as m:
            m.patch('http://geosys.com', text=content)
            return requests.patch('http://geosys.com')


def mock_http_response_binary_content(method, binary_content=None):
    if method == "GET":
        with requests_mock.Mocker() as m:
            m.get(url='http://geosys.com', content=binary_content)
            return requests.get(url='http://geosys.com')
