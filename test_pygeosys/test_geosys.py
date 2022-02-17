from pygeosys.geosys import Geosys
from dotenv import load_dotenv
import os


# read .env file
load_dotenv()

API_CLIENT_ID = os.getenv('API_CLIENT_ID')
API_CLIENT_SECRET = os.getenv('API_CLIENT_SECRET')
API_USERNAME = os.getenv('API_USERNAME')
API_PASSWORD = os.getenv('API_PASSWORD')


class TestGeosys:

    def test_authenticate(self):
        client = Geosys(API_CLIENT_ID, API_CLIENT_SECRET, API_USERNAME, API_PASSWORD)
        assert set(['access_token', 'expires_in', 'token_type', 'scope', 'expires_at', 'refresh_token']).issubset(set(client.token.keys()))
