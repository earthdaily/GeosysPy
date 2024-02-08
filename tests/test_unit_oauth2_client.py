from unittest.mock import patch, call
from geosyspy.utils.oauth2_client import Oauth2Api


def get_oauth():
    return Oauth2Api("client_id_123",
                     "client_secret_123456",
                     "username_123",
                     "password_123",
                     "preprod",
                     "na")


def test_oauth2_should_initialize():
    oauth = get_oauth()
    assert oauth.client_id == "client_id_123"
    assert oauth.client_secret == "client_secret_123456"
    assert oauth.username == "username_123"
    assert oauth.password == "password_123"
    assert oauth.server_url != ""
    assert oauth.token is None

@patch("geosyspy.utils.oauth2_client.OAuth2Session")
def test_oauth2_get_token_should_work(OAuth2Session):
    OAuth2Session.return_value = OAuth2Session
    oauth = get_oauth()
    OAuth2Session.refresh_token.return_value = "628x9x0xx447xx4x421x517x4x474x33x2065x4x1xx523xxxxx6x7x20"
    oauth.get_refresh_token()
    assert OAuth2Session.refresh_token.call_count == 1
    OAuth2Session.refresh_token.assert_has_calls(
        [
            call(
                oauth.server_url,
                client_id=oauth.client_id,
                client_secret=oauth.client_secret
            )
        ]
    )




