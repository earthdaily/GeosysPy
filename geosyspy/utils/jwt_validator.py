import jwt
import base64
import json
from datetime import datetime, timezone
from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import logging


logger = logging.getLogger(__name__)


def check_token_validity(token: str, certificate_key: str, algorithms=['RS256']) -> bool:
    """
    Check the validity of a JWT token.

    Args:
        token (str): The JWT token to check.
        certificate_key (str): The certificate key in PEM format.
        algorithms (list, optional): The encryption algorithms to use.
            Default is ['RS256'].

    Returns:
        bool: True if the token is valid, False otherwise.
    """
    try:
        cert_bytes = certificate_key.encode()
        cert = x509.load_pem_x509_certificate(cert_bytes, default_backend())
        public_key = cert.public_key()

        # extract public key in PEM format
        public_key = public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        ).decode()
        aud = __get_audience_from_token(token)
        decoded_token = jwt.decode(token, public_key, algorithms=algorithms, audience=aud)
        expiration_timestamp = decoded_token['exp']
        expiration_datetime = datetime.fromtimestamp(expiration_timestamp, tz=timezone.utc)

        if expiration_datetime > datetime.now(timezone.utc):
            return True
        else:
            return False
    except jwt.ExpiredSignatureError:
        logger.error("Expired Token")
        return False
    except jwt.InvalidTokenError as e:
        logger.error("Invalid Token." + str(e))
        return False
    except Exception as e:
        logger.error("Invalid Token." + str(e))
        return False


def __get_audience_from_token(token: str) -> str:
    """
    Get the audience from a JWT token.

    Args:
        token (str): The JWT token.

    Returns:
        str: The audience value.
    """
    # parse the token (header, payload, signature)
    header, payload, signature = token.split('.')

    # add missing data
    padding = '=' * (4 - len(payload) % 4)
    payload += padding

    # payload decoding & serialization
    decoded_payload = base64.urlsafe_b64decode(payload.encode('utf-8'))
    payload_json = decoded_payload.decode('utf-8')

    payload_dict = json.loads(payload_json)
    audience = payload_dict.get('aud')
    return audience
