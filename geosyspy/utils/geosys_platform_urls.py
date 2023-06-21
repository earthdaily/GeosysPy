"""
This file contains python dictionaries used to store the Geosys' APIs
URLs for the preprod/prod environments.

"""

IDENTITY_URLS = {
    'na': {
        'preprod': 'https://identity.preprod.geosys-na.com/v2.1/connect/token',
        'prod': 'https://identity.geosys-na.com/v2.1/connect/token'
    }
}
GEOSYS_API_URLS = {
    'na': {
        'preprod': 'https://api-pp.geosys-na.net',
        'prod': 'https://api.geosys-na.net'
    }
}
GIS_API_URLS = {
    'na': {
        'preprod': 'https://gis-services.geosys.com',
        'prod': 'https://gis-services.geosys.com'
    }
}
