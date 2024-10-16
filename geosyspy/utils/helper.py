""" Helper class"""
import re
import json
from shapely import wkt
from shapely.geometry import shape


class Helper:
    """A class providing helper functions for working with geometries.

    Methods:
        get_matched_str_from_pattern(pattern, text): 
            Returns the first occurrence of the matched pattern in text.
        convert_to_wkt(geometry): Convert a geometry (WKT or geoJson) to WKT.
        is_valid_wkt(geometry): Check if the geometry is a valid WKT.

    """
    @staticmethod
    def get_matched_str_from_pattern(pattern: str,
                                    text: str) -> str:
        """Returns the first occurence of the matched pattern in text.

        Args:
            pattern : A string representing the regex pattern to look for.
            text : The text to look into.

        Returns:
            A string representing the first occurence in text of the pattern.

        """
        p = re.compile(pattern)
        return p.findall(text)[0]

    @staticmethod
    def convert_to_wkt(geometry):
        """ convert a geometry (WKT or geoJson) to WKT
        Args:
            geometry : A string representing the geometry (WKT or geoJson)

        Returns:
            a valid WKT

        """

        try:
            # check if the geometry is a valid WKT
            return geometry if Helper.is_valid_wkt(geometry) else None
        except Exception:
            try:
                # check if the geometry is a valid geoJson
                geojson_data = json.loads(geometry)
                geom = shape(geojson_data)
                geometry = geom.wkt
                return geometry

            except ValueError:
                # geometry is not a valid geoJson
                return None

    @staticmethod
    def is_valid_wkt(geometry):
        """ check if the geometry is a valid WKT
        Args:
            geometry : A string representing the geometry

        Returns:
            boolean (True/False)

        """
        try:
            wkt.loads(geometry)
            return True
        except ValueError:
            return False
        