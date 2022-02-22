"""Setup script for pygeosys"""

# Standard library imports
import pathlib

# Third party imports
from setuptools import setup

# The directory containing this file
HERE = pathlib.Path(__file__).resolve().parent

# The text of the README file is used as a description
README = (HERE / "README.md").read_text()

# This call to setup() does all the work
setup(
    name="pygeosys",
    version="0.0.1",
    description="Easy-to-use python wrapper for Geosys APIs (time series, imagery products)",
    long_description=README,
    long_description_content_type="text/markdown",
    author="Geosys",
    packages=["pygeosys"],
    include_package_data=True,
    install_requires=["requests", "requests-oauthlib", "oauthlib", "pandas", "python-dotenv"]
)
