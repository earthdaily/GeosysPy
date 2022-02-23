<style>
img[src*="#thumbnail"] {
    width:400px;
    display:block;
    margin: 0 auto;
}
</style>

![geosys logo](https://earthdailyagro.com/wp-content/uploads/2022/01/Logo.svg#thumbnail)


<h1 align="center"> Analytics Generation Pipeline </h1>
<p align="center">
    Easy-to-use python library to request and use imagery products based on the Geosys API.
    <br />
    <a href="https://earthdailyagro.com/"><strong>Who we are</strong></a>
    <br />
</p>
<br />

## About The Project

EarthDaily Agro is the agricultural analysis division of EartDaily Analytics. Learn more about Earth Daily at [EarthDaily Analytics | Satellite imagery & data for agriculture, insurance, surveillance](https://earthdaily.com/).  EarthDaily Agro uses satellite imaging to provide advanced analytics to mitigate risk and increase efficiencies – leading to more sustainable outcomes for the organizations and people who feed the planet.

[![earthdailyagro logo](https://earthdailyagro.com/wp-content/uploads/2022/01/new-logo.png#thumbnail)](https://earthdailyagro.com/geosys/)


Throught our <geosys/> platform, we make geospatial analytics easily accessible for you to be browsed or analyzed, within our cloud or within your own environment. We provide developers and data scientists both flexibility and extensibility with analytic ready data and digital agriculture ready development blocks. We empower your team to enrich your systems with information at the field, regional or continent level via our API or Apps.

We have a team of experts around the world that understand local crops and ag industry, as well as advanced analytics to support your business.

We have established a developer community to provide you with plug-ins and integrations to be able to discover, request and use aggregate imagery products based on Landsat, Sentinel, Modis and many other open and commercial satellite sensors.

The `pygeosys` python package aims to provide an easy and ready to use library allowing any Python developers to quickly experience Earthdaily Agro capabilities.

## Getting started

### Prerequisites

Make sure you have valid credentials. If you need to get trial access, please register [here](https://earthdailyagro.com/geosys-api/#get-started).

This package has been tested on Python 3.9.7.


### Installation

```
pip install pygeosys
```

### Run the package from source

1. Install dependencies

```
conda config --add channels conda-forge
conda install --file requirements.txt
```

2. Create .env file

You need a .env file with your credentials to run the example Jupyter notebook.

```
API_CLIENT_ID=
API_CLIENT_SECRET=
API_USERNAME=
API_PASSWORD=
```

3. Run the Jupyter notebook

## Usage

Initialize client:

```python
from pygeosys.geosys import Geosys

client = Geosys("API_CLIENT_ID", "API_CLIENT_SECRET", "API_USERNAME", "API_PASSWORD")

client.get_time_series(polygon, year_ago, today, "NDVI")
```

Query data:

```python
polygon = "POLYGON((...))"

today = dt.date.today()
year_ago = dt.date.today() + relativedelta(months=-12)

dataframe = client.get_time_series(polygon, year_ago, today, "NDVI")
```

See the Jupyter notebook `examples.ipynb` for a working example.


## License

Distributed under the [GPL 3.0 License](https://www.gnu.org/licenses/gpl-3.0.en.html). 

## Contact

For any additonal information, please [email us](mailto:sales@earthdailyagro.com).

## Copyrights

© 2022 Geosys Holdings ULC, an Antarctica Capital portfolio company | All Rights Reserved.