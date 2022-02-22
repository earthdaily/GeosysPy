# Analytics Generation Pipeline

Easy-to-use python wrapper for Geosys APIs (time series, imagery products)

## How to run the project locally

### 1. Install dependencies

```
conda config --add channels conda-forge
conda install --file requirements.txt
```

or

```
pip install -r requirements.txt
```

### 2. .env file
You need a .env file to run the example. You can contact (?) to get credentials.

```
API_CLIENT_ID=
API_CLIENT_SECRET=
API_USERNAME=
API_PASSWORD=
```


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