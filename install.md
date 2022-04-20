# Installing on Windows

Geosyspy requires **rasterio** to work and rasterio in turn requires **GDAL**. You will need to downlaod the appropriate wheel files for **GDAL** and **rasterio** from [this website that includes all the built wheels for these libraries depending on your python install and your CPU architecture](https://www.lfd.uci.edu/~gohlke/pythonlibs/#gdal).   
<br/>
For instance, if you have a **python 3.9 distribution** installed on a **64 bits Windows machine**, you will need to find and download the files of which the names contain **cp39** (which means CPython 3.9 which comes with the Python3.9 distribution) and **amd64** (which means a machine running on a 64 bits cpu).  
<br/>
If you want to install GDAL 3.4.2,rasterio 1.2.10 and geosyspy on a 64 bits machine running Python3.9, below are the commands to do so :

    pip install -U pip 
    pip install -U setuptools
    pip install GDAL-3.4.2-cp39-cp39-win_amd64
    pip install rasterio-1.2.10-cp39-cp39-win_amd64
    pip install geosyspy
