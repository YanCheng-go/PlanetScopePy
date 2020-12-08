# PlanetScope_tools
Image preprocessing and raster analysis for PlanetScope imagery from Planet Lab.

Preparation
1. Download Pycharm - community version 
2. Download Preparation_for_PlanetScope_tools.zip file
3. Installation of python 3.7.X
    - double click python-3.7.6-amd64.exe in Preparation_for_PlanetScope_tools.zip
    - select customize installation
    - check [Install launcher for all users] 
    - check [Add Python to PATH]
    - make a note of the installed location (e.g., C:\Program Files (x86)\Python37_6)
4. Installation of required packages
    - search and open command prompt 
    - type command line and press enter
        - cd [PATH OF INSTALLED PYTHON 3.7.X, WITHOUT SQUARE BRACKETS] 
        - python -m pip install --user -r [PATH OF DOWNLOADED requirements.txt IN Preparation_for_PlanetScope_tools.zip, 
        WITHOUT SQUARE BRACKETS]
        - python -m pip install --user [PATH OF DOWNLOADED .WHL FILE IN Preparation_for_PlanetScope_tools.zip, WITHOUT 
        SQUARE BRACKETS]
    - find your installed packages here C:\\Users\[USER NAME]\AppData\Roaming\Python\Python37\site-packages
    - make a note of the path of osgeo package in this path... it is associate to the default_gdal_osgeo_dir variable
