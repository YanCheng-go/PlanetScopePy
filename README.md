# PlanetScopePy
Image preprocessing and raster analysis for PlanetScope imagery from Planet Lab.

### Functions
1. Download
2. Merge
3. Clip 
4. Bomas -> thumbnails of AOI where the percentage of cloud contaminated pixels is below the user-defined threshold.
5. Band math -> NDVI with the information of the probability of being clear pixel/no cloud contaminations

![conceptual diagram](https://github.com/YanCheng-go/PlanetScope_tools/blob/master/conceptual%20diagram.png)

### Preparation
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
    
### Preparation (Conda virtual environment)
#### Prerequisites
1. install Anaconda or miniconda 

#### Configure virtual environment
1. Open a terminal in the root folder of this python project
```
cd <project directory>
```

2. Install python and dependencies
```
conda env create -f environment.yml
```