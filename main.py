'''
======================================
Preprocessing for PlanetScope imagery
Author: Yan Cheng
Email:  y.cheng@utwente.nl
Contributors: Dr. Anton Vrieling
======================================
'''

import Utilities as utils
from glob import glob

# Improt class
ut = utils.Utilities()

# ===================================          Settings        ======================================#
# You can also change the variables namely default_XXXXXX in the Utilities.py file
# In this case, you do not need to set these variables when you call functions in Utilities.py
# Set environment
ut.gdal_osgeo_dir = r'C:\Users\ChengY\AppData\Roaming\Python\Python37\site-packages\osgeo'

# Set work directory
ut.work_dir = r'C:\Users\ChengY\PycharmProjects\PyPlanetScope_WD'
# Set folders for saving different outputs
ut.output_dirs = {'raw': 'raw', 'clip': 'clip', 'clipped_raw': 'clipped_raw', 'merge': 'merge',
                  'clear prob': 'clear_prob', 'NDVI': 'NDVI', 'clip clear perc': 'bomas'}
ut.api_key = "9cada8bc134546fe9c1b8bce5b71860f"
ut.satellite = 'PS'
ut.proj_code = 32737
ut.dpi = 90

# Filter settings
ut.filter_items = ['date', 'cloud_cover', 'aoi']
ut.item_types = ["PSScene4Band"]
ut.asset_types = ['analytic_sr', 'udm2']
# Set filter
ut.start_date = '2020-02-05'
ut.end_date = '2020-02-11'
ut.cloud_cover = 1
ut.aoi_shp = r'C:\Users\ChengY\PycharmProjects\PyPlanetScope_WD\shp\Kapiti\Kapiti_Jun18_v2_prj.shp'
# Settings for raster visualization
ut.rgb_composition = {'red': 4, 'green': 3, 'blue': 2}  # False color composition for PlanetScope images
ut.percentile = [2, 98]


# ===================================       Set up everything          ======================================#
# Create default folders and execution track file
ut.start_up()

#
# # # ===================================         All in one        ======================================#
# # # Download sr and udm2 --> merge --> clip --> calculate clear prob and/or ndvi --> bomas visualization
# # # the output of one process is the input of next process...
# # ut.download_assets()
# # ut.merge()
# # ut.clip()
# # ut.band_algebra(output_type='clear prob')
# # ut.band_algebra(output_type='NDVI')
# # ut.clip_clear_perc(shapefile_path=r'C:\Users\ChengY\PycharmProjects\PyPlanetScope_WD\shp\bomas\layers\POLYGON.shp', clear_perc_min=0.1,
# #                    save_rgb=True, save_clip=False) # Bomas...
#
#
# # ===================================         Download       ======================================#
# # In case you have downloaded some images before using this script and want to save new images in the same folder,
# # you can specify the folder by assigning a value to output_dir argument.
# # Another easier way to do so is that you can change the work_dir and name of the folder for saving downloaded
# # images on line 8 and line 10 in this script
# # the default directory is [..\raw], which is the automatically created folder for saving all downloaded images
# output_dir = r'C:\Users\ChengY\PycharmProjects\PyPlanetScope_WD\raw'
# ut.download_assets(output_dir=output_dir)
#
#
# # ===================================         Merge        ======================================#
# # Set input directory that includes all data to be merged
# # the default directory is [..\raw], which is the automatically created folder for saving all downloaded images
# input_dir = r'C:\Users\ChengY\PycharmProjects\PyPlanetScope_WD\raw'
# file_list = glob("{}\\*udm2.tif".format(input_dir)) # only for udm2
# # file_list = glob("{}\\*.tif".format(input_dir)) # for all tif
# ut.merge(file_list=file_list)
#
#
# # ===================================         Clip        ======================================#
# # Set input directory that includes all data to be clipped
# # the default directory is [..\merge], which is the automatically created folder for saving merged images
# input_dir = r'C:\Users\ChengY\PycharmProjects\PyPlanetScope_WD\merge'
# file_list = glob("{}\\*udm2.tif".format(input_dir))
# ut.clip(file_list=file_list)
#
#
# # ===================================         Clear probability        ======================================#
# # Set input directory
# # the default directory is [..\clip], which is the automatically created folder for saving clipped images
# input_dir = r'C:\Users\ChengY\PycharmProjects\PyPlanetScope_WD\clip'
# file_list = glob("{}\\*udm2.tif".format(input_dir))
# ut.band_algebra(output_type='clear prob', file_list=file_list)


# # ===================================         Bomas       ======================================#
# # Clip based on percentage of clear pixels, for bomas visualization
# bomas_shp = r'C:\Users\ChengY\PycharmProjects\PyPlanetScope_WD\shp\bomas\layers\POLYGON.shp'
# clear_perc_min = 0.1
# save_rgb = True
# save_clip = False
# file_list = glob("{}\\*.tif".format(r'C:\Users\ChengY\PycharmProjects\PyPlanetScope_WD\clip'))
# ut.clip_clear_perc(bomas_shp, clear_perc_min, save_rgb, save_clip, file_list)

# ===================================       Sketch-book      ==================================== #
# ut.download_assets()
# file_list = glob("{}\\*.tif".format(r'C:\Users\ChengY\PycharmProjects\PyPlanetScope_WD\merge'))
# ut.clip(file_list=file_list)









