'''
======================================
Preprocessing for PlanetScope imagery
Author: Yan Cheng
Email:  chengyan2017@gmail.com
Contributors: Dr. Anton Vrieling
======================================
'''

import Utilities as utils
from glob import glob
import os
from pathlib import Path


# ===================================          Settings        ======================================#
# You can also change the variables namely default_XXXXXX in the Utilities.py file
# In this case, you do not need to set these variables when you call functions in Utilities.py
ut = utils.Utilities(
    gdal_osgeo_dir=str(Path(os.getcwd()) / 'venv/lib/python3.8/site-packages/osgeo'),  # Set environment
    work_dir='/mnt/raid5/Planet/pre_processed/Sierra_Nevada_AOI1',  # Set work directory
    output_dirs={'raw': 'raw', 'clip': 'clip', 'clipped_raw': 'clipped_raw', 'merge': 'merge',
                 'clear prob': 'clear_prob', 'NDVI': 'NDVI'},  # Set folders for saving different outputs
    satellite='PS',
    proj_code=4326,
    dpi=90,
    filter_items=['date', 'cloud_cover', 'aoi'],  # Filter settings
    item_types=["PSScene4Band"],
    process_level='3B',
    asset_types=['analytic_sr', 'udm2'],
    start_date='2019-01-01',
    end_date='2020-01-01',
    cloud_cover=1,
    aoi_shp='/mnt/raid5/California_timeseries/aois/sn_aoi1.shp',
    rgb_composition={'red': 4, 'green': 3, 'blue': 2},  # Settings for raster visualization
    percentile=[2, 98],
    remove_latest=True,  # Set as True only when you killed the previous run and want to rerun it.
    # In this case, the lasted file will be removed in case it is not a complete file...
)


# ===================================       Set up everything          ======================================#
# Create default folders and execution track file
# ut.start_up()
#
#
# # ===================================         All in one        ======================================#
# # Download sr and udm2 --> merge --> clip --> calculate clear prob and/or ndvi --> bomas visualization
# # the output of one process is the input of next process...
# ut.download_assets()
# ut.merge()
# ut.clip()
# ut.band_algebra(output_type='clear prob')
# ut.band_algebra(output_type='NDVI')
# ut.clip_clear_perc(shapefile_path=r'C:\Users\ChengY\PycharmProjects\PyPlanetScope_WD\shp\bomas\layers\POLYGON.shp', clear_perc_min=0.1,
#                    save_rgb=True, save_clip=False) # Bomas...
#
#
# # ===================================         Download       ======================================#
# # In case you have downloaded some images before using this script and want to save new images in the same folder,
# # you can specify the folder by assigning a value to output_dir argument.
# # Another easier way to do so is that you can change the work_dir and name of the folder for saving downloaded
# # images on line 8 and line 10 in this script
# # the default directory is [..\raw], which is the automatically created folder for saving all downloaded images
# output_dir = '/nnfs/Users/Yan/California_timeseries/Sierra_Nevada/aoi1'
# ut.download_assets(output_dir=output_dir)
#
#
# ===================================         Merge        ======================================#
# Set input directory that includes all data to be merged
# the default directory is [..\raw], which is the automatically created folder for saving all downloaded images
# input_dir = '/Users/maverickmiaow/Documents/GitHub/NSFC_CityPhenology'
# file_list = glob("{}\\*SR_clip.tif".format(input_dir)) # only for udm2
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









