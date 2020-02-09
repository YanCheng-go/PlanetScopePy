import Utilities as utils
from glob import glob

# Improt class
ut = utils.Utilities()

# ===================================          Settings        ======================================#
# You can also change the variables namely default_XXXXXX in the Utilities.py file
# In this case, you do not need to set these variables when you call functions in Utilities.py
# Set environment
ut.gdal_scripts_path = r'C:\Users\ChengY\AppData\Local\Continuum\anaconda3\Lib\site-packages\osgeo\scripts'
ut.gdal_data_path = r'C:\Users\ChengY\AppData\Local\Continuum\anaconda3\Lib\site-packages\osgeo\data\gdal'
ut.gdal_calc_path = r'C:\Users\ChengY\AppData\Local\Continuum\anaconda3\Lib\site-packages\osgeo\scripts\gdal_calc.py'
ut.gdal_merge_path = r'C:\Users\ChengY\AppData\Local\Continuum\anaconda3\Lib\site-packages\osgeo\scripts\gdal_merge.py'
ut.gdal_translate_path = r'C:\Users\ChengY\AppData\Local\Continuum\anaconda3\Lib\site-packages\osgeo\gdal_translate.exe'
# Set work directory
ut.work_dir = r'C:\Users\ChengY\Desktop'
# Set folders for saving different outputs
ut.output_dirs = {'raw': 'raw', 'clip': 'clip', 'clipped_raw': 'clipped_raw',
                  'clear prob': 'clear_prob', 'NDVI': 'NDVI', 'clip clear perc': 'bomas'}
ut.api_key = "9cada8bc134546fe9c1b8bce5b71860f"
ut.satellite = 'PS'
ut.proj_code = 32737
# Filter settings
ut.filter_items = ['date', 'cloud_cover', 'aoi']
ut.item_types = ["PSScene4Band"]
ut.asset_types = ['analytic_sr', 'udm2']
# Set filter
ut.start_date = '2019-01-01'
ut.end_date = '2019-01-31'
ut.cloud_cover = 0.8
ut.aoi_shp = r'D:\Kapiti\supplementary_data\Kapiti_Jun18_v2_prj.shp'


# ===================================       Set up everything          ======================================#
# Create default folders and execution track file
ut.start_up()


# ===================================         All in one        ======================================#
# Download sr and udm2 --> merge --> clip --> calculate clear prob and/or ndvi --> bomas visualization
# the output of one process is the input of next process...
ut.download_assets()
ut.merge()
ut.clip()
ut.band_algebra(output_type='clear prob')
ut.band_algebra(output_type='NDVI')
ut.clip_clear_perc(bomas_shp= r'D:\Kapiti\supplementary_data\Kapiti_Jun18_v2_prj.shp', clear_perc_min=0.8) # Bomas...


# ===================================         Download       ======================================#
# In case you have downloaded some images before using this script and want to save new images in the same folder,
# you can specify the folder by assigning a value to output_dir argument.
# Another easier way to do so is that you can change the work_dir and name of the folder for saving downloaded
# images on line 8 and line 10 in this script
# the default directory is [..\raw], which is the automatically created folder for saving all downloaded images
output_dir = r'C:\Users\ChengY\Desktop\raw'
ut.download_assets(output_dir=output_dir)


# ===================================         Merge        ======================================#
# Set input directory that includes all data to be merged
# the default directory is [..\raw], which is the automatically created folder for saving all downloaded images
input_dir = r'C:\Users\ChengY\Desktop\raw'
file_list = glob("{}\\*udm2.tif".format(input_dir)) # only for udm2
# file_list = glob("{}\\*.tif".format(input_dir)) # for all tif
ut.merge(file_list=file_list)


# ===================================         Clip        ======================================#
# Set input directory that includes all data to be clipped
# the default directory is [..\merge], which is the automatically created folder for saving merged images
input_dir = r'C:\Users\ChengY\Desktop\merge'
file_list = glob("{}\\*udm2.tif".format(input_dir))
ut.clip(file_list=file_list)


# ===================================         Clear probability        ======================================#
# Set input directory
# the default directory is [..\clip], which is the automatically created folder for saving clipped images
input_dir = r'C:\Users\ChengY\Desktop\clip'
file_list = glob("{}\\*udm2.tif".format(input_dir))
ut.band_algebra(input_type='clear prob', file_list=file_list)


# ===================================         Biomas       ======================================#
# Clip based on percentage of clear pixels, for bomas visualization
bomas_shp = r'D:\Kapiti\supplementary_data\Kapiti_Jun18_v2_prj.shp'
clear_perc_min = 0.5
file_list = glob("{}\\*udm2.tif".format(r'C:\Users\ChengY\Desktop\clip'))
ut.clip_clear_perc(bomas_shp, clear_perc_min, file_list)









