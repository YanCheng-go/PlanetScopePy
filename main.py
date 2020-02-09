import Utilities as utils
from glob import glob
from osgeo import gdal
import numpy as np

# Improt class
ut = utils.Utilities()

# Set work directory
ut.work_dir = r'C:\Users\ChengY\Desktop'
# Set folders for saving different outputs
ut.output_dirs = {'raw': 'raw', 'clip': 'clip', 'clipped_raw': 'clipped_raw',
                  'cloud mask': 'cloud_mask', 'NDVI': 'NDVI'}

# Create default folders and execution track file
ut.start_up()

# Set filter
ut.start_date = '2019-01-01'
ut.end_date = '2019-01-31'
ut.cloud_cover = 0.8
ut.aoi_shp = r'D:\Kapiti\supplementary_data\Kapiti_Jun18_v2_prj.shp'

# Download images
# In case you have downloaded some images before using this script and want to save new images in the same folder,
# you can specify the folder by assigning a value to output_dir argument.
# Another easier way to do so is that you can change the work_dir and name of the folder for saving downloaded
# images on line 8 and line 10 in this script
# the default directory is [..\raw], which is the automatically created folder for saving all downloaded images
output_dir = r'C:\Users\ChengY\Desktop\raw'
ut.download_assets(output_dir=output_dir)

# Merge images
# Set input directory that includes all data to be merged
# the default directory is [..\raw], which is the automatically created folder for saving all downloaded images
input_dir = r'C:\Users\ChengY\Desktop\raw'
file_list = glob("{}\\*udm2.tif".format(input_dir))
ut.merge(file_list=file_list)

# Clip images
# Set input directory that includes all data to be clipped
# the default directory is [..\merge], which is the automatically created folder for saving merged images
input_dir = r'C:\Users\ChengY\Desktop\merge'
file_list = glob("{}\\*udm2.tif".format(input_dir))
ut.clip(file_list=file_list)

# Cloud mask
# Set input directory
# the default directory is [..\clip], which is the automatically created folder for saving clipped images
input_dir = r'C:\Users\ChengY\Desktop\clip'
file_list = glob("{}\\*udm2.tif".format(input_dir))
ut.band_algebra(input_type='cloud mask', file_list=file_list)

# Visualize bomas
input_path_list = glob('{}\\{}\\*udm2.tif'.format(ut.work_dir, ut.output_dirs['clip']))
pixel_res = 3
shapefile_path = r'D:\Kapiti\supplementary_data\Kapiti_Jun18_v2_prj.shp'
for input_path in input_path_list:
    output_name = input_path.split('\\')[-1]
    output_path = '/vsimem/' + output_name # save in memory
    ut.gdal_clip(input_path, pixel_res, shapefile_path, output_path)
    raster = gdal.Open(output_path)
    clear_band = np.array(raster.GetRasterBand(1).ReadAsArray())
    clear_band.shape[0] * clear_band.shape[1]
    n_cloud = np.count_nonzero(~np.isnan(clear_band))







