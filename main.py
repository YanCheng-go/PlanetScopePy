import Utilities as utils
from glob import glob

# Improt class
ut = utils.Utilities()
# Set work directory
ut.work_dir = r'C:\Users\ChengY\Desktop'
# Set folders for saving different outputs
ut.output_dirs = {'raw': 'raw', 'clip': 'clip', 'clipped_raw': 'clipped_raw',
                  'cloud mask': 'cloud_mask', 'NDVI': 'NDVI'}
ut.setup_dirs()

# Clip images
# Set input directory that includes all data to be clipped
input_dir = r'C:\Users\ChengY\Desktop\raw'
file_list = glob("{}\\*udm2.tif".format(input_dir))
ut.gdal_clip(file_list=file_list)
# Cloud mask
input_dir = r'C:\Users\ChengY\Desktop\clip'
file_list = glob("{}\\*udm2.tif".format(input_dir))
ut.gdal_cal(type_='cloud mask', file_list=file_list)


