"""
======================================
Preprocessing for PlanetScope imagery
Author: Yan Cheng
Email:  chengyan2017@gmail.com
Contributors: Dr. Anton Vrieling
======================================
"""
import datetime
import os
import Utilities as utils
from glob import glob
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


# ===================================         Merge        ======================================#
# Set input directory that includes all data to be merged
# the default directory is [..\raw], which is the automatically created folder for saving all downloaded images

# # Merge SR
# input_dir = '/mnt/raid5/California_timeseries/Sierra_Nvada/aoi1'
# file_list = [i for i in glob('/mnt/raid5/California_timeseries/Sierra_Nevada/aoi1/*/PSScene4Band/*SR*.tif')
#              if Path(i).stem.split('_3B_')[0] not in ['20190613_171923_1048', '20190621_172106_104a',
#                                                       '20190623_171851_0f46', '20190721_182401_0f34',
#                                                       '20190828_182009_0f22', '20191010_170118_100d',
#                                                       '20191011_170222_0f46', '20191107_184348_41_1058',
#                                                       '20191111_182203_1034', '20191114_182204_1025']] # udm2 + SR
#
# ut.merge(input_dir=input_dir, file_list=file_list, asset_type_list=['analytic_sr'])

# # Merge udm2
# file_list = [i for i in glob('/mnt/raid5/Planet/pre_processed/Sierra_Nevada_AOI1/raw/*udm*.tif')]
# ut.merge(file_list=file_list, asset_type_list=['udm2'])


# # ===================================         Clip        ======================================#
# # Set input directory that includes all data to be clipped
# # the default directory is [..\merge], which is the automatically created folder for saving merged images
# input_dir = '/mnt/raid5/Planet/pre_processed/Sierra_Nevada_AOI1/merge_2021'
# file_list = glob('{}/*.tif'.format(input_dir))
# aoi_shp = '/mnt/raid5/California_timeseries/aois/sn_aoi1.shp'
#
# # test
# base = datetime.datetime(2019, 1, 1)
# numdays = 30
# date_list_test = [(base + datetime.timedelta(days=x)).strftime('%Y%m%d') for x in range(numdays)]
# file_list_test = []
# for date in date_list_test:
#     fp_list = glob('{}/{}*.tif'.format(input_dir, date))
#     file_list_test.extend(fp_list)
# aoi_test = '/home/yan/Downloads/POLYGON.shp'
#
# ut.clip(file_list=file_list_test, aoi_shp=aoi_test, suffix='_clip_test', discard_empty_scene=True,
#         all_scenes='/mnt/raid5/California_timeseries/Sierra_Nevada/aoi1/sn_aoi1_20190101_20200101_1000_0000.gpkg')


# # ===================================         Stack into a netCDF file       ======================================#

# input_dir = '/mnt/raid5/Planet/pre_processed/Sierra_Nevada_AOI1/clip'
# output_dir = '/mnt/raid5/Planet/pre_processed/Sierra_Nevada_AOI1/stack'
# output_name = 'test_20190102-20190130_test.nc'
#
# # For testing
# base = datetime.datetime(2019, 1, 1)
# numdays = 30
# date_list_test = [(base + datetime.timedelta(days=x)).strftime('%Y%m%d') for x in range(numdays)]
# file_list_test = []
# for date in date_list_test:
#     fp_list = glob('{}/{}*.tif'.format(input_dir, date))
#     file_list_test.extend(fp_list)
# date_list = sorted(list(set([Path(fp).stem.split('_')[0] for fp in file_list_test])))
#
# ref_image = [i for i in file_list_test if 'AnalyticMS_SR' in i][0]
# base_date = '2019-01-01'
# input_suffix = 'AnalyticMS_SR_clip_test'
# udm2 = True
# udm2_suffix = 'udm2_clip_test'
# ref_udm2 = [i for i in file_list_test if 'udm2' in i][0]
# proj = True
#
# ut.stack_as_nc(input_dir, output_dir, output_name, ref_image, base_date, date_list,
#                input_suffix, udm2, udm2_suffix, ref_udm2, proj)
#
# import xarray as xr
# out = xr.open_dataset('/mnt/raid5/Planet/pre_processed/Sierra_Nevada_AOI1/stack/test_20190102-20190130_test.nc')
# print(out)


# # ===================================         Data preparation pipline       ======================================#
# Prepare input datasets for the deep learning models.
# 1. (prerequisite) download raw tiles, i.e., no clipping <- planetmosaic python project.
# 2. (prerequisite) merge tiles acquired the same day and on the same orbit.
# 3. (prerequisite) clip to the extent of AOI. <- clip function
# 4. stack sr and udm2 into one image (separate bands).
# 5. merge stacked images in the same day (regardless of orbits).
# 6. separate sr and udm2.
# 7. stack image time series into one file for sr and udm2 independently (in sequence of acquisition date), and save time stamps into a pickle file.
# 8. (optional) convert CRS and data format.

# ut.prep_pipline(input_dir='/mnt/raid5/Planet/pre_processed/Sierra_Nevada_AOI1/clip_2021',
#                 output_dir='/mnt/raid5/Planet/pre_processed/Sierra_Nevada_AOI1/stack_gdal',
#                 start_date='20190101',
#                 end_date='20190131',
#                 crs=None,
#                 jp2=True,
#                 clean=False,
#                 complex_merge=True)

# # Test iterative_merge()
# input_file_list = ['/mnt/raid5/Planet/pre_processed/Sierra_Nevada_AOI1/stack_sr_udm2/20190102_0f21.tif',
#                    '/mnt/raid5/Planet/pre_processed/Sierra_Nevada_AOI1/stack_sr_udm2/20190102_1001.tif',
#                    '/mnt/raid5/Planet/pre_processed/Sierra_Nevada_AOI1/stack_sr_udm2/20190102_1003.tif',
#                    '/mnt/raid5/Planet/pre_processed/Sierra_Nevada_AOI1/stack_sr_udm2/20190102_1029.tif']
# output_path = 'complex_merge_.tif'
# ut.iterative_merge(input_file_list, output_path)


