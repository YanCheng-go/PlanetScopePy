'''
======================================
Utilities for processing PlanetScope imagery
Author: Yan Cheng
Email: chengyan2017@gmail.com
Contributors: Dr. Anton Vrieling
======================================
'''
'''
Major updates
===============================================================
13/02/2020
- remove retrieve_exist_files()
- change the code for checking existing files in clip()
- check existing file for band_algebra()
- add a global variable remove_exist, set this variable as True if you killed a process manually and need to rerun it. 
In this case the latest generated file will be removed because most likely the latest one is not complete...

12/02/2020
0. New preparation instruction
1. new variables
    - default_gdal_osgeo_dir --> the directory of osgeo folder
    - default_process_level
2. os.environ['PROJ_LIB'] = self.gdal_proj_path
3. udm2_setnull()
    - Check existing files
4. merge()
    - check existing merged files in the output direstory
5. clip()
    - check existing clipped files in the output directory
6. asset_attrs(asset_type)
    - change the function name from asset_suffix to asset_attrs
    - Add data type infromation for different asset type
'''

'''
Preparations
=======================================
1. Download Pycharm - community version 
2. Download Preparation_for_PlanetScope_tools.zip file
3. Installation of python 3.7.X
    - double click python-3.7.6-amd64.exe in Preparation_for_PlanetScope_tools.zip
    - select customize installation
    - check [Install launcher for all users] 
    - check [Add Python to PATH
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
    
To do list   
========================================================== 
- Check existing clip_clear_perc()
- Compress data
- Stack NDVI and clear prob
- Plot time series
- Optimize for loops
- Satellite id info from Execution Track file
- gdal warp
'''

from planet import api
from planet.api import filters
from datetime import datetime
import time
import geopandas as gpd
import json
from tqdm import tqdm
from glob import glob
import os
from osgeo import ogr, gdal
import rasterio
import matplotlib.pyplot as plt
import numpy as np
import requests
from requests.auth import HTTPBasicAuth
import sys
import warnings
import netCDF4
import shutil

warnings.simplefilter('ignore')

from pathlib import Path
import string

class Utilities:
    '''
    Commonly used tools for the processing and raster analytics of PlanetScope imagery
    :param:
    '''

    # Set environment
    default_gdal_osgeo_dir = str(Path(os.getcwd()) / 'venv/lib/python3.8/site-packages/osgeo')

    # Set directories
    default_work_dir = '/mnt/raid5/Planet/pre_processed/Sierra_Nevada_AOI1'
    default_output_dirs = {'raw': 'raw', 'clipped raw': 'clipped_raw', 'merge': 'merge', 'clip': 'clip',
                           'clear prob': 'clear_prob', 'NDVI': 'NDVI', 'clip clear perc': 'bomas'}
    # API Key
    default_api_file = str(Path(os.getcwd()) / 'api_key.txt')
    default_api_key = open(default_api_file, 'r').readlines()[0]

    # Specs
    default_satellite = 'PS'
    default_proj_code = 32737
    # Filter settings
    default_filter_items = ['date', 'cloud_cover', 'aoi']
    default_item_types = ["PSScene4Band"]
    default_process_level = '3B'
    default_asset_types = ['analytic_sr', 'udm2']
    default_start_date = '2019-01-01'
    default_end_date = '2020-01-01'
    default_cloud_cover = 1
    default_aoi_shp = '/mnt/raid5/California_timeseries/aois/sn_aoi1.shp'
    default_all_scenes = '/mnt/raid5/California_timeseries/Sierra_Nevada/aoi1/sn_aoi1_20190101_20200101_1000_0000.gpkg'
    # Color composition for visualization
    default_rgb_composition = {'red': 4, 'green': 3, 'blue': 2}  # False color composition for PlanetScope images
    default_dpi = 90
    default_percentile = [2, 98]
    default_remove_latest = True

    def __init__(self, gdal_osgeo_dir=default_gdal_osgeo_dir, work_dir=default_work_dir,
                 output_dirs=default_output_dirs, satellite=default_satellite, proj_code=default_proj_code,
                 api_key=default_api_key, filter_items=default_filter_items, item_types=default_item_types,
                 process_level=default_process_level, asset_types=default_asset_types, start_date=default_start_date,
                 end_date=default_end_date, cloud_cover=default_cloud_cover, aoi_shp=default_aoi_shp,
                 rgb_composition=default_rgb_composition, dpi=default_dpi, percentile=default_percentile,
                 remove_latest=default_remove_latest, all_scenes=default_all_scenes):
        '''

        :param gdal_osgeo_dir: string
        :param work_dir: string
        :param output_dirs: dictionary, the name of folder for storing different outputs
        :param satellite: string, abbreviation of satellite name, PS - PLanetScope, S2 - Sentinel-2
        :param proj_code: int, the EPSG code of projection systtem
        :param api_key: string
        :param filter_items: list, a list of filter item names
        :param item_types: list, a list of item type, more info: https://developers.planet.com/docs/data/items-assets/
        :param process_level: string, processing level
        :param asset_types: list, a list of asset type, more info: https://developers.planet.com/docs/data/psscene4band/
        :param start_date: string, start date with a format of 'YYYY-MM-DD'
        :param end_date: string, end date with a format of 'YYYY-MM-DD'
        :param cloud_cover: float, maximum cloud cover
        :param aoi_shp: string, file path of AOI.shp, better to be projected one
        :param rgb_composition: list, band sequence for rgb color composition, [4, 1, 3] is false color composition for
                                PlanetScope images
        :param dpi: int, dpi of saved plot
        :param percentile: list, minimum and maximum percentile
        :param remove_latest: boolean, true means remove the latest file in the folder because the process was killed
                            manually and the latest file is not complete. If false, the latest file will not be removed.
        '''

        # self.gdal_osgeo_dir = gdal_osgeo_dir
        # self.gdal_scripts_path = str(Path(gdal_osgeo_dir) / 'scripts')
        # self.gdal_data_path = str(Path(gdal_osgeo_dir) / 'data/gdal')
        # self.gdal_proj_path = str(Path(gdal_osgeo_dir) / 'data/proj')
        # sys.path.append(self.gdal_scripts_path)
        # os.environ["GDAL_DATA"] = self.gdal_data_path
        # os.environ['PROJ_LIB'] = self.gdal_proj_path
        self.gdal_calc_path = '/home/yan/anaconda3/envs/PlanetScopePy_new/bin/gdal_calc.py'
        # self.gdal_calc_path = str(Path(gdal_osgeo_dir) / 'scripts/gdal_calc.py')
        self.gdal_merge_path = '/home/yan/anaconda3/envs/PlanetScopePy_new/bin/gdal_merge.py'
        # self.gdal_merge_path = str(Path(gdal_osgeo_dir) / 'scripts/gdal_merge.py')
        self.gdal_vrtmerge_path = str(Path(
            '/home/yan/anaconda3/envs/PlanetScopePy_gdal333/lib/python3.7/site-packages/osgeo_utils/samples/gdal_vrtmerge.py'))
        self.work_dir = work_dir
        self.output_dirs = output_dirs
        self.satellite = satellite
        self.proj_code = proj_code
        self.api_key = api_key
        self.client = api.ClientV1(api_key=api_key)
        self.filter_items = filter_items
        self.item_types = item_types
        self.process_level = process_level
        self.asset_types = asset_types
        self.start_date = datetime(year=int(start_date.split('-')[0]), month=int(start_date.split('-')[1]),
                                   day=int(start_date.split('-')[2]))
        self.end_date = datetime(year=int(end_date.split('-')[0]), month=int(end_date.split('-')[1]),
                                 day=int(end_date.split('-')[2]))
        self.cloud_cover = cloud_cover
        self.aoi_shp = aoi_shp
        self.rgb_composition = rgb_composition
        self.dpi = dpi
        self.percentile = percentile
        self.remove_latest = remove_latest
        self.records_path = None  # File path of execution track document
        self.id_list_download = None  # a list of item id which will be downloaded

        self.all_scenes = all_scenes

    def shp_to_json(self):
        '''
        Convert AOI shapefile to json format that is required for retrieve imagery for specific location
        using Planet API
        :return: aoi_geom, dictionary
        '''

        shp = gpd.read_file(self.aoi_shp)
        if shp.crs != {'init': 'epsg:{}'.format(str(self.proj_code))}:
            shp = shp.to_crs({'init': 'epsg:{}'.format(str(self.proj_code))})
        else:
            shp = shp.to_crs({'init': 'epsg:4326'})
        coors = np.array(dict(json.loads(shp['geometry'].to_json()))
                         ['features'][0]['geometry']['coordinates'])[:, :, 0:2].tolist()
        aoi_geom = {"type": "Polygon", "coordinates": coors}
        # print(self.aoi_geom)
        return aoi_geom

    @staticmethod
    def asset_attrs(asset_type):
        '''
        Attributes, such as suffix and data type, for each asset type
        :param asset_type: string, one item in the list of asset type, more info:
                            https://developers.planet.com/docs/data/psscene4band/
        :return: string, associated attributes of each asset type
        '''

        switch = {
            'analytic_sr': {
                'suffix': 'AnalyticMS_SR',
                'data type': 'UInt16'
            },
            'udm2': {
                'suffix': 'udm2',
                'data type': 'Byte'
            }
        }
        return switch.get(asset_type, 'None')

    @staticmethod
    def pixel_res(satellite_abbr):
        '''
        :param satellite_abbr: string, abbreviation of satellite name, PS - PLanetScope, S2 - Sentinel-2
        :return: int, associated pixel resolution of each satellite imagery
        '''
        switch = {
            'PS': 3,
            'S2': 30
        }
        return switch.get(satellite_abbr, 'None')

    @staticmethod
    def create_dir(f):
        '''
        Create new folder if it does not exist
        :param f: string, the path of a folder to be created
        :return:
        '''
        if not os.path.exists(f):
            os.mkdir(f)

    def setup_dirs(self):
        '''
        Create all required folders for saving different outputs
        :return:
        '''

        print('The outputs will be saved in this directory: ' + self.work_dir)

        for i in self.output_dirs.keys():
            self.create_dir(Path(self.work_dir) / '{}'.format(self.output_dirs[i]))

    def create_track_file(self):
        '''
        Create a txt file to track all the execution of code and outputs, including:
        1. Date
        2. Item id that does not have required types of asset
        3. Item id of downloaded items
        4. Metadata of each downloaded item
        :return:
        '''

        records_path = Path(self.work_dir) / 'Execution_Track.txt'
        time_str = datetime.now().strftime("%Y%m%d-%H%M%S")
        if not os.path.exists(records_path):
            records_file = open(records_path, "w")
            records_file.write('Execution Track for Utilities.py\n\n'.format(time_str))
            records_file.write('Execution date and time: {}\n\n'.format(time_str))
        else:
            records_file = open(records_path, "a+")
            records_file.write('Execution date and time: {}\n\n'.format(time_str))
        records_file.close()
        self.records_path = records_path

    def start_up(self):
        # self.create_track_file()
        self.setup_dirs()

    def create_filter(self):
        '''
        Creater filters
        :return: filter
        '''

        if 'date' in list(self.filter_items):
            date_filter = filters.date_range('acquired', gte=self.start_date, lte=self.end_date)
            and_filter = date_filter
        if 'cloud_cover' in list(self.filter_items):
            cloud_filter = filters.range_filter('cloud_cover', lte=self.cloud_cover)
            and_filter = filters.and_filter(and_filter, cloud_filter)
        if 'aoi' in list(self.filter_items):
            aoi_geom = self.shp_to_json()
            aoi_filter = filters.geom_filter(aoi_geom)
            and_filter = filters.and_filter(and_filter, aoi_filter)
        return and_filter

    def download_one(self, item_id, asset_type, item_type):
        '''
        Download individual asset without using Planet client
        :param item_id: string, item id
        :param asset_type: string, one item in the list of asset type, more info:
                            https://developers.planet.com/docs/data/psscene4band/
        :param item_type: string, one item in the list of item type, more info:
                            https://developers.planet.com/docs/data/items-assets/
        :return: asset_exist, boolean, existence of required asset
        '''

        output_dir = Path(self.work_dir) / self.output_dirs['raw']
        # records_file = open(self.records_path, "a+")

        # Request asset with item id
        item_url = 'https://api.planet.com/data/v1/item-types/{}/items/{}/assets'.format(item_type, item_id)
        result = requests.get(item_url, auth=HTTPBasicAuth(self.api_key, ''))
        # List the asset types available for this particular satellite image
        asset_type_list = result.json().keys()
        # print(asset_type_list)
        if asset_type in asset_type_list:
            links = result.json()[u'{}'.format(asset_type)]['_links']
            self_link = links['_self']
            activation_link = links['activate']
            # Activate asset
            activation = requests.get(activation_link, auth=HTTPBasicAuth(self.api_key, ''))
            # print(activation.response.status_code)
            asset_activated = False
            # i = 0
            while not asset_activated:
                activation_status_result = requests.get(self_link, auth=HTTPBasicAuth(self.api_key, ''))
                asset_status = activation_status_result.json()["status"]
                if asset_status == 'active':
                    asset_activated = True
                    # print("Asset is active and ready to download")
                else:
                    # Still activating. Wait and check again.
                    # print("...Still waiting for asset activation...")
                    # i += 1
                    # print('You have been waiting for {} minutes'.format(i - 1))
                    time.sleep(60)
            # Download asset with download url
            download_url = activation_status_result.json()["location"]
            # print(download_link)
            response = requests.get(download_url, stream=True)
            total_length = response.headers.get('content-length')
            with open(Path(output_dir) / '{}_{}_{}.tif'.format(item_id, self.process_level,
                                                               self.asset_attrs(asset_type)['suffix']), "wb") as handle:
                if total_length is None:
                    for data in response.iter_content():
                        handle.write(data)
                else:
                    dl = 0
                    total_length = int(total_length)
                    for data in response.iter_content(chunk_size=1024):
                        handle.write(data)
                        dl += len(data)
                        done = int(50 * dl / total_length)
                        sys.stdout.write("\r[%s%s]" % ('=' * done, ' ' * (50 - done)))
                        sys.stdout.flush()
            asset_exist = True
        else:
            # records_file.write("NO FILE: {} {} {}\n\n".format(item_id, asset_type, item_type))
            asset_exist = False
        # records_file.close()
        return asset_exist

    def download_client(self, item_id, asset_type, item_type):
        '''
        Activate and download individual asset with specific item id and asset type
        Using Planet client, slower than using download_one()
        :param item_id: string, item id
        :param asset_type: string, one item in the list of asset type, more info:
                            https://developers.planet.com/docs/data/psscene4band/
        :param item_type: string, one item in the list of item type, more info:
                            https://developers.planet.com/docs/data/items-assets/
        :return: asset_exist, boolean, existence of required asset
        '''

        output_dir = Path(self.work_dir) / self.output_dirs['raw']
        # records_file = open(self.records_path, "a+")

        assets = self.client.get_assets_by_id(item_type, item_id).get()
        # print(assets.keys())
        if asset_type in assets.keys():
            # Activate asset
            activation = self.client.activate(assets[asset_type])
            # print(activation.response.status_code)
            asset_activated = False
            # i = 0
            while not asset_activated:
                assets = self.client.get_assets_by_id(item_type, item_id).get()
                asset = assets.get(asset_type)
                asset_status = asset["status"]
                # If asset is already active, we are done
                if asset_status == 'active':
                    asset_activated = True
                    # print("Asset is active and ready to download")
                # Still activating. Wait and check again.
                else:
                    # print("...Still waiting for asset activation...")
                    # i += 1
                    # print('You have been waiting for {} minutes'.format(i - 1))
                    time.sleep(60)
            # Download asset
            callback = api.write_to_file(directory=output_dir)
            body = self.client.download(assets[asset_type], callback=callback)
            body.wait()
            asset_exist = True
        else:
            # records_file.write("{} {} {}\n\n".format(item_id, asset_type, item_type))
            asset_exist = False
        # records_file.close()
        return asset_exist

    def download_clipped(self, item_id, item_type, asset_type='analytic_sr'):
        '''
        Activate and download clipped assets, does not support udm2
        :param item_id: string, item id
        :param asset_type: string, one item in the list of asset type, more info:
                            https://developers.planet.com/docs/data/psscene4band/
        :param item_type: string, one item in the list of item type, more info:
                            https://developers.planet.com/docs/data/items-assets/
        :return: asset_exist, boolean, existence of required asset
        '''

        # Create new folder
        output_dir = Path(self.work_dir) / self.output_dirs['clipped_raw']
        self.create_dir(output_dir)
        # records_file = open(self.records_path, "a+")
        print('The clipped raw images will be saved in this directory: ' + output_dir)
        # Construct clip API payload
        clip_payload = {
            'aoi': self.shp_to_json(),
            'targets': [{
                'item_id': item_id,
                'item_type': item_type,
                'asset_type': asset_type
            }]}
        # Request clip of scene (This will take some time to complete)
        request = requests.post('https://api.planet.com/compute/ops/clips/v1', auth=(self.api_key, ''),
                                json=clip_payload)
        asset_type_list = request.json().keys()
        # print(asset_type_list)
        if asset_type in asset_type_list:
            clip_url = request.json()['_links']['_self']
            # print(request.json())
            # Poll API to monitor clip status. Once finished, download and upzip the scene
            clip_succeeded = False
            while not clip_succeeded:
                # Poll API
                check_state_request = requests.get(clip_url, auth=(self.api_key, ''))
                # If clipping process succeeded , we are done
                if check_state_request.json()['state'] == 'succeeded':
                    clip_download_url = check_state_request.json()['_links']['results'][0]
                    clip_succeeded = True
                    # print("Clip of scene succeeded and is ready to download")
                else:
                    # print("...Still waiting for asset activation...")
                    # i += 1
                    # print('You have been waiting for {} minutes'.format(i - 1))
                    time.sleep(60)
            # Download clipped asset
            response = requests.get(clip_download_url, stream=True)
            total_length = response.headers.get('content-length')
            with open(Path(output_dir) / '{}_{}_{}.tif'.format(item_id, self.process_level,
                                                               self.asset_attrs(asset_type)['suffix']),
                      "wb") as handle:
                if total_length is None:
                    for data in response.iter_content():
                        handle.write(data)
                else:
                    dl = 0
                    total_length = int(total_length)
                    for data in response.iter_content(chunk_size=1024):
                        handle.write(data)
                        dl += len(data)
                        done = int(50 * dl / total_length)
                        sys.stdout.write("\r[%s%s]" % ('=' * done, ' ' * (50 - done)))
                        sys.stdout.flush()
            asset_exist = True
        else:
            # records_file.write("NO FILE: {} {} {}\n\n".format(item_id, asset_type, item_type))
            asset_exist = False
        # records_file.close()
        return asset_exist

    def download_assets(self, clipped=None, output_dir=None):
        '''
        Download all required assets
        :param clipped: boolean, True means downloading clipped assests, otherwise downloading raw imagery
        :param output_dir: string, the directory for saving downloaded assets. In case you have downloaded some assets
        before using this script, this argument can avoid downloading the same assets again.
        :return:
        '''

        print('Start to download assets :)')
        # records_file = open(self.records_path, "a+")
        time_str = datetime.now().strftime("%Y%m%d-%H%M%S")
        # records_file.write('Execute download_assets():\nArguments: clipped={} output_dir={}\nStart time: {}\n\n'
        #                    .format(clipped, output_dir, time_str))
        if output_dir is None:
            output_dir = Path(self.work_dir) / self.output_dirs['raw']

        # Download clipped assets or not
        if clipped is None:
            clipped = False

        # Create filter
        and_filter = self.create_filter()
        # records_file.write('Filter settings: {}\n\n'.format(and_filter))
        # Search items
        req = filters.build_search_request(and_filter, self.item_types)
        res = self.client.quick_search(req)
        # List id of all items in the search result
        id_list_search = [i['id'] for i in res.items_iter(250)]

        for asset_type in self.asset_types:
            # Retrieve id of existing assets in the folder used to save all downloaded assets
            file_list = glob('{}\\*{}.tif'.format(output_dir, self.asset_attrs(asset_type)['suffix']))
            id_list_exist = [i.split('\\')[-1].split('_{}_'.format(self.process_level))[0] for i in file_list]
            # List id of all items to be downloaded
            self.id_list_download = [i for i in id_list_search if i not in id_list_exist]
            # print(self.id_list_download)
            # Download (clipped) assets based on their item id
            for item_id in tqdm(self.id_list_download, total=len(self.id_list_download), unit="item",
                                desc='Downloading assets'):
                if not clipped:
                    for item_type in self.item_types:
                        asset_exist = self.download_one(item_id, asset_type, item_type)
                        if asset_exist is True:
                            metadata = [i for i in res.items_iter(250) if i['id'] == item_id]
                            # records_file = open(self.records_path, "a+")
                            # records_file.write('File Exists: {}_{}_{} {}\n\n'
                            #                    .format(item_id, self.process_level,
                            #                            self.asset_attrs(asset_type)['suffix'], item_type))
                            # records_file.write('Metadata for {}_{}_{} {}\n{}\n\n'
                            #                    .format(item_id, self.process_level,
                            #                            self.asset_attrs(asset_type)['suffix'],
                            #                            item_type, metadata))
                else:
                    for item_type in self.item_types:
                        self.download_clipped(item_id, item_type)

        time_str = datetime.now().strftime("%Y%m%d-%H%M%S")
        print('Finish downloading assets :)')
        print('The raw images have been saved in this directory: ' + output_dir)
        # print('The information of missing assets has be saved in this file: ' + self.records_path)
        # records_file.write('The outputs have been saved in this directory: {}\n\n'.format(output_dir))
        # records_file.write('End time: {}\n\n'.format(time_str))
        # records_file.close()

    def gdal_udm2_setnull(self, input_path, output_path):
        '''
        Set the value of background pixels as no data
        :param input_path:
        :param output_path:
        :return:
        '''

        temp_output_path = Path(self.work_dir) / 'TempFile.tif'
        gdal_calc_str = 'python {0} --calc "A+B+C+D+E+F+G" --co "COMPRESS=LZW" --format GTiff ' \
                        '--type Byte -A {1} --A_band 1 -B {2} --B_band 2 -C {3} --C_band 3 -D {4} --D_band 4 ' \
                        '-E {5} --E_band 5 -F {6} --F_band 6 -G {7} --G_band 7 --NoDataValue 0.0 ' \
                        '--outfile {8} --overwrite'
        gdal_calc_process = gdal_calc_str.format(self.gdal_calc_path, input_path, input_path, input_path, input_path,
                                                 input_path, input_path, input_path, temp_output_path)
        os.system(gdal_calc_process)

        gdal_calc_str = 'python {0} --calc "A*(B>0)" --co "COMPRESS=LZW" --format GTiff ' \
                        '--type Byte -A {1} -B {2} --outfile {3} --allBands A --overwrite'
        gdal_calc_process = gdal_calc_str.format(self.gdal_calc_path, input_path, temp_output_path, output_path)
        os.system(gdal_calc_process)
        os.remove(temp_output_path)

    def udm2_setnull(self, file_list=None):
        '''
        Set the value of background pixels as no data
        :param file_list:
        :return:
        '''

        print('Start to process udm2 data :)')
        # records_file = open(self.records_path, "a+")
        time_str = datetime.now().strftime("%Y%m%d-%H%M%S")
        # records_file.write('Execute udm2_setnull():\nArguments: file_list={}\nStart time: {}\n\n'
        #                    .format(file_list, time_str))

        input_dir = str(Path(self.work_dir) / self.output_dirs['raw'])
        output_dir = input_dir

        if file_list is None:
            file_list = []
            for i in self.id_list_download:
                a = glob(str(Path('{}/{}*udm2.tif'.format(input_dir, i))))
                for j in a:
                    file_list.append(j)
        else:
            file_list = [file for file in file_list if 'udm2' in file]
        # print(file_list)

        # Check existing setnull data and remove the latest one in case it was not complete
        item_id_list = list(set([Path(file).stem.split('_{}_'.format(self.process_level))[0]
                                 for file in file_list]))
        exist_setnull = list(
            set([Path(file).stem.split('_{}_'.format(self.process_level))[0]
                 for file in glob(str(Path('{}/*setnull*.tif'.format(output_dir))))]))
        new_setnull = [i for i in item_id_list if i not in exist_setnull]
        if new_setnull:
            if exist_setnull:
                file_list_exist_setnull = glob(str(Path('{}/*setnull.tif'.format(output_dir))))
                if self.remove_latest is True:
                    latest_file = max(file_list_exist_setnull, key=os.path.getctime)
                    # Remove the latest file, in case it is not complete
                    os.remove(latest_file)
                    file_list_exist_setnull.remove(latest_file)
                # Add the removed one to the file list
                new_setnull.append(Path(latest_file).stem.split('_{}_'.format(self.process_level))[0])
        file_list = [file for file in file_list for i in new_setnull if i in file]

        for input_path in tqdm(file_list, total=len(file_list), unit="item", desc='Processing udm2 data'):
            output_path = str(Path(output_dir) / str(Path(input_path).stem.split('.')[0] + '_setnull.tif'))
            try:
                self.gdal_udm2_setnull(input_path, output_path)
            except:
                pass

        time_str = datetime.now().strftime("%Y%m%d-%H%M%S")
        print('Finish processing udm2 data :)')
        print('The outputs have been saved in this directory: ' + output_dir)
        # records_file.write('The outputs have been saved in this directory: {}\n\n'.format(output_dir))
        # records_file.write('End time: {}\n\n'.format(time_str))
        # records_file.close()

    def gdal_merge(self, input_path, output_path, data_type, separate=False, compression=None):
        '''
        GDAL merge function. More info: https://gdal.org/programs/gdal_merge.html
        :param output_path:
        :param input_path:
        :param data_type:
        :return:
        '''

        gdal_merge_str = 'python {0} -o {1} {2} -ot {3}' if separate is False \
            else 'python {0} -o {1} {2} -ot {3} -separate'
        gdal_merge_str = ' '.join([gdal_merge_str, f'-co COMPRESS={compression}']) if compression is not None else gdal_merge_str
        gdal_merge_process = gdal_merge_str.format(self.gdal_merge_path, output_path, input_path, data_type)
        os.system(gdal_merge_process)

    def merge(self, input_dir=None, file_list=None, asset_type_list=default_asset_types):
        '''
        Merge images acquired in the same day with the same satellite id
        :param input_dir: string, input folder
        :param file_list: list, a list of file path
        :param asset_type_list: list, a list of asset types
        :return:
        '''

        if 'udm2' in asset_type_list:
            # Preprocessing udm2 data
            self.udm2_setnull(file_list)
        else:
            pass

        print('Start to merge images collected in the same day on the same orbit :)')
        # records_file = open(self.records_path, "a+")
        time_str = datetime.now().strftime("%Y%m%d-%H%M%S")
        # records_file.write('Execute merge():\nArguments: file_list={}\nStart time: {}\n\n'.format(file_list, time_str))
        input_dir = str(Path(self.work_dir) / self.output_dirs['raw']) if input_dir is None else input_dir
        output_dir = str(Path(self.work_dir) / self.output_dirs['merge'])

        if file_list is None:
            file_list = []
            for i in self.id_list_download:
                a = glob(str(Path('{}/{}*.tif'.format(input_dir, i))))
                for j in a:
                    file_list.append(j)
            # print(file_list)

        for asset_type in asset_type_list:
            date_list = list(set([Path(file).stem.split('_')[0]
                                  for file in file_list if self.asset_attrs(asset_type)['suffix'] in file]))
            # Check existing merged data and remove the latest file in case it was not complete
            file_list_exist = glob(str(Path('{}/*{}.tif'.format(output_dir, self.asset_attrs(asset_type)['suffix']))))
            if file_list_exist:
                date_list_exist = list(set([Path(file).stem.split('_')[0] for file in file_list_exist]))
                if self.remove_latest is True:
                    latest_file = max(file_list_exist, key=os.path.getctime)
                    latest_file_date = Path(latest_file).stem.split('_')[0]
                    os.remove(latest_file)
                    date_list_exist.remove(latest_file_date)
                date_list = [date for date in date_list if date not in date_list_exist]

            for date in tqdm(date_list, total=len(date_list), unit="item", desc='Merging images'):
                if asset_type == 'udm2':
                    file_list_new = glob(str(Path('{}/{}*{}*_setnull.tif'
                                                  .format(input_dir, date, self.asset_attrs(asset_type)['suffix']))))
                else:
                    file_list_new = [i for i in file_list if
                                     date in Path(i).stem and self.asset_attrs(asset_type)['suffix'] in Path(i).stem]
                satellite_id_list = list(set([Path(x).stem.split('_{}_'.format(self.process_level))[0]
                                             .split('_')[-1] for x in file_list_new]))
                for satellite_id in satellite_id_list:
                    output_path = str(Path(output_dir) / str(date + '_' + satellite_id + '_{}.tif'
                                                             .format(self.asset_attrs(asset_type)['suffix'])))
                    input_path = ' '.join(str(i) for i in file_list_new if satellite_id in i)
                    self.gdal_merge(input_path, output_path, self.asset_attrs(asset_type)['data type'])

        time_str = datetime.now().strftime("%Y%m%d-%H%M%S")
        print('Finish merging images :)')
        print('The merged images have been saved in this directory: ' + output_dir)
        # records_file.write('The outputs have been saved in this directory: {}\n\n'.format(output_dir))
        # records_file.write('End time: {}\n\n'.format(time_str))
        # records_file.close()

    def gdal_clip(self, input_path, pixel_res, shapefile_path, output_path, data_type):
        '''
        GDAL clip function
        :param input_path:
        :param pixel_res:
        :param shapefile_path:
        :param data_type:
        :param output_path:
        :return:
        '''

        # Open datasets
        raster = gdal.Open(input_path, gdal.GA_ReadOnly)
        # Projection = Raster.GetProjectionRef()
        # print(Projection)
        vector_driver = ogr.GetDriverByName('ESRI Shapefile')
        vector_dataset = vector_driver.Open(shapefile_path, 0)  # 0=Read-only, 1=Read-Write
        layer = vector_dataset.GetLayer()
        feature = layer.GetFeature(0)
        geom = feature.GetGeometryRef()
        minX, maxX, minY, maxY = geom.GetEnvelope()  # Get bounding box of the shapefile feature
        if data_type == 'UInt16':
            gdal_data_type = gdal.GDT_UInt16
        if data_type == 'Byte':
            gdal_data_type = gdal.GDT_Byte
        # Create raster
        OutTile = gdal.Warp('temp.vrt', raster, format='VRT',
                            outputType=gdal_data_type,
                            outputBounds=[minX, minY, maxX, maxY],
                            xRes=pixel_res, yRes=pixel_res,
                            targetAlignedPixels=True,
                            # dstSRS='epsg:{}'.format(str(self.proj_code)),
                            resampleAlg=gdal.GRA_NearestNeighbour,
                            cutlineDSName=shapefile_path,
                            cutlineLayer=Path(shapefile_path).stem.split('.')[0],
                            cropToCutline=True,
                            # dstNodata=-9999,
                            options=['COMPRESS=LZW'])

        # Compression
        translateoptions = gdal.TranslateOptions(gdal.ParseCommandLine("-of Gtiff -co COMPRESS=LZW"))
        gdal.Translate(output_path, 'temp.vrt', options=translateoptions)
        # gdal_translate_str = 'python {0} -co COMPRESS=LZW temp.vrt {1}'
        # gdal_translate_process = gdal_translate_str.format(self.gdal_translate_path, output_path)
        # os.system(gdal_translate_process)

        # Close dataset
        OutTile = None
        raster = None
        vector_dataset.Destroy()

    @staticmethod
    def get_aoi_scenes(all_scenes, aoi):
        """
        Retrieve scenes intersect with an aoi.
        :param all_scenes:
        :param aoi:
        :return: GeoDataFrame,
        """

        all_scenes_gdf = gpd.GeoDataFrame.from_file(all_scenes)
        aoi_gdf = gpd.GeoDataFrame.from_file(aoi)
        out = gpd.overlay(all_scenes_gdf, aoi_gdf, how='intersection')
        return out

    def clip(self, file_list=None, aoi_shp=None, suffix='', discard_empty_scene=None, all_scenes=None):
        '''
        Clip imagery to the extent of AOI
        :param file_list:
        :return:
        '''

        print('Start GDAL clip :)')
        # records_file = open(self.records_path, "a+")
        time_str = datetime.now().strftime("%Y%m%d-%H%M%S")
        # records_file.write('Execute clip():\nArguments: file_list={}\nStart time: {}\n\n'.format(file_list, time_str))
        input_dir = str(Path(self.work_dir) / self.output_dirs['merge'])
        output_dir = str(Path(self.work_dir) / self.output_dirs['clip'])

        aoi_shp = self.aoi_shp if aoi_shp is None else aoi_shp

        if file_list is None:
            file_list = []
            for i in self.id_list_download:
                a = glob(str(Path('{}/{}*.tif'.format(input_dir, i))))
                for j in a:
                    file_list.append(j)
            # print(file_list)

        # Check existing clipped images and remove the latest file, in case it is not complete
        file_list_exist = glob(str(Path('{}/*.tif'.format(output_dir))))
        if file_list_exist:
            if self.remove_latest is True:
                latest_file = max(file_list_exist, key=os.path.getctime)
                os.remove(latest_file)
                file_list_exist.remove(latest_file)
            file_list = [file for file in file_list if file not in file_list_exist]

        if discard_empty_scene is True:
            all_scenes = self.all_scenes if all_scenes is None else all_scenes
            overlayed_gdf = self.get_aoi_scenes(all_scenes=all_scenes, aoi=aoi_shp)
            date_orbit_list = overlayed_gdf['id'].apply(lambda x: x.split('_')).apply(lambda x: '_'.join([x[0], x[-1]])).tolist()
            file_list = [fp for date_orbit in date_orbit_list for fp in file_list if date_orbit in fp]

        for input_path in tqdm(file_list, total=len(file_list), unit="item", desc='Clipping images'):
            output_name = str(Path(input_path).stem) + f'{suffix}' + str(Path(input_path).suffix)
            output_path = str(Path(output_dir) / output_name)
            if self.asset_attrs('udm2')['suffix'] in input_path:
                data_type = self.asset_attrs('udm2')['data type']
            if self.asset_attrs('analytic_sr')['suffix'] in input_path:
                data_type = self.asset_attrs('analytic_sr')['data type']
            self.gdal_clip(input_path, self.pixel_res(self.satellite), aoi_shp, output_path, data_type)

        time_str = datetime.now().strftime("%Y%m%d-%H%M%S")
        print('Finish clipping images :)')
        print('The clipped images have been saved in this directory: ' + output_dir)
        # records_file.write('The outputs have been saved in this directory: {}\n\n'.format(output_dir))
        # records_file.write('End time: {}\n\n'.format(time_str))
        # records_file.close()

    def gdal_calc_ndvi(self, input_path, output_path):
        '''
        Band algebra for NDVI
        :param input_path:
        :param output_path:
        :return:
        '''
        gdal_calc_str = 'python {0} --calc "(A-B)/(A+B)*10000" --co "COMPRESS=LZW" --format GTiff ' \
                        '--type UInt16 -A {1} --A_band 4 -B {2} --B_band 3 --outfile {3} --overwrite'
        gdal_calc_process = gdal_calc_str.format(self.gdal_calc_path, input_path, input_path, output_path)
        os.system(gdal_calc_process)

    def gdal_calc_clear_prob(self, input_path, output_path):
        '''
        Band algebra for clear probability
        :param input_path:
        :param output_path:
        :return:
        '''
        gdal_calc_str = 'python {0} --calc "A*B" --co "COMPRESS=LZW" --format GTiff --type UInt16 -A {1} ' \
                        '--A_band 1 -B {2} --B_band 7 --outfile {3} --overwrite'
        gdal_calc_process = gdal_calc_str.format(self.gdal_calc_path, input_path, input_path, output_path)
        os.system(gdal_calc_process)

    def band_algebra(self, output_type, file_list=None):
        '''
        Band algebra for clear probablity or NDVI
        :param output_type:
        :param file_list:
        :return:
        '''

        print('Start GDAL band calculation :)')
        # records_file = open(self.records_path, "a+")
        time_str = datetime.now().strftime("%Y%m%d-%H%M%S")
        # records_file.write('Execute band_algebra():\nArguments: output_type={} file_list={}\nStart time: {}\n\n'
        #                    .format(output_type, file_list, time_str))
        input_dir = self.work_dir + '\\' + self.output_dirs['clip']

        if output_type == 'clear prob':
            asset_type = 'udm2'
            clear_prob_dir = self.work_dir + '\\' + self.output_dirs['clear prob']
            if file_list is None:
                file_list = []
                for i in self.id_list_download:
                    a = glob("{}\\{}*{}.tif".format(input_dir, i, self.asset_attrs('udm2')['suffix']))
                    for j in a:
                        file_list.append(j)
            else:
                file_list = [file for file in file_list if self.asset_attrs('udm2')['suffix'] in file]

            # Check existing clipped images and remove the latest file, in case it is not complete
            file_list_exist = glob('{}\\*.tif'.format(clear_prob_dir))
            if file_list_exist:
                item_id_list_exist = [
                    file.split('\\')[-1].split('_{}'.format(self.asset_attrs(asset_type)['suffix']))[0]
                    for file in file_list_exist]
                if self.remove_latest is True:
                    latest_file = max(file_list_exist, key=os.path.getctime)
                    os.remove(latest_file)
                    file_list_exist.remove(latest_file)
                file_list = [file for file in file_list if file.split('\\')[-1].split(
                    '_{}'.format(self.asset_attrs(asset_type)['suffix']))[0] not in item_id_list_exist]

            for udm2_path in file_list:
                clear_prob_path = clear_prob_dir + '\\' + \
                                  udm2_path.split('\\')[-1].split('_{}'.format(self.asset_attrs('udm2')['suffix']))[
                                      0] + '_clearprob.tif'
                self.gdal_calc_clear_prob(input_path=udm2_path, output_path=clear_prob_path)
            print('Finish GDAL Calculation :)')
            print('The outputs have been saved in this directory: ' + clear_prob_dir)

        if output_type == 'NDVI':
            ndvi_dir = Path(self.work_dir) / self.output_dirs['NDVI']
            if file_list is None:
                file_list = []
                for i in self.id_list_download:
                    a = glob("{}\\{}*SR.tif".format(input_dir, i))
                    for j in a:
                        file_list.append(j)
            else:
                file_list = [file for file in file_list if 'SR' in file]
            for sr_path in file_list:
                ndvi_path = Path(ndvi_dir) / sr_path.split('\\')[-1].split(
                    '_{}'.format(self.asset_attrs('analytic_sr')['suffix']))[0] + '_ndvi.tif'
                self.gdal_calc_ndvi(input_path=sr_path, output_path=ndvi_path)
            time_str = datetime.now().strftime("%Y%m%d-%H%M%S")
            print('Finish GDAL Calculation :)')
            print('The outputs have been saved in this directory: ' + ndvi_dir)
            # records_file.write('The outputs have been saved in this directory: {}\n\n'.format(ndvi_dir))
            # records_file.write('End time: {}\n\n'.format(time_str))
            # records_file.close()

    def stack_as_nc(self, input_dir, output_dir, output_name, ref_image, base_date='19000101', date_list=None,
                    input_suffix=None, udm2=None, udm2_suffix=None, ref_udm2=None, proj=True):
        """

        :param base_date: string, in the form of 'yyyy-mm-dd'
        :return:
        """

        print('Start!')
        if udm2_suffix is None:
            udm2_suffix = ''
        if input_suffix is None:
            input_suffix = ''
        if date_list is None:
            fp_list_all = glob(os.path.join(input_dir, '*.tif'))
        else:
            fp_list_all = [fp for fp in glob(os.path.join(input_dir, '*.tif'))
                           if Path(fp).stem.split('_')[0] in date_list]
        date_orbit_list = sorted(list(
            set(['_'.join([str(Path(fp).stem.split('_')[0]), str(Path(fp).stem.split('_')[1])]) for fp in fp_list_all])))


        ds = gdal.Open(ref_image)  # reference -> could be any image from the candidate images to be stacked into a netCDF file
        # get the number of bands
        n_band = int(ds.RasterCount)
        # convert image to array
        a = ds.ReadAsArray()
        # get the number of rows and columns
        if int(n_band) == 1:
            ny, nx = np.shape(a)
        else:
            ny, nx = np.shape(a[0])

        # get the information of coordinate transformation
        b = ds.GetGeoTransform()  # bbox, interval
        prj = ds.GetProjection()
        # calculate coordinates
        x = np.arange(nx)*b[1]+b[0] if proj is True else range(nx)
        y = np.arange(ny)*b[5]+b[3] if proj is True else range(ny)
        ds = None

        # the date of the first image
        basedate = datetime.strptime(base_date, '%Y-%m-%d')

        # create NetCDF file
        nco = netCDF4.Dataset(str(Path(output_dir) / output_name), 'w', clobber=True)

        # chunking is optional, but can improve access a lot:
        # (see: http://www.unidata.ucar.edu/blogs/developer/entry/chunking_data_choosing_shapes)
        chunk_x = 16
        chunk_y = 16
        chunk_time = 12

        # create dimensions, variables and attributes:
        nco.createDimension('x', nx)
        nco.createDimension('y', ny)
        nco.createDimension('time', None)

        # assign crs
        crs = nco.createVariable('spatial_ref', 'i4')
        crs.spatial_ref = prj

        timeo = nco.createVariable('time', 'u1', ('time'))
        timeo.units = 'day'
        timeo.standard_name = f'days since {basedate}'

        xo = nco.createVariable('x', 'u4', ('x'))
        xo.units = 'm' if proj is True else ''
        xo.standard_name = 'projection_x_coordinate' if proj is True else 'column_id'

        yo = nco.createVariable('y', 'u4', ('y'))
        yo.units = 'm' if proj is True else ''
        yo.standard_name = 'projection_y_coordinate' if proj is True else 'row_id'

        # create variable for surface reflectance, quality layers, and additional information (metadata), with chunking
        def create_variables(var, name, fmt, dims=('time', 'y', 'x'), chunksizes=[chunk_time, chunk_y, chunk_x],
                             fill_value=None, least_significant_digit=None):
            out = nco.createVariable(var, fmt, dims, chunksizes=chunksizes, zlib=True,
                                     fill_value=fill_value, least_significant_digit=least_significant_digit)
            out.standard_name = name
            return out
        # surface reflectance
        sro_list = list([create_variables(f'B{i+1}', f'B{i+1}', 'u2') for i in range(n_band)])
        # quality bands
        if udm2 is True:
            ds = gdal.Open(ref_udm2)
            n_qa = int(ds.RasterCount)
            ds = None
            qao_list = list([create_variables(f'B{n_band+i+1}', f'UDM2_{i+1}', 'u1') for i in range(n_qa)])
        # metadata
        mdo = create_variables('orbit', 'orbit', str, dims=('time'), chunksizes=[chunk_time])

        nco.Conventions = 'CF-1.6'

        # write x,y
        xo[:] = x
        yo[:] = y

        # step through data, writing time and data to NetCDF
        def func(date_orbit):
            # read the time values by parsing the filename
            year = int(date_orbit[0:4])
            mon = int(date_orbit[4:6])
            day = int(date_orbit[6:8])
            date_ = datetime(year, mon, day, 0, 0, 0)
            dtime = (date_ - basedate).total_seconds() / 86400.
            itime = date_orbit_list.index(date_orbit)
            timeo[itime] = dtime
            # metadata information
            orbit_id = date_orbit.split('_')[1]
            mdo[itime] = orbit_id
            # surface reflectance
            sr_fp = glob(os.path.join(input_dir, f'{date_orbit}*{input_suffix}.tif'))[0]
            ds = gdal.Open(sr_fp)
            #             print(input_dir+'\\'+ fileName)
            sr = ds.ReadAsArray()  # data
            ds = None
            for band_idx in range(n_band):
                sro_list[band_idx][itime, :, :] = sr[band_idx]
            # quality information
            if udm2 is True:  # after or before masking out clouds
                udm2_fp = glob(os.path.join(input_dir, f'{date_orbit}*{udm2_suffix}.tif'))[0]
                ds = gdal.Open(udm2_fp)
                qa = ds.ReadAsArray()  # data
                ds = None
                for qa_idx in range(n_qa):
                    qao_list[qa_idx][itime, :, :] = qa[qa_idx]

        list(map(func, tqdm(date_orbit_list, total=len(date_orbit_list), unit='file', desc='tif_to_netCDF')))

        print('Done!')
        print('Check your output: ' + output_dir)
        nco.close()

    # def gdal_vrtmerge(self, out_filename, data_type, input_file_list, separate=False):
    #     # has bugs to be fixed
    # Error -> -ot unrecognized
    #     gdal_vrtmerge_str = 'python {0} -o {1} -ot {2} {3}' if separate is False else \
    #         'python {0} -o {1} -separate -ot {2} {3}'
    #     gdal_vrtmerge_process = gdal_vrtmerge_str.format(self.gdal_vrtmerge_path, out_filename, data_type,
    #                                                      ' '.join(input_file_list))
    #     os.system(gdal_vrtmerge_process)

    # def gdal_vrtmerge(self, out_filename, data_type, input_file_list, separate=False):
    #     # has bugs to be fixed
    #     # ERROR 1: Writing through VRTSourcedRasterBand is not supported.
    #     gdal_vrtmerge_str = 'python {0} -o {1} -ot {2} {3} -of VRT' if separate is False else \
    #         'python {0} -o {1} -separate -ot {2} {3} -of VRT'
    #     gdal_vrtmerge_process = gdal_vrtmerge_str.format(self.gdal_merge_path, out_filename, data_type,
    #                                                      ' '.join(input_file_list))
    #     os.system(gdal_vrtmerge_process)

    @staticmethod
    def gdal_progress_callback(complete, message, data):
        if data:
            data.update(int(complete * 100) - data.n)
            if complete == 1:
                data.close()
        return 1

    def complex_gdal_merge(self, input_path0, input_path1, output_path=None):
        """
        merge two images based on cloud probability in udm2
        :param input_path0:a
        :param input_path1:
        :param output_path:
        :return:
        """

        # mask individual image
        masked_img0 = 'TempFile0.tif'
        masked_img1 = 'TempFile1.tif'
        cal_exp0 = '"Y*(logical_or(' \
                   'logical_and(logical_and((A*B*C*D*E*F*G*H*I*J*K*L)!=1, (A+B+C+D+E+F+G+H+I+J+K+L)!=0), logical_and((E*K+Q*W)==0, K>=W)), ' \
                   'logical_and((A*B*C*D*E*F*G*H*I*J*K*L)!=1, logical_and((E*K+Q*W)>0, (E*K)>=(Q*W)))' \
                   '))"'
        cal_exp1 = '"Z*(logical_or(' \
                   'logical_and(logical_and((M*N*O*P*Q*R*S*T*U*V*W*X)!=1, (M+N+O+P+Q+R+S+T+U+V+W+X)!=0), logical_and((E*K+Q*W)==0, K<W)), ' \
                   'logical_and((M*N*O*P*Q*R*S*T*U*V*W*X)!=1, logical_and((E*K+Q*W)>0, (E*K)<(Q*W)))' \
                   '))"'
        str0 = ' '.join(
            ["-{} {} --{}_band {}".format(string.ascii_uppercase[i], '{2}', string.ascii_uppercase[i], i + 1) for i in
             list(range(12))])
        str1 = ' '.join(
            ["-{} {} --{}_band {}".format(string.ascii_uppercase[i+12], '{3}', string.ascii_uppercase[i+12], i + 1) for i in
             list(range(12))])
        gdal_calc_str = 'python {0} --calc {1} -Y {2} -Z {3} --allBands {4} --outfile {5} --co "COMPRESS=LZW" ' \
                        '--overwrite' + ' ' + str0 + ' ' + str1

        for (current_img, cal_exp, masked_img) in list(zip(['Y', 'Z'], [cal_exp0, cal_exp1],
                                                            [masked_img0, masked_img1])):
            gdal_calc_process = gdal_calc_str.format(self.gdal_calc_path, cal_exp, input_path0, input_path1,
                                                     current_img, masked_img)
            os.system(gdal_calc_process)

        # merge images
        input_file_list = [masked_img0, masked_img1]
        temp_filepath = 'TempFile.tif'
        output_path = temp_filepath if output_path is None else output_path
        gdal_merge_str = 'python {0} -n 0.0 -o {1} {2} -co COMPRESS=LZW'
        gdal_merge_process = gdal_merge_str.format(self.gdal_merge_path, output_path, ' '.join(input_file_list))
        os.system(gdal_merge_process)
        list([os.remove(fp) for fp in [masked_img0, masked_img1]])
        if output_path != temp_filepath and os.path.exists(temp_filepath):
            os.remove(temp_filepath)
        return output_path

    def iterative_merge(self, input_file_list, output_path):
        """merge images acquired in the same day based on cloud probability in udm2"""

        if len(input_file_list) == 2:
            self.complex_gdal_merge(input_file_list[0], input_file_list[1], output_path)
        else:
            temp_filepath = self.complex_gdal_merge(input_file_list[0], input_file_list[1])
            for idx, fp in enumerate(input_file_list[2:]):
                if idx != len(input_file_list[2:]) - 1:
                    self.complex_gdal_merge(temp_filepath, fp)
                else:
                    self.complex_gdal_merge(temp_filepath, fp, output_path)

    def prep_pipline(self, input_dir, output_dir, start_date, end_date, crs=None,  jp2=True, clean=True,
                     complex_merge=None):
        """
        Prepare input datasets for the deep learning models.
        1. (prerequisite) download raw tiles, i.e., no clipping <- planetmosaic python project.
        2. (prerequisite) merge tiles acquired the same day and on the same orbit.
        3. (prerequisite) clip to the extent of AOI. <- clip function
        4. stack sr and udm2 into one image (separate bands).
        5. merge stacked images in the same day (regardless of orbits).
        6. separate sr and udm2.
        7. stack image time series into one file for sr and udm2 independently (in sequence of acquisition date), and save time stamps into a pickle file.
        8. (optional) convert CRS and data format.
        :param input_dir:
        :param output_dir:
        :param start_date:
        :param end_date:
        :param jp2:
        :return:
        """
        # To be updated: use multiprocessing and on-the-fly processes

        # create folders
        sr_udm2_dir = '/mnt/raid5/Planet/pre_processed/Sierra_Nevada_AOI1/stack_sr_udm2'
        merge_orbit_dir = '/mnt/raid5/Planet/pre_processed/Sierra_Nevada_AOI1/merge_combine_orbits'
        merge_orbit_sep_dir = '/mnt/raid5/Planet/pre_processed/Sierra_Nevada_AOI1/merge_orbits_sr_udm2'
        folder_list = [output_dir, sr_udm2_dir, merge_orbit_dir, merge_orbit_sep_dir]
        for folder_path in folder_list:
            if not os.path.exists(folder_path):
                os.mkdir(folder_path)

        # main
        date_orbit_list = [(Path(fp).stem.split('_')[0], Path(fp).stem.split('_')[1])
                           for fp in sorted(glob(os.path.join(input_dir, '*AnalyticMS_SR*.tif')))
                           if (Path(fp).stem.split('_')[0] >= start_date) and (Path(fp).stem.split('_')[0] <= end_date)]
        date0 = None
        orbit_list = []
        date_list = []
        for idx, (date, orbit) in enumerate(date_orbit_list[:]):
            # stack sr and udm2 with the same date and orbit into one image.
            self.gdal_merge(input_path=' '.join(
                [glob(os.path.join(input_dir, f'{date}_{orbit}*AnalyticMS_SR*.tif'))[0],
                 glob(os.path.join(input_dir, f'{date}_{orbit}*udm2*.tif'))[0]]),
                output_path=os.path.join(sr_udm2_dir, f"{date}_{orbit}.tif"), data_type='UInt16',
                separate=True, compression='LZW')

            if idx == len(date_orbit_list):
                date0 = date
                date += 1

            if (date0 is not None) and (not date0 == date):
                # merge images in the same day regardless of orbits.
                input_file_list = [os.path.join(sr_udm2_dir, f'{date0}_{orbit}.tif') for orbit in orbit_list]
                if complex_merge is True and len(input_file_list) >= 2:
                    self.iterative_merge(input_file_list=input_file_list,
                                         output_path=os.path.join(merge_orbit_dir, f'{date0}.tif'))
                else:
                    self.gdal_merge(
                        input_path=' '.join(input_file_list),
                        output_path=os.path.join(merge_orbit_dir, f'{date0}.tif'), data_type='UInt16', separate=False,
                        compression='LZW')
                orbit_list = []
                date_list.append(date0)
                # split sr and udm2
                translateoptions = gdal.TranslateOptions(gdal.ParseCommandLine("-b 1 -b 2 -b 3 -b 4 -of Gtiff "
                                                                               "-co COMPRESS=LZW -ot UInt16"))
                gdal.Translate(os.path.join(merge_orbit_sep_dir, f'{date0}_AnalyticMS_SR.tif'),
                               os.path.join(merge_orbit_dir, f'{date0}.tif'), options=translateoptions)
                translateoptions = gdal.TranslateOptions(gdal.ParseCommandLine("-b 5 -b 6 -b 7 -b 8 -b 9 -b 10 -b 11 "
                                                                               "-b 12 -of Gtiff -co COMPRESS=LZW "
                                                                               "-ot Byte"))
                gdal.Translate(os.path.join(merge_orbit_sep_dir, f'{date0}_udm2.tif'),
                               os.path.join(merge_orbit_dir, f'{date0}.tif'), options=translateoptions)
                # stack all images into one file for both sr and udm2
                if len(date_list) == 1:
                    list([self.gdal_merge(
                        input_path=os.path.join(merge_orbit_sep_dir, f'{date_list[0]}_{self.asset_attrs(asset_type)["suffix"]}.tif'),
                        output_path=os.path.join(output_dir, f'stack0_{self.asset_attrs(asset_type)["suffix"]}.tif'),
                        data_type=self.asset_attrs(asset_type)['data type'],
                        separate=True, compression='LZW') for asset_type in ['analytic_sr', 'udm2']])
                elif len(date_list) > 1:
                    list([self.gdal_merge(
                        input_path=' '.join([os.path.join(output_dir, f'stack0_{self.asset_attrs(asset_type)["suffix"]}.tif'),
                                             os.path.join(merge_orbit_sep_dir, f'{date_list[-1]}_{self.asset_attrs(asset_type)["suffix"]}.tif')]),
                        output_path=os.path.join(output_dir, f'stack_{self.asset_attrs(asset_type)["suffix"]}.tif'),
                        data_type=self.asset_attrs(asset_type)['data type'],
                        separate=True, compression='LZW') for asset_type in ['analytic_sr', 'udm2']])
                    # delete stack0.tif
                    list([os.remove(os.path.join(output_dir, f'stack0_{self.asset_attrs(asset_type)["suffix"]}.tif'))
                          for asset_type in ['analytic_sr', 'udm2']])
                    # rename stack.tif to stack0.tif
                    list([os.rename(os.path.join(output_dir, f'stack_{self.asset_attrs(asset_type)["suffix"]}.tif'),
                                    os.path.join(output_dir, f'stack0_{self.asset_attrs(asset_type)["suffix"]}.tif'))
                          for asset_type in ['analytic_sr', 'udm2']])

            date0 = date
            orbit_list.append(orbit)

        # export dates as a pickle file
        with open(os.path.join(output_dir, f'PS_stack_dates_{start_date}_{end_date}.txt'), 'w') as txt:
            for date in date_list:
                txt.write(date + ",")

        # delete temporary datasets and folders
        if clean is True:
            list([shutil.rmtree(i) for i in [sr_udm2_dir, merge_orbit_dir, merge_orbit_sep_dir]])
        # rename
        list([os.rename(
            os.path.join(output_dir, f'stack0_{self.asset_attrs(asset_type)["suffix"]}.tif'),
            os.path.join(output_dir, f'PS_{self.asset_attrs(asset_type)["suffix"]}_stack_{start_date}_{end_date}.tif')
        ) for asset_type in ['analytic_sr', 'udm2']])

        # change coordinate system
        if crs is not None:
            # to be completed
            pass
            # ref_img = None
            # ds = gdal.Open(ref_img)
            # crs_original = ds.GetProjection()
            # options = None
            # gdal.Warp()

        # convert to jp2 format
        if jp2 is True:
            options = dict(
                format="JP2OpenJPEG",
                creationOptions=["QUALITY=80"],
                callback=self.gdal_progress_callback,
                callback_data=tqdm(total=100, position=0, leave=True, desc="")
            )
            list([gdal.Translate(
                os.path.join(output_dir,
                             f'PS_{self.asset_attrs(asset_type)["suffix"]}_stack_{start_date}_{end_date}.jp2'),
                os.path.join(output_dir,
                             f'PS_{self.asset_attrs(asset_type)["suffix"]}_stack_{start_date}_{end_date}.tif'),
                **options) for asset_type in ['analytic_sr', 'udm2']])
            list([os.remove(os.path.join(
                output_dir, f'PS_{self.asset_attrs(asset_type)["suffix"]}_stack_{start_date}_{end_date}.tif'))
                  for asset_type in ['analytic_sr', 'udm2']])

    def plot_time_series(self):
        '''

        :return:
        '''

    def normalize(self, array, percentile=None):
        '''
        Normalize bands into 0.0 - 1.0 scale
        :param array: list or numpy array
        :param percentile: list, a list of percentile, [20, 50] - 20th and 50th percentile
        :return: numpy array, normalized array
        '''
        array_np = np.array(array)
        array_flat = np.array(array_np).flatten()
        if percentile is None:
            array_min, array_max = array_flat.min(), array_flat.max()
        else:
            array_min = np.percentile(array_flat, min(percentile))
            array_max = np.percentile(array_flat, max(percentile))
        return (array_np - array_min) / (array_max - array_min)

    def clip_clear_perc(self, shapefile_path, clear_perc_min, save_rgb=True, save_clip=False, file_list=None):
        '''
        Clip images to the extent of AOI.shp if only the percentage of clear pixels within the extent of AOI.shp
        higher than the threshold
        :param shapefile_path: string, file path of AOI.shp
        :param clear_perc_min: float, maximum percent of clear pixels
        :param save_rgb:
        :param save_clip:
        :param file_list: list, a list of udm2 images
        :return:
        '''

        print('Start to clip images :)')
        # records_file = open(self.records_path, "a+")
        time_str = datetime.now().strftime("%Y%m%d-%H%M%S")
        # records_file.write('Execute clip_clear_perc():\nArguments: shapefile_path={} clear_perc_min={} save_rgb={}, '
        #                    'save_clip={} file_list={}\nStart time: {}\n\n'.format(shapefile_path, clear_perc_min,
        #                                                                           save_rgb, save_clip, file_list,
        #                                                                           time_str))
        input_dir = Path(self.work_dir) / self.output_dirs['clip']
        output_dir = Path(self.work_dir) / self.output_dirs['clip clear perc']

        pixel_res = self.pixel_res(self.satellite)  # pixel resolution
        shp_name = shapefile_path.split('\\')[-1].split('.shp')[0]
        # List merged and clipped udm2 file path
        if file_list is None:
            file_list = glob('{}\\{}\\*udm2.tif'.format(self.work_dir, self.output_dirs['clip']))
        else:
            file_list = [file for file in file_list if 'udm2' in file]

        # Check existing file
        # if save_clip is True:
        #     file_list_exist = glob('{}\\*.tif'.format(output_dir))
        #     if file_list_exist:
        #         item_id_list_exist = [file.split('\\')[0].split('_{}'.format(self.asset_attrs('analytic_sr')['suffix']))[0]
        #                               for file in file_list_exist]
        #         if self.remove_latest is True:
        #             latest_file = max(file_list_exist, key=os.path.getctime)
        #             os.remove(latest_file)
        #             file_list_exist.remove(latest_file)
        #         file_list = [file for file in file_list if file.split('\\')[0].split(
        #             '_{}'.format(self.asset_attrs('analytic_sr')['suffix']))[0] not in item_id_list_exist]

        asset_id_list = []
        for file in file_list:
            asset_name = file.split('\\')[-1].split('.')[0]
            asset_id = asset_name.split('_udm2')[0]
            # Clip udm2 to the extent of bomas AOI.shp and save in memory
            vsimem_path = '/vsimem/' + asset_name + '.tif'
            # print(vsimem_path)
            self.gdal_clip(file, pixel_res, shapefile_path, vsimem_path, data_type='Byte')
            raster = gdal.Open(vsimem_path)
            # Convert the first band (clear) to numpy array
            clear_band_array = np.array(raster.GetRasterBand(1).ReadAsArray())
            # Calculate the number of pixels
            n_all_pixels = clear_band_array.shape[0] * clear_band_array.shape[1]
            # Calculate the number of clear pixels
            n_clear_pixels = np.count_nonzero(clear_band_array != 0)
            # Calculate the percentage of non-clear pixels
            clear_perc = n_clear_pixels / n_all_pixels
            raster = None
            # If the percentage of clear pixels is larger than the threshold, clip and save the associated
            # analytic_sr imagery to specific folder
            if clear_perc >= clear_perc_min:
                asset_id_list.append(asset_id)
                asset_name = asset_id + '_' + self.asset_attrs(asset_type='analytic_sr')['suffix']
                input_path = '{}\\{}.tif'.format(input_dir, asset_name)
                if save_clip is True:
                    output_path = '{}\\{}_{}.tif'.format(output_dir, asset_name, shp_name)
                    try:
                        self.gdal_clip(input_path, pixel_res, shapefile_path, output_path, data_type='UInt16')
                    except:
                        pass
                if save_rgb is True:
                    vsimem_path = '/vsimem/{}.tif'.format(asset_name)
                    try:
                        self.gdal_clip(input_path, pixel_res, shapefile_path, vsimem_path, data_type='UInt16')
                        with rasterio.open(vsimem_path) as raster:
                            # Convert to numpy arrays
                            nir = raster.read(self.rgb_composition['red'])
                            red = raster.read(self.rgb_composition['green'])
                            green = raster.read(self.rgb_composition['blue'])
                            # Normalize band DN
                            nir_norm = self.normalize(nir, self.percentile)
                            red_norm = self.normalize(red, self.percentile)
                            green_norm = self.normalize(green, self.percentile)
                            # Stack bands
                            nrg = np.dstack((nir_norm, red_norm, green_norm))
                            # View the color composite
                            fig = plt.figure()
                            plt.imshow(nrg)
                            plot_path = '{}\\{}_{}_thumbnail.png'.format(output_dir, asset_name, shp_name)
                            fig.savefig(plot_path, dpi=self.dpi)
                    except:
                        pass

        # records_file.write('List of asset id of processed images: {}\n\n'.format(asset_id_list))
        time_str = datetime.now().strftime("%Y%m%d-%H%M%S")
        print('Finish clipping images:)')
        print('The outputs have been saved in this directory: ' + output_dir)
        # records_file.write('The outputs have been saved in this directory: {}\n\n'.format(output_dir))
        # records_file.write('End time: {}\n\n'.format(time_str))
        # records_file.close()


# Testing
if __name__ == '__main__':
    # ut = Utilities()
    # ut.start_up()
    pass
    # # Test several data
    # ut.id_list_download = ['20190107_074019_1049', '20190107_074018_1049']
    # date_list = [i.split('_')[0] for i in ut.id_list_download]
    # for date in date_list:
    #     file_list = []
    #     a = glob("{}\\{}*udm2.tif".format(ut.work_dir + ut.output_dirs['merge'], date))
    #     for i in a:
    #         file_list.append(i)
    # ut.download_assets()
    # ut.merge()
    # ut.clip()
    # ut.band_algebra(output_type='clear prob')

    # Test merge, clip and band_algebra
    # ut.merge(file_list=glob("{}\\{}\\*.tif".format(ut.work_dir, ut.output_dirs['raw'])))
    # ut.clip(file_list=glob("{}\\{}\\*.tif".format(ut.work_dir, ut.output_dirs['merge'])))
    # ut.band_algebra(output_type='clear prob', file_list=glob("{}\\{}\\*.tif".format(ut.work_dir, ut.output_dirs['clip'])))
    # ut.clip_clear_perc(shapefile_path=r'C:\Users\ChengY\PycharmProjects\PyPlanetScope_WD\shp\bomas\layers\POLYGON.shp',
    #                    clear_perc_min=0.1, save_rgb=True, save_clip=True,
    #                    file_list=glob("{}\\{}\\*.tif".format(ut.work_dir, ut.output_dirs['clip'])))
