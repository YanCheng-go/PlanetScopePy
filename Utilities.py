'''
======================================
Utilities for processing PlanetScope imagery
Author: Yan Cheng
Contributors: Dr. Anton Vrieling
======================================
'''

'''
Preparations
=======================================
0. Download Anaconda
1. conda install rasterio
2. conda install -c conda-forge geopandas
3. remove gdal from site-packages
4. remove pyproj from site-packages
5. pip install pyproj
6. pip install planet
7. install gdal using the .whl file from https://www.lfd.uci.edu/~gohlke/pythonlibs/
    pip install [PATH OF GDAL WHL FILE]

To do list   
====================================== 
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
from rasterio.plot import show
import matplotlib.pyplot as plt
import numpy as np
import requests
from requests.auth import HTTPBasicAuth
import sys
import warnings
warnings.simplefilter('ignore')


class Utilities:
    '''
    Commonly used tools for the processing and raster analytics of PlanetScope imagery
    :param:
    '''

    # Set environment
    default_gdal_scripts_path = r'C:\Users\ChengY\AppData\Local\Continuum\anaconda3\Lib\site-packages\osgeo\scripts'
    default_gdal_data_path = r'C:\Users\ChengY\AppData\Local\Continuum\anaconda3\Lib\site-packages\osgeo\data\gdal'
    sys.path.append(default_gdal_scripts_path)
    os.environ["GDAL_DATA"] = default_gdal_data_path
    # File path of gdal_calc.py and gdal_merge.py
    default_gdal_calc_path = r'C:\Users\ChengY\AppData\Local\Continuum\anaconda3\Lib\site-packages\osgeo\scripts\gdal_calc.py'
    default_gdal_merge_path = r'C:\Users\ChengY\AppData\Local\Continuum\anaconda3\Lib\site-packages\osgeo\scripts\gdal_merge.py'
    default_gdal_translate_path = r'C:\Users\ChengY\AppData\Local\Continuum\anaconda3\Lib\site-packages\osgeo\gdal_translate.exe'
    # Set directories
    default_work_dir = r'C:\Users\ChengY\Desktop'
    default_output_dirs = {'raw': 'raw', 'clipped raw': 'clipped_raw', 'merge': 'merge', 'clip': 'clip',
                           'clear prob': 'clear_prob', 'NDVI': 'NDVI', 'clip clear perc': 'bomas'}
    # API Key
    default_api_key = "9cada8bc134546fe9c1b8bce5b71860f"
    # Specs
    default_satellite = 'PS'
    default_proj_code = 32737
    # Filter settings
    default_filter_items = ['date', 'cloud_cover', 'aoi']
    default_item_types = ["PSScene4Band"]
    default_asset_types = ['analytic_sr', 'udm2']
    default_start_date = '2019-01-20'
    default_end_date = '2019-01-31'
    default_cloud_cover = 0.8
    default_aoi_shp = r'D:\Kapiti\supplementary_data\Kapiti_Jun18_v2_prj.shp'
    # Color composition for visualization
    default_rgb_composition = {'red': 4, 'green': 3, 'blue': 2} # False color composition for PlanetScope images
    default_dpi = 90
    default_percentile = [2, 98]

    def __init__(self, gdal_scripts_path=default_gdal_scripts_path, gdal_data_path=default_gdal_data_path,
                 gdal_calc_path=default_gdal_calc_path, gdal_merge_path=default_gdal_merge_path,
                 gdal_translate_path=default_gdal_translate_path, work_dir=default_work_dir,
                 output_dirs=default_output_dirs, satellite=default_satellite, proj_code=default_proj_code,
                 api_key=default_api_key, filter_items=default_filter_items, item_types=default_item_types,
                 asset_types=default_asset_types, start_date=default_start_date, end_date=default_end_date,
                 cloud_cover=default_cloud_cover, aoi_shp=default_aoi_shp, rgb_composition=default_rgb_composition,
                 dpi=default_dpi, percentile=default_percentile):
        '''

        :param gdal_scripts_path: string
        :param gdal_data_path: string
        :param gdal_calc_path: string
        :param gdal_merge_path: string
        :param gdal_translate_path: string
        :param work_dir: string
        :param output_dirs: dictionary, the name of folder for storing different outputs
        :param satellite: string, abbreviation of satellite name, PS - PLanetScope, S2 - Sentinel-2
        :param proj_code: int, the EPSG code of projection systtem
        :param api_key: string
        :param filter_items: list, a list of filter item names
        :param item_types: list, a list of item type, more info: https://developers.planet.com/docs/data/items-assets/
        :param asset_types: list, a list of asset type, more info: https://developers.planet.com/docs/data/psscene4band/
        :param start_date: string, start date with a format of 'YYYY-MM-DD'
        :param end_date: string, end date with a format of 'YYYY-MM-DD'
        :param cloud_cover: float, maximum cloud cover
        :param aoi_shp: string, file path of AOI.shp, better to be projected one
        :param rgb_composition: list, band sequence for rgb color composition, [4, 1, 3] is false color composition for
                                PlanetScope images
        :param dpi: int, dpi of saved plot
        :param percentile: list, minimum and maximum percentile
        '''

        self.gdal_scripts_path = gdal_scripts_path
        self.gdal_data_path = gdal_data_path
        self.gdal_calc_path = gdal_calc_path
        self.gdal_merge_path = gdal_merge_path
        self.gdal_translate_path = gdal_translate_path
        self.work_dir = work_dir
        self.output_dirs = output_dirs
        self.satellite = satellite
        self.proj_code = proj_code
        self.api_key = api_key
        self.client = api.ClientV1(api_key=api_key)
        self.filter_items = filter_items
        self.item_types = item_types
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
        self.records_path = None # File path of execution track document
        self.id_list_download = None # a list of item id which will be downloaded

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
            shp2 = shp.to_crs({'init': 'epsg:4326'})
        coors = np.array(dict(json.loads(shp2['geometry'].to_json()))
                         ['features'][0]['geometry']['coordinates'])[:, :, 0:2].tolist()
        aoi_geom = {"type": "Polygon", "coordinates": coors}
        # print(self.aoi_geom)
        return aoi_geom

    @staticmethod
    def asset_suffix(asset_type):
        '''
        :param asset_type: string, one item in the list of asset type, more info:
                            https://developers.planet.com/docs/data/psscene4band/
        :return: string, associated suffix of each asset type
        '''

        switch = {
            'analytic_sr': 'AnalyticMS_SR',
            'udm2': 'udm2'
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
            self.create_dir(self.work_dir + '\\{}'.format(self.output_dirs[i]))

    def create_track_file(self):
        '''
        Create a txt file to track all the execution of code and outputs, including:
        1. Date
        2. Item id that does not have required types of asset
        3. Item id of downloaded items
        4. Metadata of each downloaded item
        :return:
        '''

        records_path = self.work_dir + '\\Execution_Track.txt'
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
        self.create_track_file()
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

        output_dir = self.work_dir + '\\' + self.output_dirs['raw']
        records_file = open(self.records_path, "a+")

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
            with open(output_dir + '\\' + '{}_3B_{}.tif'.format(item_id, self.asset_suffix(asset_type)),
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
            records_file.write("NO FILE: {} {} {}\n\n".format(item_id, asset_type, item_type))
            asset_exist = False
        records_file.close()
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

        output_dir = self.work_dir + '\\' + self.output_dirs['raw']
        records_file = open(self.records_path, "a+")

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
            records_file.write("{} {} {}\n\n".format(item_id, asset_type, item_type))
            asset_exist = False
        records_file.close()
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
        output_dir = self.work_dir + '\\' + self.output_dirs['clipped_raw']
        self.create_dir(output_dir)
        records_file = open(self.records_path, "a+")
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
            with open(output_dir + '\\' + '{}_3B_{}.tif'.format(item_id, self.asset_suffix(asset_type)),
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
            records_file.write("NO FILE: {} {} {}\n\n".format(item_id, asset_type, item_type))
            asset_exist = False
        records_file.close()
        return asset_exist

    @staticmethod
    def retrieve_exist_files(dir):
        '''
        Retrieve item id of existing assests in a folder
        :param dir: string, the path of a folder contains downloaded assests
        :return: id_list_exist, list, id list of existing items in the folder
        '''

        file_list_udm2 = glob('{}\\*udm2.tif'.format(dir))
        file_list_sr = glob('{}\\*SR.tif'.format(dir))
        id_list_exist_udm2 = [i.split('\\')[-1].split('_3B_')[0] for i in file_list_udm2]
        id_list_exist_sr = [i.split('\\')[-1].split('_3B_')[0] for i in file_list_sr]
        if len(id_list_exist_udm2) >= len(id_list_exist_sr):
            id_list_exist = id_list_exist_sr
        else:
            id_list_exist = id_list_exist_udm2

        return id_list_exist

    def download_assets(self, clipped=None, output_dir=None):
        '''
        Download all required assets
        :param clipped: boolean, True means downloading clipped assests, otherwise downloading raw imagery
        :param output_dir: string, the directory for saving downloaded assets. In case you have downloaded some assets
        before using this script, this argument can avoid downloading the same assets again.
        :return:
        '''

        print('Start to download assets :)')
        records_file = open(self.records_path, "a+")
        time_str = datetime.now().strftime("%Y%m%d-%H%M%S")
        records_file.write('Execute download_assets():\nArguments: clipped={} output_dir={}\nStart time: {}\n\n'
                           .format(clipped, output_dir, time_str))
        if output_dir is None:
            output_dir = self.work_dir + '\\' + self.output_dirs['raw']

        # Download clipped assets or not
        if clipped is None:
            clipped = False

        # Create filter
        and_filter = self.create_filter()
        records_file.write('Filter settings: {}\n\n'.format(and_filter))
        # Search items
        req = filters.build_search_request(and_filter, self.item_types)
        res = self.client.quick_search(req)
        # List id of all items in the search result
        id_list_search = [i['id'] for i in res.items_iter(250)]
        # Retrieve id of existing assets in the folder used to save all downloaded assets
        if output_dir is None:
            output_dir = self.work_dir + '\\' + self.output_dirs['raw']
            id_list_exist = self.retrieve_exist_files(output_dir)
        else:
            id_list_exist = self.retrieve_exist_files(output_dir)
        # List id of all items to be downloaded
        self.id_list_download = [i for i in id_list_search if i not in id_list_exist]
        # print(self.id_list_download)

        # Download (clipped) assets based on their item id
        for item_id in tqdm(self.id_list_download, total=len(self.id_list_download), unit="item",
                            desc='Downloading assets'):
            if not clipped:
                for item_type in self.item_types:
                    for asset_type in self.asset_types:
                        asset_exist = self.download_one(item_id, asset_type, item_type)
                        if asset_exist is True:
                            metadata = [i for i in res.items_iter(250) if i['id'] == item_id]
                            records_file = open(self.records_path, "a+")
                            records_file.write('File Exists: {}_3B_{} {}\n\n'.format(item_id,
                                                                                 self.asset_suffix(asset_type),
                                                                                 item_type))
                            records_file.write('Metadata for {}_3B_{} {}\n{}\n\n'.format(item_id,
                                                                                       self.asset_suffix(asset_type),
                                                                                       item_type, metadata))
                            records_file.close()
            else:
                self.download_clipped(item_id)

        time_str = datetime.now().strftime("%Y%m%d-%H%M%S")
        print('Finish downloading assets :)')
        print('The raw images have been saved in this directory: ' + output_dir)
        print('The information of missing assets has be saved in this file: ' + self.records_path)
        records_file.write('The outputs have been saved in this directory: {}\n\n'.format(output_dir))
        records_file.write('End time: {}\n\n'.format(time_str))
        records_file.close()

    def gdal_translate(self, input_path, output_path):
        '''
        GDAL translate function
        Remove band 8 and set backgound pixel as no data
        :param input_path:
        :param output_path:
        :return:
        '''

        gdal_translate_str = '{0} -b 1 -b 2 -b 3 -b 4 -b 5 -b 6 -b 7 -a_nodata 0 -ot UInt16 -of GTiff {1} {2}'
        gdal_translate_process = gdal_translate_str.format(self.gdal_translate_path, input_path, output_path)
        os.system(gdal_translate_process)

    def gdal_udm2_setnull(self, input_path, output_path):
        '''
        Set the value of background pixels as no data
        :param input_path:
        :param output_path:
        :return:
        '''

        temp_output_path = self.work_dir + '\\TempFile.tif'
        gdal_calc_str = 'python {0} --calc "A+B+C+D+E+F+G" --co "COMPRESS=LZW" --format GTiff ' \
                        '--type Byte -A {1} --A_band 1 -B {2} --B_band 2 -C {3} --C_band 3 -D {4} --D_band 4 ' \
                        '-E {5} --E_band 5 -F {6} --F_band 6 -G {7} --G_band 7 --outfile {8} --overwrite'
        gdal_calc_process = gdal_calc_str.format(self.gdal_calc_path, input_path, input_path, input_path, input_path,
                                                 input_path, input_path, input_path, temp_output_path)
        os.system(gdal_calc_process)

        gdal_calc_str = 'python {0} --calc "A*(B>0)" --co "COMPRESS=LZW" --format GTiff ' \
                        '--type Byte --NoDataValue 0 -A {1} -B {2} --outfile {3} --allBands A --overwrite'
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
        records_file = open(self.records_path, "a+")
        time_str = datetime.now().strftime("%Y%m%d-%H%M%S")
        records_file.write('Execute udm2_setnull():\nArguments: file_list={}\nStart time: {}\n\n'
                           .format(file_list, time_str))

        input_dir = self.work_dir + '\\' + self.output_dirs['raw']
        output_dir = input_dir

        if file_list is None:
            file_list = []
            for i in self.id_list_download:
                a = glob("{}\\{}*udm2.tif".format(input_dir, i))
                for j in a:
                    file_list.append(j)
        else:
            file_list = [file for file in file_list if 'udm2' in file]

        print(file_list)

        for input_path in tqdm(file_list, total=len(file_list), unit="item", desc='Processing udm2 data'):
            output_path = output_dir + '\\' + input_path.split('\\')[-1].split('.')[0] + '_setnull.tif'
            self.gdal_udm2_setnull(input_path, output_path)

        time_str = datetime.now().strftime("%Y%m%d-%H%M%S")
        print('Finish processing udm2 data :)')
        print('The outputs have been saved in this directory: ' + output_dir)
        records_file.write('The outputs have been saved in this directory: {}\n\n'.format(output_dir))
        records_file.write('End time: {}\n\n'.format(time_str))
        records_file.close()

    def gdal_merge(self, input_path, output_path, data_type):
        '''
        GDAL merge function
        https://gdal.org/programs/gdal_merge.html
        :param gdal_merge_path:
        :param output_file_path:
        :param input_file_path:
        :return:
        '''

        gdal_merge_str = 'python {0} -a_nodata 0 -o {1} {2} -ot {3}'
        gdal_merge_process = gdal_merge_str.format(self.gdal_merge_path, output_path, input_path, data_type)
        os.system(gdal_merge_process)

    def merge(self, file_list=None):
        '''
        Merge images acquired in the same day with the same satellite id
        :param file_list:
        :return:
        '''

        # Preprocessing udm2 data
        # Remove exist setnull data from file_list
        exist_setnull = list(set([file.split('\\')[-1].split('_3B_')[0] for file in file_list if 'setnull' in file]))
        item_id_list = list(set([file.split('\\')[-1].split('_3B_')[0] for file in file_list]))
        new_setnull = [i for i in item_id_list if i not in exist_setnull]
        if len(new_setnull) != 0:
            self.udm2_setnull(file_list=[file for file in file_list for i in new_setnull if i in file])
        exist_setnull, item_id_list, new_setnull = None, None, None

        print('Start to merge images collected in the same day on the same orbit :)')
        records_file = open(self.records_path, "a+")
        time_str = datetime.now().strftime("%Y%m%d-%H%M%S")
        records_file.write('Execute merge():\nArguments: file_list={}\nStart time: {}\n\n'.format(file_list, time_str))
        input_dir = self.work_dir + '\\' + self.output_dirs['raw']
        output_dir = self.work_dir + '\\' + self.output_dirs['merge']

        if file_list is None:
            file_list = []
            for i in self.id_list_download:
                a = glob("{}\\{}*.tif".format(input_dir, i))
                for j in a:
                    file_list.append(j)
            # print(file_list)
        date_list = list(set([x.split('\\')[-1].split('_')[0] for x in file_list]))

        for date in tqdm(date_list, total=len(date_list), unit="item", desc='Merging images'):
            file_list_sr = glob("{}\\{}*SR.tif".format(input_dir, date))
            file_list_udm2 = glob("{}\\{}*udm2_setnull.tif".format(input_dir, date))
            input_path_sr = ' '.join(str(i) for i in file_list_sr)
            input_path_udm2 = ' '.join(str(i) for i in file_list_udm2)
            satellite_id_list = list(set([x.split('\\')[-1].split('_3B_')[0].split('_')[-1] for x in file_list_udm2]))
            for satellite_id in satellite_id_list:
                output_path_sr = output_dir + '\\' + date + '_' + satellite_id + '_AnalyticMS_SR.tif'
                output_path_udm2 = output_dir + '\\' + date + '_' + satellite_id + '_udm2.tif'
                self.gdal_merge(input_path_sr, output_path_sr, 'UInt16')
                self.gdal_merge(input_path_udm2, output_path_udm2, 'Byte')

        time_str = datetime.now().strftime("%Y%m%d-%H%M%S")
        print('Finish merging images :)')
        print('The merged images have been saved in this directory: ' + output_dir)
        records_file.write('The outputs have been saved in this directory: {}\n\n'.format(output_dir))
        records_file.write('End time: {}\n\n'.format(time_str))
        records_file.close()

    @staticmethod
    def gdal_clip(input_path, pixel_res, shapefile_path, output_path, data_type):
        '''
        GDAL clip function
        :param input_file_path:
        :param pixel_res:
        :param shapefile_path:
        :param output_file_path:
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
        OutTile = gdal.Warp(output_path, raster, format='GTiff',
                            outputType=gdal_data_type,
                            outputBounds=[minX, minY, maxX, maxY],
                            xRes=pixel_res, yRes=pixel_res,
                            targetAlignedPixels=True,
                            # dstSRS='epsg:{}'.format(str(self.proj_code)),
                            resampleAlg=gdal.GRA_NearestNeighbour,
                            cutlineDSName=shapefile_path,
                            cutlineLayer=shapefile_path.split('\\')[-1].split('.')[0],
                            cropToCutline=True,
                            dstNodata=-9999,
                            options=['COMPRESS=LZW'])
        # Close dataset
        OutTile = None
        raster = None
        vector_dataset.Destroy()

    def clip(self, file_list=None):
        '''
        Clip imagery to the extent of AOI
        :return:
        '''

        print('Start GDAL clip :)')
        records_file = open(self.records_path, "a+")
        time_str = datetime.now().strftime("%Y%m%d-%H%M%S")
        records_file.write('Execute clip():\nArguments: file_list={}\nStart time: {}\n\n'.format(file_list, time_str))
        input_dir = self.work_dir + '\\' + self.output_dirs['merge']
        output_dir = self.work_dir + '\\' + self.output_dirs['clip']

        if file_list is None:
            file_list = []
            for i in self.id_list_download:
                a = glob("{}\\{}*.tif".format(input_dir, i))
                for j in a:
                    file_list.append(j)
            # print(file_list)

        for input_path in tqdm(file_list, total=len(file_list), unit="item", desc='Clipping images'):
            output_name = input_path.split('\\')[-1]
            output_path = output_dir + '\\' + output_name
            if 'udm2' in input_path:
                data_type = 'Byte'
            if 'SR' in input_path:
                data_type = 'UInt16'
            self.gdal_clip(input_path, self.pixel_res(self.satellite), self.aoi_shp, output_path, data_type)

        time_str = datetime.now().strftime("%Y%m%d-%H%M%S")
        print('Finish clipping images :)')
        print('The clipped images have been saved in this directory: ' + output_dir)
        records_file.write('The outputs have been saved in this directory: {}\n\n'.format(output_dir))
        records_file.write('End time: {}\n\n'.format(time_str))
        records_file.close()

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
        records_file = open(self.records_path, "a+")
        time_str = datetime.now().strftime("%Y%m%d-%H%M%S")
        records_file.write('Execute band_algebra():\nArguments: output_type={} file_list={}\nStart time: {}\n\n'
                           .format(output_type, file_list, time_str))
        input_dir = self.work_dir + '\\' + self.output_dirs['clip']

        if output_type == 'clear prob':
            clear_prob_dir = self.work_dir + '\\' + self.output_dirs['clear prob']
            if file_list is None:
                file_list = []
                for i in self.id_list_download:
                    a = glob("{}\\{}*udm2.tif".format(input_dir, i))
                    for j in a:
                        file_list.append(j)
            else:
                file_list = [file for file in file_list if 'udm2' in file]
            for udm2_path in file_list:
                clear_prob_path = clear_prob_dir + '\\' + udm2_path.split('\\')[-1].split('_udm2')[0] + '_clearprob.tif'
                self.gdal_calc_clear_prob(input_path=udm2_path, output_path=clear_prob_path)
            print('Finish GDAL Calculation :)')
            print('The outputs have been saved in this directory: ' + clear_prob_dir)

        if output_type == 'NDVI':
            ndvi_dir = self.work_dir + '\\' + self.output_dirs['NDVI']
            if file_list is None:
                file_list = []
                for i in self.id_list_download:
                    a = glob("{}\\{}*SR.tif".format(input_dir, i))
                    for j in a:
                        file_list.append(j)
            else:
                file_list = [file for file in file_list if 'SR' in file]
            for sr_path in file_list:
                ndvi_path = ndvi_dir + '\\' + sr_path.split('\\')[-1].split('_Analytic_SR')[0] + '_ndvi.tif'
                self.gdal_calc_ndvi(input_path=sr_path, output_path=ndvi_path)
            time_str = datetime.now().strftime("%Y%m%d-%H%M%S")
            print('Finish GDAL Calculation :)')
            print('The outputs have been saved in this directory: ' + ndvi_dir)
            records_file.write('The outputs have been saved in this directory: {}\n\n'.format(ndvi_dir))
            records_file.write('End time: {}\n\n'.format(time_str))
            records_file.close()

    def stack_bands(self):
        '''

        :return:
        '''

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
        :param cloud_perc_max: float, maximum percent of clear pixels
        :param file_list: list, a list of udm2 images
        :return:
        '''

        print('Start to clip images :)')
        records_file = open(self.records_path, "a+")
        time_str = datetime.now().strftime("%Y%m%d-%H%M%S")
        records_file.write('Execute clip_clear_perc():\nArguments: shapefile_path={} clear_perc_min={} save_rgb={}, '
                           'save_clip={} file_list={}\nStart time: {}\n\n'.format(shapefile_path, clear_perc_min,
                                                                                  save_rgb, save_clip, file_list,
                                                                                  time_str))
        input_dir = self.work_dir + '\\' + self.output_dirs['clip']
        output_dir = self.work_dir + '\\' + self.output_dirs['clip clear perc']

        pixel_res = self.pixel_res(self.satellite)  # pixel resolution
        shp_name = shapefile_path.split('\\')[-1].split('.shp')[0]
        # List merged and clipped udm2 file path
        if file_list is None:
            file_list = glob('{}\\{}\\*udm2.tif'.format(self.work_dir, self.output_dirs['clip']))
        else:
            file_list = [file for file in file_list if 'udm2' in file]

        asset_id_list = []
        for file in file_list:
            asset_name = file.split('\\')[-1].split('.')[0]
            asset_id = asset_name.split('_udm2')[0]
            # Clip udm2 to the extent of bomas AOI.shp and save in memory
            vsimem_path = '/vsimem/' + asset_name + '.tif'
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
                asset_name = asset_id + '_' + self.asset_suffix(asset_type='analytic_sr')
                input_path = '{}\\{}.tif'.format(input_dir, asset_name)
                if save_clip is True:
                    output_path = '{}\\{}_{}.tif'.format(output_dir, asset_name, shp_name)
                    self.gdal_clip(input_path, pixel_res, shapefile_path, output_path, data_type='UInt16')
                if save_rgb is True:
                    vsimem_path = '/vsimem/{}.tif'.format(asset_name)
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

        records_file.write('List of asset id of processed images: {}\n\n'.format(asset_id_list))
        time_str = datetime.now().strftime("%Y%m%d-%H%M%S")
        print('Finish clipping images:)')
        print('The outputs have been saved in this directory: ' + output_dir)
        records_file.write('The outputs have been saved in this directory: {}\n\n'.format(output_dir))
        records_file.write('End time: {}\n\n'.format(time_str))
        records_file.close()


# Testing
if __name__ == '__main__':
    ut = Utilities()
    ut.start_up()
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
    # ut.clip_clear_perc(shapefile_path=r'C:\Users\ChengY\Desktop\shp\bomas\layers\POLYGON.shp',
    #                    clear_perc_min=0.1, save_rgb=True, save_clip=True,
    #                    file_list=glob("{}\\{}\\*.tif".format(ut.work_dir, ut.output_dirs['clip'])))
