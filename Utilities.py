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
1. conda install -c conda-forge geopandas
2. remove gdal from site-packages
3. remove pyproj from site-packages
4. pip install pyproj
5. pip install planet
6. install gdal using the .whl file from https://www.lfd.uci.edu/~gohlke/pythonlibs/
    pip install [PATH OF GDAL WHL FILE]

To do list   
====================================== 
- record all steps
# Create a txt file to record:
# 1. Date
# 2. Item id of downloaded items
- Remove clouds
- Stack NDVI and clear prob
- Plot time series (merge and cloud)
- Map or apply
- satellite id... split
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
import numpy as np
import requests
import sys
import zipfile
import warnings
warnings.simplefilter('ignore')

class Utilities:
    '''Commonly used tools for the processing of PlanetScope imagery
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
                           'clear prob': 'clear_prob', 'NDVI': 'NDVI'}
    # API Key
    default_api_key = "9cada8bc134546fe9c1b8bce5b71860f"
    # Specs
    default_satellite = 'PS'
    default_proj_code = 32737
    # Filter settings
    default_filter_items = ['date', 'cloud_cover', 'aoi']
    default_item_types = ["PSScene4Band"]
    default_asset_types = ['analytic_sr', 'udm2']
    default_start_date = '2019-01-01'
    default_end_date = '2019-01-31'
    default_cloud_cover = 0.8
    default_aoi_shp = r'D:\Kapiti\supplementary_data\Kapiti_Jun18_v2_prj.shp'

    def __init__(self, gdal_scripts_path=default_gdal_scripts_path, gdal_data_path=default_gdal_data_path,
                 gdal_calc_path=default_gdal_calc_path, gdal_merge_path=default_gdal_merge_path,
                 gdal_translate_path=default_gdal_translate_path, work_dir=default_work_dir,
                 output_dirs=default_output_dirs, satellite=default_satellite, proj_code=default_proj_code,
                 api_key=default_api_key, filter_items=default_filter_items, item_types=default_item_types,
                 asset_types=default_asset_types, start_date=default_start_date, end_date=default_end_date,
                 cloud_cover=default_cloud_cover, aoi_shp=default_aoi_shp):
        '''

        :param gdal_scripts_path:
        :param gdal_data_path:
        :param gdal_calc_path:
        :param gdal_merge_path:
        :param work_dir:
        :param output_dirs:
        :param satellite:
        :param proj_code:
        :param api_key:
        :param filter_items:
        :param item_types:
        :param asset_types:
        :param start_date:
        :param end_date:
        :param cloud_cover:
        :param aoi_shp:
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
        # Convert AOI shapefile to json format that is required for retrieve imagery
        # for specific location using Planet API
        shp = gpd.read_file(self.aoi_shp)
        if shp.crs != {'init': 'epsg:{}'.format(str(self.proj_code))}:
            shp = shp.to_crs({'init': 'epsg:{}'.format(str(self.proj_code))})
            # save to file
        else:
            shp2 = shp.to_crs({'init': 'epsg:4326'})
        coors = np.array(dict(json.loads(shp2['geometry'].to_json()))['features'][0]['geometry']['coordinates'])[:, :,
                0:2].tolist()
        self.aoi_geom = {"type": "Polygon", "coordinates": coors}
        # print(self.aoi_geom)
        # Create empty variables
        self.records_path = None
        self.id_list_download = None


    @staticmethod
    def pixel_res(arg):
        '''

        :param arg:
        :return:
        '''
        switch = {
            'PS': 3,
            'S2': 30
        }
        return switch.get(arg, 'None')

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

    def create_filter(self):
        '''
        Set filters
        :return: filter
        '''

        if 'date' in list(self.filter_items):
            date_filter = filters.date_range('acquired', gte=self.start_date, lte=self.end_date)
            and_filter = date_filter
        if 'cloud_cover' in list(self.filter_items):
            cloud_filter = filters.range_filter('cloud_cover', lte=self.cloud_cover)
            and_filter = filters.and_filter(and_filter, cloud_filter)
        if 'aoi' in list(self.filter_items):
            aoi_filter = filters.geom_filter(self.aoi_geom)
            and_filter = filters.and_filter(and_filter, aoi_filter)
        return and_filter

    def download_one(self):
        response = requests.get(download_url, stream=True)
        with open('output/' + image_id + '.zip', "wb") as handle:
            for data in tqdm(response.iter_content()):
                handle.write(data)

        # Unzip file
        ziped_item = zipfile.ZipFile('output/' + image_id + '.zip')
        ziped_item.extractall('output/' + image_id)

    def download_main(self, item_id, asset_type):
        '''
        Activate and download individual asset with specific item id and asset type
        :param item:
        :param asset_type:
        :return: asset_exist, bolean
        '''

        output_dir = self.work_dir + '\\' + self.output_dirs['raw']
        records_file = open(self.records_path, "a+")

        for item_type in self.item_types:
            # Get asset and its activation status
            assets = self.client.get_assets_by_id(item_type, item_id).get()
            # print(assets.keys())
            if asset_type in assets.keys():
                # Activate
                # activation = self.client.activate(assets[asset_type])
                # print(activation.response.status_code)
                asset_activated = False
                i = 0
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
                        i+=1
                        print(i)
                        # print("...Still waiting for asset activation...")
                        time.sleep(60)
                # Download
                callback = api.write_to_file(directory=output_dir)
                body = self.client.download(assets[asset_type], callback=callback)
                # body.wait()
            else:
                records_file.write("{} {} {}\n".format(item_id, asset_type, item_type))
                asset_exist =False

        records_file.close()

        return asset_exist, item_type

    def download_clipped(self, item_id):
        '''
        Activate and download clipped assets
        :param item:
        :return:
        '''

        # Create new folder
        clipped_raw_dir = self.work_dir + '\\' + self.output_dirs['clipped_raw']
        self.create_dir(clipped_raw_dir)
        print('The clipped raw images will be saved in this directory: ' + clipped_raw_dir)
        # Construct clip API payload
        clip_payload = {
            'aoi': self.aoi_geom,
            'targets': [{
                    'item_id': item_id,
                    'item_type': "PSScene4Band",
                    'asset_type': 'analytic_sr'
                }]}
        # Request clip of scene (This will take some time to complete)
        request = requests.post('https://api.planet.com/compute/ops/clips/v1', auth=(self.api_key, ''),
                                json=clip_payload)
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
                print("Clip of scene succeeded and is ready to download")
                # Download clip
                response = requests.get(clip_download_url, stream=True)
                with open(+ item_id + '.zip', "wb") as handle:
                    for data in tqdm(response.iter_content()):
                        handle.write(data)
                # Unzip file
                ziped_item = zipfile.ZipFile(clipped_raw_dir + '\\' + item_id + '.zip')
                ziped_item.extractall(clipped_raw_dir + '\\' + item_id)
                # Delete zip file
                os.remove(clipped_raw_dir + '\\' + item_id + '.zip')
            else:
                # Still activating. Wait 1 second and check again.
                print("...Still waiting for clipping to complete...")
                time.sleep(1)

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
        if id_list_exist_udm2 >= id_list_exist_sr:
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

        # Download clipped assets or not
        if clipped is None:
            clipped = False

        # Create a txt file to record:
        # 1. Date
        # 2. Item id that does not have required types of asset
        # 3. Item id of downloaded items
        # 4. Metadata of each downloaded item
        time_str = datetime.now().strftime("%Y%m%d-%H%M%S")
        records_path = self.work_dir + '\\records_{}.txt'.format(time_str)
        if not os.path.exists(records_path):
            records_file = open(records_path, "w")
            records_file.close()
        self.records_path = records_path

        # Create filter
        and_filter = self.create_filter()
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
                for asset_type in self.asset_types:
                    asset_exist, item_type = self.download_main(item_id, asset_type)
                    if asset_exist is True:
                        metadata = [i for i in res.items_iter(250) if i['id'] == item_id]
                        records_file = open(self.records_path, "a+")
                        records_file.write('Metadata for {}_{}_{}\n{}'.format(item_id, asset_type, item_type, metadata))
                        records_file.close()
            else:
                self.download_clipped(item_id)

        print('Finish downloading assets :)')
        print('The raw images have been saved in this directory: ' + self.work_dir + '\\' + self.output_dirs['raw'])
        print('The information of missing assets has be saved in this file: ' + self.records_path)

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

        :param file_list:
        :return:
        '''

        print('Start to process udm2 data :)')
        input_dir = self.work_dir + '\\' + self.output_dirs['raw']
        output_dir = input_dir

        if file_list is None:
            file_list = []
            for i in self.id_list_download:
                a = glob("{}\\{}*udm2.tif".format(input_dir, i))
                for j in a:
                    file_list.append(j)

        for input_path in tqdm(file_list, total=len(file_list), unit="item", desc='Processing udm2 data'):
            output_path = output_dir + '\\' + input_path.split('\\')[-1].split('.')[0] + '_setnull.tif'
            self.gdal_udm2_setnull(input_path, output_path)

        print('Finish processing udm2 data :)')
        print('The outputs have been saved in this directory: ' + self.work_dir + '\\' + self.output_dirs['raw'])

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

        self.udm2_setnull()

        print('Start to merge images collected in the same day on the same orbit :)')
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

        print('Finish merging images :)')
        print('The merged images have been saved in this directory: ' + self.work_dir + '\\' + self.output_dirs['merge'])

    @staticmethod
    def gdal_clip(input_path, pixel_res, shapefile_path, cut_line_name, output_path):
        '''
        GDAL clip function
        :param input_file_path:
        :param pixel_res:
        :param shapefile_path:
        :param cut_line_name:
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
        # Create raster
        OutTile = gdal.Warp(output_path, raster, format='GTiff',
                            outputType=gdal.GDT_Byte,
                            outputBounds=[minX, minY, maxX, maxY],
                            xRes=pixel_res, yRes=pixel_res,
                            targetAlignedPixels=True,
                            # dstSRS='epsg:{}'.format(str(self.proj_code)),
                            resampleAlg=gdal.GRA_NearestNeighbour,
                            cutlineDSName=shapefile_path,
                            cutlineLayer=cut_line_name,
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
        input_dir = self.work_dir + '\\' + self.output_dirs['merge']
        output_dir = self.work_dir + '\\' + self.output_dirs['clip']

        pixel_res = self.pixel_res(self.satellite)
        shapefile_path = self.aoi_shp
        cut_line_name = shapefile_path.split('\\')[-1].split('.')[0]

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
            self.gdal_clip(input_path, pixel_res, shapefile_path, cut_line_name, output_path)

        print('Finish clipping images :)')
        print('The clipped images have been saved in this directory: ' + output_dir)

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
        input_dir = self.work_dir + '\\' + self.output_dirs['clip']

        if output_type == 'clear prob':
            clear_prob_dir = self.work_dir + '\\' + self.output_dirs['clear prob']
            if file_list is None:
                file_list = []
                for i in self.id_list_download:
                    a = glob("{}\\{}*udm2.tif".format(input_dir, i))
                    for j in a:
                        file_list.append(j)
            for udm2_path in file_list:
                clear_prob_path = clear_prob_dir + '\\' + udm2_path.split('\\')[-1]
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
            for sr_path in file_list:
                ndvi_path = ndvi_dir + '\\' + sr_path.split('\\')[-1]
                self.gdal_calc_ndvi(input_path=sr_path, output_path=ndvi_path)
            print('Finish GDAL Calculation :)')
            print('The outputs have been saved in this directory: ' + ndvi_dir)

    def mask_cloud(self):
        '''
        Remove cloudy pixels
        :return:
        '''

    def stack_bands(self):
        '''

        :return:
        '''

    def plot_time_series(self):
        '''

        :return:
        '''

    def plot_thrumbnail(self):
        '''

        :return:
        '''


# Testing
if __name__ == '__main__':
    ut = Utilities()
    ut.setup_dirs()
    # ut.id_list_download = ['20190107_074019_1049', '20190107_074018_1049']
    ut.download_assets()
    ut.merge()
    ut.clip()
    ut.band_algebra()
    # date_list = [i.split('_')[0] for i in ut.id_list_download]
    # for date in date_list:
    #     file_list = []
    #     a = glob("{}\\{}*udm2.tif".format(ut.work_dir + '\\merge', date))
    #     for i in a:
    #         file_list.append(i)
    # ut.clip(file_list=file_list)
    # ut.band_algebra(output_type='clear prob', file_list=file_list)


