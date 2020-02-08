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
0. data format and merge 
1. Remove clouds
2. Stack NDVI and cloud mask
3. Plot time series (merge and cloud)
4. Map or apply
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

sys.path.append(r'C:\Users\ChengY\AppData\Local\Continuum\anaconda3\Lib\site-packages\osgeo\scripts')
os.environ["GDAL_DATA"] = r'C:\Users\ChengY\AppData\Local\Continuum\anaconda3\Lib\site-packages\osgeo\data\gdal'
gdal_calc_path = r'C:\Users\ChengY\AppData\Local\Continuum\anaconda3\Lib\site-packages\osgeo\scripts\gdal_calc.py'


class Utilities:
    '''Commonly used tools for the processing of PlanetScope imagery
    :param:
    '''

    # Environment settings
    default_work_dir = r'C:\Users\ChengY\Desktop'
    default_output_dirs = {'raw': 'raw', 'clip': 'clip', 'clipped raw': 'clipped_raw',
                           'cloud mask': 'cloud_mask', 'NDVI': 'NDVI'}
    # API Key
    default_api_key = "9cada8bc134546fe9c1b8bce5b71860f"
    # Specs
    default_satellite = 'PS'
    default_proj_code = 32737
    # Filter settings
    default_filter_items = ['date', 'cloud_cover', 'aoi']
    default_item_types = ["PSScene4Band"]
    default_asset_types = ['udm2']
    default_start_date = '2019-04-01'
    default_end_date = '2019-04-10'
    default_cloud_cover = 0.8
    default_aoi_shp = r'D:\Kapiti\supplementary_data\Kapiti_Jun18_v2_prj.shp'

    def __init__(self, work_dir=default_work_dir, output_dirs=default_output_dirs, satellite=default_satellite,
                 proj_code=default_proj_code, api_key=default_api_key, filter_items=default_filter_items,
                 item_types=default_item_types, asset_types=default_asset_types, start_date=default_start_date,
                 end_date=default_end_date, cloud_cover=default_cloud_cover, aoi_shp=default_aoi_shp):
        '''

        :param work_dir: string, work directory path
        :param output_dirs: dictionary, a dictionary of folder names for storing different outputs
        :param satellite: string, abbreviation of satellite imagery
        :param proj_code: int, EPSG code of projection system
        :param api_key: string, API key for accessing Plant data
        :param filter_items: list, a list of filter names
        :param item_types: list, a list of item types
        :param asset_types: list, a list of asset types
        :param start_date: string, start date with the format of YYYY-MM-DD
        :param end_date: string, end date with the format of YYYY-MM-DD
        :param cloud_cover: int, upper limit of cloud cover
        :param aoi_shp: string, the file path of AOI shapefile
        '''

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
        self.id_list = None


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

    def download_main(self, item, asset_type):
        '''
        Activate and download individual asset with specific item id and asset type
        :param item:
        :param asset_type:
        :return:
        '''

        output_dir = self.work_dir + '\\' + self.output_dirs['raw']
        records_file = open(self.records_path, "a+")

        # Get asset and its activation status
        assets = self.client.get_assets(item).get()
        # print(assets.keys())
        activation = self.client.activate(assets[asset_type])
        print(activation.response.status_code)
        if asset_type in assets.keys():
            asset_activated = False
            while not asset_activated:
                asset = assets.get(asset_type)
                asset_status = asset["status"]
                # If asset is already active, we are done
                if asset_status == 'active':
                    asset_activated = True
                    print("Asset is active and ready to download")
                    callback = api.write_to_file(directory=output_dir)
                    body = self.client.download(assets[asset_type], callback=callback)
                    body.wait()
                # Still activating. Wait and check again.
                else:
                    print("...Still waiting for asset activation...")
                    time.sleep(10)
        else:
            records_file.write("{} {}\n".format(item['id'], asset_type))

        records_file.close()

    def download_clipped(self, item):
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
                    'item_id': item['id'],
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
                with open(+ item['id'] + '.zip', "wb") as handle:
                    for data in tqdm(response.iter_content()):
                        handle.write(data)
                # Unzip file
                ziped_item = zipfile.ZipFile(clipped_raw_dir + '\\' + item['id'] + '.zip')
                ziped_item.extractall(clipped_raw_dir + '\\' + item['id'])
                # Delete zip file
                os.remove(clipped_raw_dir + '\\' + item['id'] + '.zip')
            else:
                # Still activating. Wait 1 second and check again.
                print("...Still waiting for clipping to complete...")
                time.sleep(1)

    def download_assets(self, clipped=None):
        '''
        Download all assets
        :param clipped: Boolean, True means downloading clipped assests, otherwise downloading raw imagery
        :return:
        '''

        print('Start to download assets :)')

        if clipped is None:
            clipped = False

        time_str = datetime.now().strftime("%Y%m%d-%H%M%S")
        records_path = self.work_dir + '\\records_{}.txt'.format(time_str)
        if not os.path.exists(records_path):
            records_file = open(records_path, "w")
            records_file.close()
        self.records_path = records_path

        and_filter = self.create_filter()
        req = filters.build_search_request(and_filter, self.item_types)
        res = self.client.quick_search(req)
        self.id_list = [i['id'] for i in res.items_iter(250)]
        # print(self.id_list)
        for item in tqdm(res.items_iter(250), total=len(self.id_list), unit="item", desc='Downloading assets'):
            # print(item['id'], item['properties']['item_type'])
            if not clipped:
                for asset_type in self.asset_types:
                    self.download_main(item, asset_type)
            else:
                self.download_clipped(item)

        print('Finish downloading assets :)')
        print('The raw images have been saved in this directory: ' + self.work_dir + '\\' + self.output_dirs['raw'])
        print('The information of missing assets has be saved in this file: ' + self.records_path)


    # def gdal_merge(self):

    def gdal_clip(self, file_list=None):
        '''
        Clip imagery to the extent of AOI
        :return:
        '''

        print('Start GDAL clip :)')
        output_dir = self.work_dir + '\\' + self.output_dirs['clip']
        input_dir = self.work_dir + '\\' + self.output_dirs['raw']

        Shapefile = self.aoi_shp
        cutlineLayer = Shapefile.split('\\')[-1].split('.')[0]
        RasterFormat = 'GTiff'
        VectorFormat = 'ESRI Shapefile'
        PixelRes = self.pixel_res(self.satellite)

        if file_list is None:
            file_list = []
            for i in self.id_list:
                a = glob("{}\\{}*.tif".format(input_dir, i))
                for j in a:
                    file_list.append(j)
            # print(file_list)

        for i in tqdm(file_list, total=len(file_list), unit="item", desc='Clipping images'):
            file_name = i.split('\\')[-1]
            InputImage = i
            # Open datasets
            Raster = gdal.Open(InputImage, gdal.GA_ReadOnly)
            # Projection = Raster.GetProjectionRef()
            # print(Projection)
            VectorDriver = ogr.GetDriverByName(VectorFormat)
            VectorDataset = VectorDriver.Open(Shapefile, 0)  # 0=Read-only, 1=Read-Write
            layer = VectorDataset.GetLayer()
            feature = layer.GetFeature(0)
            geom = feature.GetGeometryRef()
            minX, maxX, minY, maxY = geom.GetEnvelope()  # Get bounding box of the shapefile feature
            # Create raster
            OutTileName = output_dir + '\\' + file_name
            OutTile = gdal.Warp(OutTileName, Raster, format=RasterFormat,
                                outputBounds=[minX, minY, maxX, maxY],
                                xRes=PixelRes, yRes=PixelRes,
                                targetAlignedPixels=True,
                                # dstSRS='epsg:{}'.format(str(self.proj_code)),
                                resampleAlg=gdal.GRA_NearestNeighbour,
                                cutlineDSName=Shapefile,
                                cutlineLayer=cutlineLayer,
                                cropToCutline=True,
                                dstNodata=-9999,
                                options=['COMPRESS=LZW'])
            OutTile = None  # Close dataset

        # Close datasets
        Raster = None
        VectorDataset.Destroy()
        print('Finish GDAL Clip :)')
        print('The outputs have been saved in this directory: ' + output_dir)

    def gdal_cal(self, type_, file_list=None):
        '''
        Band algebra for cloud mask and NDVI
        :return:
        '''

        print('Start GDAL band calculation :)')
        input_dir = self.work_dir + '\\' + self.output_dirs['clip']

        if type_ == 'cloud mask':
            cloud_mask_dir = self.work_dir + '\\' + self.output_dirs['cloud mask']
            if file_list is None:
                file_list = []
                for i in self.id_list:
                    a = glob("{}\\{}*udm2.tif".format(input_dir, i))
                    for j in a:
                        file_list.append(j)
            for udm2_path in file_list:
                print(udm2_path)
                cloud_mask_path = cloud_mask_dir + '\\' + udm2_path.split('\\')[-1]
                gdal_calc_str = 'python {0} --calc "A*B" --co="COMPRESS=LZW" --format GTiff --type Int16 -A {1} ' \
                                '--A_band 1 -B {2} --B_band 7 --outfile {3} --overwrite'
                gdal_calc_process = gdal_calc_str.format(gdal_calc_path, udm2_path, udm2_path, cloud_mask_path)
                os.system(gdal_calc_process)
            print('Finish GDAL Calculation :)')
            print('The outputs have been saved in this directory: ' + cloud_mask_dir)

        if type_ == 'NDVI':
            ndvi_dir = self.work_dir + '\\' + self.output_dirs['NDVI']
            if file_list is None:
                file_list = []
                for i in self.id_list:
                    a = glob("{}\\{}SR*.tif".format(input_dir, i))
                    for j in a:
                        file_list.append(j)
            for sr_path in file_list:
                ndvi_path = ndvi_dir + '\\' + sr_path.split('\\')[-1]
                gdal_calc_str = 'python {0} --calc "(A-B)/(A+B)*10000" --co="COMPRESS=LZW" --format GTiff ' \
                                '--type Int16 -A {1} --A_band 4 -B {2} --B_band 3 --outfile {3} --overwrite'
                gdal_calc_process = gdal_calc_str.format(gdal_calc_path, sr_path, sr_path, ndvi_path)
                os.system(gdal_calc_process)
            print('Finish GDAL Calculation :)')
            print('The outputs have been saved in this directory: ' + ndvi_dir)


# Testing
if __name__ == '__main__':
    ut = Utilities()
    # ut.id_list = ['20190107_074019_1049']
    ut.setup_dirs()
    ut.download_assets()
    ut.gdal_clip()
    ut.gdal_cal(type_='cloud mask')


