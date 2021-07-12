from hashlib import sha1
import hmac
from wsgiref.handlers import format_date_time
from datetime import datetime
from time import mktime
import base64
import requests
from pprint import pprint
import json

import os
import pandas as pd
import shutil
from tqdm import tqdm
from datetime import datetime
from dateutil.rrule import rrule, DAILY
import dload

class Auth():

    def __init__(self, app_id, app_key): 
        self.app_id = app_id
        self.app_key = app_key

    def get_auth_header(self):
        xdate = format_date_time(mktime(datetime.now().timetuple()))
        hashed = hmac.new(self.app_key.encode('utf8'), ('x-date: ' + xdate).encode('utf8'), sha1)
        signature = base64.b64encode(hashed.digest()).decode()

        authorization = 'hmac username="' + self.app_id + '", ' + \
                        'algorithm="hmac-sha1", ' + \
                        'headers="x-date", ' + \
                        'signature="' + signature + '"'
        return {
            'Authorization': authorization,
            'x-date': format_date_time(mktime(datetime.now().timetuple())),
            'Accept - Encoding': 'gzip'
        }


class APIConnector:

    url_GantryInfo = "https://traffic.transportdata.tw/MOTC/v2/Road/Traffic/ETag/Freeway/"
    url_suf = "?$format=JSON"

    def __init__(self):
        with open("key.json") as jsonFile:
            keys = json.load(jsonFile)
            jsonFile.close()

        self.app_id = keys['app_id']
        self.app_key = keys['app_key']
        self.auth = Auth(self.app_id, self.app_key)

    def GantryInfo(self, ETagGantryID: str) -> tuple:
        '''
        To get the position(longtitude & latitude) of a ETaGantry

        Args:
            ETagGantryID (str):
                the ETagGantryID of you target Gantry

        Yields:
            (tuple)
            longtitude and latitude of the Gantry
        '''
        url =  self.url_GantryInfo + ETagGantryID + self.url_suf

        headers = self.auth.get_auth_header()

        r = requests.get(url, headers = self.auth.get_auth_header())
        json_file = r.json()
        
        try:
            lon = json_file['ETags'][0]['PositionLon']
            lat = json_file['ETags'][0]['PositionLat']
        except:
            print(ETagGantryID, "can't find match ID")
            return (0, 0)

        return (lon, lat)

    def RouteInfo(self, raw_route: str) -> list:
        '''
        To get the detailed route position(longtitude & latitude) from ETaGantry IDs

        Args:
            raw_route (str):
                the original data in M06 records

        Yields:
            (list) [ date_time, (float, float) ]
            list of time and positions(longtitude and latitude) of Gantrys
        '''
        route = [i.split('+') for i in raw_route.split('; ')]

        for i in route:
            i[0] = datetime.strptime(i[0], "%Y-%m-%d %H:%M:%S")
            i[1] = self.GantryInfo(i[1])

        return route
        
    def GantryName(self, ETagGantryID: str) -> str:
        url =  self.url_GantryInfo + ETagGantryID + self.url_suf

        headers = self.auth.get_auth_header()

        r = requests.get(url, headers = self.auth.get_auth_header())
        json_file = r.json()
        
        try:
            rod_nam = json_file['ETags'][0]["RoadName"]
            rod_sta = json_file['ETags'][0]["RoadSection"]["Start"]
            rod_end = json_file['ETags'][0]["RoadSection"]["End"]
        except:
            print(ETagGantryID, "can't find match ID")
            return "-1"

        return f"{rod_nam} {rod_sta}-{rod_end}"


class DfLoader:
    def single_get(self, name, date, sv_name):
        to_get = f'https://tisvcloud.freeway.gov.tw/history/TDCS/{name}/{name}_{date}.tar.gz'

        tar_bytes = dload.bytes(to_get)
        if (tar_bytes == b''): 
            raise ValueError('Wrong (name,date) or server down!')
        
        with open('myfile.tar.gz', 'wb') as w:
            w.write(tar_bytes)
            
        shutil.unpack_archive("myfile.tar.gz", f'extracted/{sv_name}')
        os.remove('myfile.tar.gz')
            
    def download_data(self, name, start, end = None):
        if (end == None): end = start
        a = datetime.strptime(start, '%Y%m%d')
        b = datetime.strptime(end, '%Y%m%d')
        for dt in tqdm(rrule(DAILY, dtstart=a, until=b)):
            date = dt.strftime("%Y%m%d")
            self.single_get(name, date, f'{start}_{end}')

    def to_df(self, fpath, colnames):
        csvs = []
        days_folders = os.listdir(fpath)
        
        for day in days_folders :
            P1 = os.path.join(fpath, day)
            day_folder = sorted(os.listdir(P1))
            
            for time in day_folder :
                P2 = os.path.join(P1, time)
                time_folder = sorted(os.listdir(P2))

                for csv_name in time_folder :
                    csv_loc = os.path.join(P2, csv_name)
                    csvs.append(csv_loc)

        li = []
        for csv_path in tqdm(csvs) :
          df = pd.read_csv(csv_path, names=COL_NAMES)
        li.append(df)

        all_df = pd.concat(li, axis=0, ignore_index=True)
        return all_df
        
    def get_df(self, name, start, end, col_name = None):
        try:
            f_name = f'{start}_{end}'
            path = os.path.join("extracted/", f_name)
            path = os.path.join(path, name)

            if col_name == None:
                if name == "M03A": col_name = ['TimeInterval', 'GantryID', 'Direction', 'VehicleType', '交通量']
                if name == "M04A": col_name = ['TimeInterval', 'GantryFrom', 'GantryTo', 'VehicleType', 'TravelTime', '交通量']
                if name == "M05A": col_name = ['TimeInterval', 'GantryFrom', 'GantryTo', 'VehicleType', 'SpaceMeanSpeed', '交通量']
                if name == "M06A": col_name = ['VehicleType', 'DetectionTime_O', 'GantryID_O', 'DetectionTime_D', 'GantryID_D', 'TripLength', 'TripEnd', 'TripInformation']
                if name == "M07A": col_name = ['TimeInterval', 'GantryFrom', 'VehicleType', '旅次平均長度', '交通量']
                if name == "M08A": col_name = ['TimeInterval', 'GantryFrom', 'GantryTo', 'VehicleType', '交通量']
            
            if not os.path.isdir(path):
                  self.download_data(name, start, end)
            
            df = self.to_df(path, col_name)
            return df
            
        except Exception as ex:
            print(f'Exception: {ex}')
