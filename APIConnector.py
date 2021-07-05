from hashlib import sha1
import hmac
from wsgiref.handlers import format_date_time
from datetime import datetime
from time import mktime
import base64
import requests
from pprint import pprint
import json

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