'''
filename: bog_api.py
author: Valeria Blaza
description: Retrieves data from BOG API
'''

import pandas as pd
import requests
from functools import reduce
from utilities import files


MAX_API_CALLS = 3


class BOG_API():
    '''
    Class to retrieve data from BOG API.
    '''

    def __init__(self):
        '''
        Constructor for the BOG API Class
        '''
        endpoint, username, password = files.get_auth()

        self.endpoint = endpoint
        self._username = username
        self._password = password
        
        self.get_token()
        self._header = {'Authorization': 'Bearer ' + str(self._token)}
        self.get_buoy_ids()


    def get_token(self):
        '''
        Retrieves authentication token for API access
        '''

        attempt = 0
        while attempt <= MAX_API_CALLS:
            attempt += 1
            r = requests.post(self.endpoint + "/auth", data={"type": "login",
                                "username": self._username,
                                "password": self._password})
            if r.ok:
                self._token = r.json()["token"]
                break

            if attempt == MAX_API_CALLS:
                self._token = None
                raise Exception('''User authentication failed: \n HTTP {} - {},
                                Message {}'''.format(r.status_code, r.reason, r.text))


    def get_buoy_ids(self):
        '''
        Retrieves list of buoy's for which the authenticated user has access to
        '''

        try:
            r = requests.get(self.endpoint + "/user", headers=self._header)
            self.buoy_ids = r.json()["buoys"]
        except:
            self.buoy_ids = []
            raise Exception('''Buoy id list request failed: \n HTTP {} - {}, 
                            Message {}'''.format(r.status_code, r.reason, r.text))


    def logout(self):
        '''
        Logs the authenticated user out of the API
        '''

        try:
            requests.post(self.endpoint + "/auth", data={"type": "logout"})
        except:
            raise Exception("Logout Failed")


    def get_current_status(self, buoy_id, check=True):
        '''
        Retrieves current status for a specified buoy_id
        Input(s): 
            - buoy_id (int): Buoy identification number (e.g., 133)
        Output(s): JSON file containing information from the 
            buoy's last update, including timestamp, latitude,
            longitude, battery, system status, and available variables
            for historical data retrieval
        '''

        if check:
            assert int(buoy_id) in self.buoy_ids, "Buoy does not exist"

        try:
            req_str = "/buoy/{}/details".format(str(buoy_id))
            r = requests.get(self.endpoint + req_str, headers=self._header)
            if r.ok:
                return r.json()
        except:
            raise Exception('''Current status request failed: \n HTTP {} - {}, 
                            Message {}'''.format(r.status_code, r.reason, r.text))


    def get_historical_data(self, buoy_id, series=None):
        '''
        Retrieves historical data for a specified buoy_id
        - Input(s):
            - buoy_id (int): Buoy identification number (e.g., 133)
            - series (lst): List of variables to be retrieved. If no
            variables are specified, retrieve all available variables
        - Output(s): JSON file containing all data information from 
            the buoy given the specified series.
        '''

        assert buoy_id in self.buoy_ids, "Buoy {} does not exist".format(buoy_id)
        available_vars = self.get_current_status(buoy_id)["series"]

        if not series:
            series = available_vars

        try:
            req_str = "/buoy/{}/reports?series={}".format(str(buoy_id), ','.join(series))
            r = requests.get(self.endpoint + req_str, headers=self._header)
            if r.ok:
                return r.json()
        except:
            raise Exception('''Historical data request failed: \n HTTP {} - {}, 
                            Message {}'''.format(r.status_code, r.reason, r.text))



    def create_buoy_df(self, buoy_id, series=None):
        '''
        Creates dataframe containing timeseries data for buoys
        Input(s):
            - buoy_id (int): Buoy identification number (e.g., 133)
            - series (lst): List of variables to be retrieved. If no
            variables are specified, retrieve all available variables
        Output(s):
            - final_df (pandas dataframe): dataframe containing the
            specified information for the given buoy
        '''
    
        assert buoy_id in self.buoy_ids, "Buoy {} does not exist".format(buoy_id)
        available_vars = self.get_current_status(buoy_id)["series"]

        if not series:
            series = available_vars

        assert set(series).issubset(available_vars), \
            "Variable(s) specified ({}) not available".format(", ".join(series))

        data = self.get_historical_data(buoy_id, series)["series"]["series"]

        dfs = [pd.DataFrame(data[var], columns=["time", \
               "value"]).rename(columns={"value":var}) for var in data]
        final_df = reduce(lambda left, right: pd.merge(left, right, on="time"), dfs)\
                          .rename(columns={"position_latitude":"buoy_lat", 
                                           "position_longitude":"buoy_lon"})

        final_df.insert(0, 'buoy_id', buoy_id)

        return final_df


    def build_historical_df(self, buoy_ids, series=None):
        '''
        Retrieves and concatenates dataframes for a list of buoys
        Input(s):
            - buoy_ids (lst): A list of buoy ids (e.g., [72, 76, 77])
            - series (lst): List of variables to be retrieved. If no
            variables are specified, retrieve all available variables
        Output(s): 
            - dataframe containing the specified information for 
            the given buoy
        '''

        assert buoy_ids, "Please include a list of buoy ids"
        dfs = [self.create_buoy_df(buoy_id, series) for buoy_id in buoy_ids]
        filename = "buoys/buoys_{}.tsv".format("_".join([str(b_id) for (b_id) in buoy_ids]))

        final_df = pd.concat(dfs, ignore_index=True)
        files.save_df(final_df, filename, index=False)

        self.logout()


    def build_current_df(self):
        '''
        Retrieves and concatenates dataframes for all the available
        buoys with each buoy's most recent location (lat/lon)
        Output(s): 
            - dataframe containing each buoy's most recent location
        '''

        data = pd.DataFrame([self.get_current_status(buoy_id, check=False) for buoy_id in \
               self.buoy_ids], columns=["buoy_id", "summary"])
        current_buoys = pd.concat([data.drop(['summary'], axis=1), data['summary'].apply(pd.Series)], \
                                   axis=1).rename(columns={"latitude" : "buoy_lat", 
                                                           "longitude" : "buoy_lon"})

        filename = "buoys/current_buoys_{}.tsv".format(current_buoys.last_updated.max())

        files.save_df(current_buoys, filename, index=False)
        self.logout()

        return current_buoys
