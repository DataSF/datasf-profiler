# coding: utf-8
#!/usr/bin/env python
#
from __future__ import division
import shutil
import inflection
import pandas as pd
import re
import csv
import inflection
import re
import datetime
import os
import requests
from sodapy import Socrata
import yaml
import base64
import itertools
import datetime
import json
import time
import logging
from retry import retry
import urllib2
import zipfile
from retry import retry
from socket import error as SocketError
import errno
from ConfigUtils import *
from Utils import *


class SocrataClient:
    def __init__(self, inputdir, configItems, logger):
        self.inputdir = inputdir
        self.configItems = configItems
        self._logger = logger

    def connectToSocrata(self):
        clientConfigFile = self.inputdir + self.configItems['socrata_client_config_fname']
        with open(clientConfigFile,  'r') as stream:
            try:
                client_items = yaml.load(stream)
                client = Socrata(client_items['url'],  client_items['app_token'], username=client_items['username'], password=base64.b64decode(client_items['password']))
                return client
            except yaml.YAMLError as exc:
                self._logger.error('Failed to open yaml file', exc_info=True)
        return 0

    def connectToSocrataConfigItems(self):
        clientConfigFile = self.inputdir + self.configItems['socrata_client_config_fname']
        with open(clientConfigFile,  'r') as stream:
            try:
                clientItems = yaml.load(stream)
                return clientItems
            except yaml.YAMLError as exc:
                print(exc)
                self._logger.error('Failed to open yaml file', exc_info=True)
        return 0


class SocrataCRUD:

    def __init__(self, client, clientItems, configItems, logger):
        self.client = client
        self.chunkSize = 1000
        self.row_id = configItems['row_id_field']
        self.name = configItems['dataset_name_field']
        self.fourXFour = configItems['fourXFour']
        self.rowsInserted = configItems['dataset_records_cnt_field']
        self.clientItems = clientItems
        self.isLoaded = configItems['isLoaded']
        self.src_records_cnt_field = configItems['src_records_cnt_field']
        self._logger = logger

    @retry( tries=5, delay=1, backoff=2)
    def insertDataSet(self, dataset, insertDataSet):
        insertChunks = self.makeChunks(insertDataSet)
        #overwrite the dataset on the first insert chunk[0] if there is no row id
        if dataset[self.rowsInserted] == 0 and dataset[self.row_id ] == '':
            msg =  "replacing and inserting " + dataset[self.name]
            print msg
            self._logger.info(msg)
            rejectedChunk = self.replaceDataSet(dataset, insertChunks[0])
            if len(insertChunks) > 1:
                for chunk in insertChunks[1:]:
                    rejectedChunk = self.insertData(dataset, chunk)
        else:
            msg =  "upserting... " + dataset[self.name]
            print msg
            self._logger.info(msg)
            for chunk in insertChunks:
                rejectedChunk = self.insertData(dataset, chunk)
        return dataset


    @retry( tries=10, delay=1, backoff=2)
    def replaceDataSet(self, dataset, chunk):
        result = self.client.replace( dataset[self.fourXFour], chunk )
        dataset[self.rowsInserted] = dataset[self.rowsInserted] + int(result['Rows Created'])
        time.sleep(0.25)


    @retry( tries=10, delay=1, backoff=2)
    def insertData(self, dataset, chunk):
        result = self.client.upsert(dataset[self.fourXFour], chunk)
        dataset[self.rowsInserted] = dataset[self.rowsInserted] + int(result['Rows Created']) + int(result['Rows Updated'])
        time.sleep(0.25)


    def makeChunks(self, insertDataSet):
        return [insertDataSet[x:x+ self.chunkSize] for x in xrange(0, len(insertDataSet), self.chunkSize)]


    def postDataToSocrata(self, dataset, insertDataSet ):
        if dataset[self.fourXFour]!= 0:
            dataset = self.insertDataSet(dataset, insertDataSet)
            dataset = self.checkCompleted(dataset)
        else:
            msg =  dataset[self.name] + "  does not exist"
            print msg
            self._logger.info(msg)
        return dataset

    def checkCompleted(self, dataset):
        if dataset[self.rowsInserted] == dataset[self.src_records_cnt_field]:
            dataset[self.isLoaded] = 'success'
        else:
            dataset[self.isLoaded] = 'failed'
            dataset['record_cnt_on_portal'] =  self.getRowCnt(dataset)
            if dataset['record_cnt_on_portal'] == dataset[self.src_records_cnt_field]:
                dataset[self.isLoaded] = 'success'
            #Up to 5% difference in the dataset file size to records on portal is okay
            diff = float(dataset[self.rowsInserted])/float(dataset[self.src_records_cnt_field])
            if diff < 0.05:
                dataset[self.isLoaded] = 'success'
        if dataset[self.isLoaded] == 'success':
            msg =  "data insert success for " + dataset[self.name] + " !"  + " Loaded " + str(dataset[self.rowsInserted]) + "rows!"
            print msg
            self._logger.info(msg)
        return dataset

    def getRowCnt(self, dataset):
        time.sleep(1)
        qry = '?$select=count(*)'
        qry = "https://"+ self.clientItems['url']+"/resource/" +dataset[self.fourXFour]+ ".json" + qry
        row_count  = None
        try:
            r = requests.get( qry , auth=( self.clientItems['username'],  base64.b64decode(self.clientItems['password'])))
            cnt =  r.json()
            row_count = int(cnt[0]['count'])
        except Exception, e:
            print str(e)
            self._logger.info(str(e))
        return row_count


class SocrataQueries:
    def __init__(self, clientItems, configItems, logger):
        self.base_url = configItems['base_url']
        self.full_url = "https://"+ configItems['base_url'] +"/resource/"
        self.username = clientItems['username']
        self.passwd =  clientItems['password']
        self._logger = logger
        self.rowsInserted = configItems['dataset_records_cnt_field']
        self.src_records_cnt_field = configItems['src_records_cnt_field']

    def getRowCnt(self, fourXFour):
        time.sleep(1)
        qry = '?$select=count(*)'
        qry = self.full_url +fourXFour+ ".json" + qry
        count = None
        try:
            r = requests.get( qry , auth=( self.username, base64.b64decode(self.passwd)))
            cnt =  r.json()
            count =  int(cnt[0]['count'])
        except Exception, e:
            print str(e)
            self._logger.info(str(e))
        return count

    def getQry(self, fourXFour, qry):
        qry = self.full_url +fourXFour+ ".json" + qry
        r = requests.get( qry , auth=( self.username, base64.b64decode(self.passwd)))
        return r.json()

    def getQryFull(self, qry):
        try:
            r = requests.get( qry , auth=( self.username, base64.b64decode(self.passwd)))
            if r.status_code != 200:
                return None
            return r.json()
        except Exception, e:
            print str(e)
            return None

    def pageThroughResultsSelect(self, fbf, qry_cols):
        row_cnt = self.getRowCnt(fbf)
        returned_records = 0
        offset = 0
        all_results = []
        while offset < row_cnt:
            limit_offset = "&$limit=1000&$offset="+ str(offset)
            qry = '?$select='+qry_cols+ limit_offset
            try:
                results = self.getQry(fbf, qry)
            except Exception, e:
                print str(e)
                self._logger.info(str(e))
            try:
                all_results =  all_results + results
            except Exception, e:
                print results
                self._logger.info(results)
                self._logger.info(str(e))
                print str(e)
                break
            offset = offset + 1000
        return all_results



    def setDatasetDicts(self, dataset):
        dataset[self.rowsInserted] = 0
        dataset[self.src_records_cnt_field] = 0
        return dataset

if __name__ == "__main__":
    main()
