# coding: utf-8
import csv
import time
import os
import itertools
import base64
import inflection
import csv, codecs, cStringIO
import glob
import math
import pycurl
from io import BytesIO
import pandas as pd
import requests
import shutil
from csv import DictWriter
from cStringIO import StringIO
import datetime
import collections
import os.path

class DateUtils:
    @staticmethod
    def get_current_date_month_day_year():
        return datetime.datetime.now().strftime("%m/%d/%Y")
        #datetime.datetime(2017, 1, 13, 10, 42, 2, 313956)

    @staticmethod
    def get_current_date_year_month_day():
        return datetime.datetime.now().strftime("%Y_%m_%d_")

    @staticmethod
    def get_current_timestamp():
        '''returns a timestamp of the current time in the format: 2017-01-13T10:53:33.858411Z'''
        return datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    @staticmethod
    def compare_two_timestamps(t1, t2, dt_format1=None, dt_format2=None):
        '''compares two timestamps a particular time format;
           returns the true when t1 is larger than t2'''
        default_time_format = '%Y-%m-%dT%H:%M:%S.%fZ'
        if dt_format1 is None:
            dt_format1 =  default_time_format
        if dt_format2 is None:
            dt_format2 =  default_time_format
        t1_dtt =  datetime.datetime.strptime(t1,dt_format1)
        t2_dtt =  datetime.datetime.strptime(t2,dt_format2)
        t1_dtt = t1_dtt.replace(second=0, microsecond=0)
        t2_dtt = t2_dtt.replace(second=0, microsecond=0)
        if t1_dtt > t2_dtt:
            return True
        elif t2_dtt > t1_dtt:
            return False
        return False

class PickleUtils:
    @staticmethod
    def pickle_cells(cells, pickle_name ):
        pickle.dump( cells, open(picked_dir + pickle_name + "_pickled_cells.p", "wb" ) )

    @staticmethod
    def unpickle_cells(pickle_name):
        return pickle.load( open(picked_dir + pickle_name +"_pickled_cells.p", "rb" ) )


class UnicodeWriter:
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        self.writer.writerow([s.encode("utf-8") for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)

class FileUtils:
    '''class for file/os util functions'''
    @staticmethod
    def fileExists(file_path):
        return os.path.exists(file_path)

    @staticmethod
    def read_csv_into_dictlist(fn):
        dictList = []
        if os.path.exists(fn):
            with open(fn) as f:
                dictList = [row for row in csv.DictReader(f, skipinitialspace=True)]
        return dictList

    @staticmethod
    def getFileListForDir(filepath_str_to_search):
        '''gets file list in a directory based on some path string to search- ie: /home/adam/*.txt'''
        return glob.glob(filepath_str_to_search)

    @staticmethod
    def getAttachmentFullPath(output_dir, output_fn, download_url):
        '''downloads an attachment from whereever'''
        #equivelent to: curl -L "https://screendoor.dobt.co/attachments/s5wflD750Nxhai9MfNmxes4TR-0xoDyw/download" > whateverFilename.csv
        # As long as the file is opened in binary mode, can write response body to it without decoding.
        downloaded = False
        try:
            with open(output_dir + output_fn, 'wb') as f:
                c = pycurl.Curl()
                c.setopt(c.URL, download_url)
                # Follow redirect.
                c.setopt(c.FOLLOWLOCATION, True)
                c.setopt(c.WRITEDATA, f)
                c.perform()
                c.close()
                downloaded = True
        except Exception, e:
            print str(e)
        return downloaded

    @staticmethod
    def getFiles(output_dir, output_fn, download_url ):
        dowloaded = False
        r = requests.get(download_url, stream=True)
        with open(output_dir+output_fn, 'wb') as f:
            shutil.copyfileobj(r.raw, f)
            downloaded = True
        return downloaded

    @staticmethod
    def remove_files_on_regex(dir, regex):
        files_to_remove =  FileUtils.getFileListForDir(dir + regex )
        for the_file in files_to_remove:
            try:
                if os.path.isfile(the_file):
                    os.unlink(the_file)
                #this would remove subdirs
                #elif os.path.isdir(file_path): shutil.rmtree(file_path)
            except Exception as e:
                print(e)

    @staticmethod
    def write_wkbk_csv(fn, dictList, headerCols):
        wrote_wkbk = False
        with open(fn, 'w') as csvfile:
            try:
                fieldnames = headerCols
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                for data in dictList:
                    #print data
                    try:
                        writer.writerow({ s:str(v).encode("ascii",  'ignore') for s, v in data.iteritems()  } )
                    except Exception, e:
                        print str(e)
                        print "could not write row"
                wrote_wkbk = True
            except Exception, e:
                print str(e)
        return wrote_wkbk


class ListUtils:

    '''class for list util functions'''
    @staticmethod
    def flatten_list(listofLists):
        return [item for sublist in listofLists for item in sublist]


class EncodeObjects:

    @staticmethod
    def convertToString(data):
        '''converts unicode to string'''
        if isinstance(data, basestring):
            return str(data)
        elif isinstance(data, collections.Mapping):
            return dict(map(EncodeObjects.convertToString, data.iteritems()))
        elif isinstance(data, collections.Iterable):
            return type(data)(map(EncodeObjects.convertToString, data))
        else:
            return data

    @staticmethod
    def convertToUTF8(data):
        '''converts unicode to string'''
        if isinstance(data, basestring):
            return data.encode('utf-8')
        elif isinstance(data, collections.Mapping):
            return dict(map(EncodeObjects.convertToUTF8, data.iteritems()))
        elif isinstance(data, collections.Iterable):
            return type(data)(map(EncodeObjects.convertToUTF8, data))
        else:
            return data


class ShtUtils:
    '''class for common wksht util functions'''
    @staticmethod
    def getWkbk(fn):
        wkbk = pd.ExcelFile(fn)
        return wkbk

    @staticmethod
    def get_sht_names(wkbk):
        shts =  wkbk.sheet_names
        return [ sht for sht in shts if sht != 'Dataset Summary']

class WkbkUtils:
    '''util class for dealing with excel workbooks'''

    @staticmethod
    def get_shts(fn):
      '''gets the sheets from the workbook as a dcitionary'''
      wkbk = ShtUtils.getWkbk(fn)
      sht_names = ShtUtils.get_sht_names(wkbk)
      return {'wkbk': wkbk, 'shts': sht_names}

    @staticmethod
    def getShtDf(wkbk_stuff, wkbkName, skipRows):
      '''turns a wksht into a df based on a name and the number of rows to skip'''
      dfSht = False
      df = wkbk_stuff['wkbk'].parse(wkbkName, header=skipRows )
      dfCols = list(df.columns)
      if len(dfCols) > 3:
        return df
      return dfSht


if __name__ == "__main__":
    main()
