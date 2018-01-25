

# coding: utf-8
#!/usr/bin/env python

from __future__ import division
import pandas as pd
from PandasUtils import *
from Utils import *
from DictUtils import *
from ProfileFields import *
from DictUtils import *
import requests
import time
from datetime import date,timedelta
from ConfigUtils import *


class ProfileDatasets:


  @staticmethod
  def getAssetInventoryInfo(sQobj, base_url, fbf):
    '''returns a nested dict obj of all the datasetids as keys from the asset inventory; values dicts of pertinent asset inventory info'''
    qryCols = '''u_id as datasetid, category, downloads, publishing_frequency, data_change_frequency, visits, description, data_notes as notes, name as dataset_name, keywords WHERE public = 'true' '''
    results =  sQobj.pageThroughResultsSelect(fbf, qryCols)
    #qry = '''%s%s.json?$query=SELECT u_id as datasetid, category, downloads, publishing_frequency, data_change_frequency, visits ''' % (base_url, fbf)
    #results = sQobj.getQryFull(qry)
    assets_inventory_dict = {}
    for result in results:
      datasetid = result['datasetid']
      del result['datasetid']
      assets_inventory_dict[datasetid] = result
    return assets_inventory_dict


  @staticmethod
  def getViewsLastUpdatedAt( datesetid,last_updt_dp, clientItems):
    privateordeleted = False
    columns = []
    qry = '''https://data.sfgov.org/api/views/%s.json''' %(datesetid)
    try: 
      r = requests.get( qry )
      view_info =  r.json()
    except Exception, e:
      print "ERROR: something went wrong with %s" %(datasetid)
      print str(e)
      return {}
    #print view_info
    if 'code' in view_info.keys() and 'message' in view_info.keys():
      print "*** error message "
      if view_info['code'] == 'authentication_required':
        privateordeleted = True
        print "****get view info private***"
        r = requests.get( qry, auth=(clientItems['username'], base64.b64decode(clientItems['password'])))
        print "*******"
        print r.json()
        print
        view_info = r.json()
        print view_info
        print "*** private or deleted**"
      elif view_info['code'] == 'not_found':
        print "*** Dataset not found ****"
        return {}
    dataset_name =  view_info['name']
    if('geo' in view_info['metadata']):
      last_updt_views = ProfileDatasets.getMostRecentGeoUpdateDate(view_info)
      try:
        last_updt_views =  datetime.datetime.strptime(last_updt_views, "%Y-%m-%dT%H:%M:%S")
        columns = ProfileDatasets.getInfoFromGeoView( view_info, 'columns')
      except Exception, e:
        print str(e)
        print "*** ERROR***** "
        print dataset_name
        print qry
        print "************"
    else:
      last_updt_views = view_info['rowsUpdatedAt']
      last_updt_views = datetime.datetime.utcfromtimestamp(last_updt_views)
      columns = view_info['columns']
    column_names = [ datesetid + "_" + col['fieldName'] for col in columns ]
    last_updt_dp =  datetime.datetime.strptime(last_updt_dp, "%Y-%m-%dT%H:%M:%S")
    #last_updt_dp_plus = last_updt_dp + datetime.timedelta(hours=0)
    if privateordeleted:
      print "**** deleted dataset*****"
      print dataset_name
      return {'dataset_name': dataset_name, 'cols': [ {'columnid':col, 'privateordeleted': True} for col in column_names]}
      print
    if last_updt_views > last_updt_dp:
      print "***timestamps**:" +dataset_name  + " " + datesetid
      print "***last_uptdt"
      print last_updt_dp
      print
      print "**views"
      print last_updt_views
      print "*****"
      last_updt_views = last_updt_views.strftime('%Y-%m-%dT%H:%M:%S')
      return {'dataset_name': dataset_name, 'cols': [ {'columnid':col, 'last_updt_dt_data': last_updt_views} for col in column_names]}
    return {}



  @staticmethod
  def getCurrentDatasetProfiles(sQobj, base_url, fbf, clientItems=None, daily=False):
    '''gets dict of the the datasetid and the dt the dataset was updated; used for lookup purposes'''
    qry = '''%s%s.json?$query=SELECT datasetid,  last_updt_dt_data  ''' % (base_url, fbf)
    if daily:
      qry = '''%s%s.json?$query=SELECT datasetid, last_updt_dt_data WHERE (publishing_frequency = 'Daily' OR publishing_frequency = 'Streaming')  ''' % (base_url, fbf)
    dictList = PandasUtils.resultsToDictList(sQobj, qry)
    return PandasUtils.getDictListAsMappedDict('datasetid', 'last_updt_dt_data', dictList)

  @staticmethod
  def getTriggerToRefreshFields(sQobj, base_url, fbf, datasetid, colKey):
    '''gets dict of the the datasetid and the dt the dataset was updated; used for lookup purposes'''
    qry = '''%s%s.json?$query=SELECT %s where datasetid = '%s' ''' % (base_url, fbf, colKey, datasetid)
    dictList = PandasUtils.resultsToDictList(sQobj, qry)
    if(dictList is None):
      return ""
    else:
      itemTest = dictList[0]
      if colKey in itemTest.keys():
        return itemTest[colKey]
    return ""

  @staticmethod
  def joinAuxDataSetInfo(dataset, asset_inventory_dict):
    '''joins info from the asset inventory to the dataset dict'''
    datasetid = dataset['datasetid']
    if datasetid in asset_inventory_dict.keys():
      return asset_inventory_dict[datasetid]
    return {}

  @staticmethod
  def getBaseDatasets(sQobj, base_url, fbf):
    '''returns a list of datasets from the master data dictionary dataset'''
    #qry =  '''%s%s.json?$query=SELECT datasetid, nbeid, last_updt_dt_data, dataset_name, department, created_date,  count(*) as value  WHERE privateordeleted != true and nbeid IS NOT NULL GROUP BY datasetid, nbeid, last_updt_dt_data, dataset_name, department, created_date ''' % (base_url, fbf)
    qry =  '''%s%s.json?$query=SELECT datasetid, nbeid, last_updt_dt_data, department, created_date, dataset_name,  count(*) as value  WHERE privateordeleted != true and nbeid IS NOT NULL GROUP BY datasetid, nbeid, last_updt_dt_data, department, created_date, dataset_name ''' % (base_url, fbf)
    df = PandasUtils.resultsToDf(sQobj, qry)
    df['base_url'] = base_url
    df =  PandasUtils.fillNaWithBlank(df)
    return PandasUtils.convertDfToDictrows(df)

  @staticmethod
  def getFieldTypes(sQobj, base_url, fieldtype_dd):
    '''returns the various field types from the Field Types Dataset'''
    qry =  '''%s%s.json?$query=SELECT field_type  ''' % (base_url, fieldtype_dd)
    results = sQobj.getQryFull(qry)
    df = PandasUtils.makeDfFromJson(results)
    return df['field_type'].tolist()

  @staticmethod
  def getRowIdentifier(json):
    if "rowIdentifier" in   json['metadata'].keys():
      rowIdentifier = json['metadata']['rowIdentifier']
      if rowIdentifier != '0':
        cols = json['columns']
        rowIdentifierName = [col['name'] for col in cols if col['id'] == rowIdentifier]
        if len(rowIdentifierName ) > 0:
          return rowIdentifierName[0]
        else:
          print "****ERROR: something went wrong with parsing the row_id"
          print rowIdentifier
          print "*******"
          return None
    return None

  @staticmethod
  def getInfoFromGeoView(json, key):
    if 'layers' in json['metadata']['geo']:
      geoFbf =  json['metadata']['geo']['layers']
      geoFbf = geoFbf.split(",")
      #print geoFbf
      qry = '''https://data.sfgov.org/api/views/%s.json''' %(geoFbf[0])
      r = requests.get( qry )
      try:
        view_info =  r.json()
        if key in view_info.keys():
          return view_info[key]
      except Exception, e:
        print "ERROR: Could not decode json**"
        print str(e)
    return None

  @staticmethod
  def getMostRecentGeoUpdateDate(json):
    if 'layers' in json['metadata']['geo']:
      rowsUpdatedAtUTC = ProfileDatasets.getInfoFromGeoView(json, 'rowsUpdatedAt')
    if(rowsUpdatedAtUTC is None):
      rowsUpdatedAtUTC = ProfileDatasets.getInfoFromGeoView(json, 'viewLastModified')
    if rowsUpdatedAtUTC:
      rowsUpdatedAt = datetime.datetime.utcfromtimestamp(rowsUpdatedAtUTC)
      rowsUpdatedAt = rowsUpdatedAt.strftime('%Y-%m-%dT%H:%M:%S')
      return rowsUpdatedAt
    return ''



  @staticmethod
  def getLastUpdatedFromViews(json):
    rowsUpdatedAt = ''
    if('geo' in json['metadata']):
      print "***** this is a geo dataset******"
      return ProfileDatasets.getMostRecentGeoUpdateDate(json)
    else:
      if ('rowsUpdatedAt' in json.keys()):
        rowsUpdatedAt = datetime.datetime.utcfromtimestamp(json['rowsUpdatedAt'])
        rowsUpdatedAt = rowsUpdatedAt.strftime('%Y-%m-%dT%H:%M:%S')
    return rowsUpdatedAt

  @staticmethod
  def getRowLabel(json):
    rowLabel = "Row"
    try:
      if "rowLabel" in  json['metadata'].keys():
        if json['metadata']['rowLabel'] != '':
          rowLabel = json['metadata']['rowLabel']
    except Exception, e:
      print
      print str(e)
      print
      print json
    return rowLabel

  @staticmethod
  def getRowInfo(fbf):
    qry = '''https://data.sfgov.org/api/views/%s.json''' % (fbf)
    #print qry
    r = requests.get(qry)
    json = r.json()
    rowLabel = ProfileDatasets.getRowLabel(json)
    rowIdentifier = ProfileDatasets.getRowIdentifier(json)
    rowsUpdatedAt = ProfileDatasets.getLastUpdatedFromViews(json)
    return { 'rowLabel': rowLabel, 'rowIdentifier': rowIdentifier, 'last_updt_dt_data': rowsUpdatedAt}



  @staticmethod
  def getDatasetDupes(sQobj, mmdd_fbf, dataset):
    '''finds the number of ROW level duplicates in the dataset'''
    qry =  '''%s%s.json?$query=SELECT api_key WHERE datasetid = '%s' and field_type != 'blob' ''' % (dataset['base_url'],mmdd_fbf, dataset['datasetid'])
    results = sQobj.getQryFull(qry)
    results = DictUtils.consolidateDictList(results, 'api_key')
    reserve_word_keys = ['by', 'having', 'group', 'select']
    #reserve_word_mapping = {'by': 'by as bby'}
    fields = [field for field in results if field not in reserve_word_keys]
    #fields_weird =  [reserve_word_mapping[field] for field in results if field in reserve_word_keys ]
    fields = ', '.join(fields)
    if 'value' in results:
      qry2 = '''%s%s.json?$query=SELECT %s, COUNT(*) AS cnt GROUP BY %s HAVING COUNT(*) > 1 |>  SELECT SUM(cnt) AS cnt  ''' % (dataset['base_url'],dataset['nbeid'], fields, fields)
    else:
      qry2 = '''%s%s.json?$query=SELECT %s, COUNT(*) AS value GROUP BY %s HAVING COUNT(*) > 1 |>  SELECT SUM(value) AS value  ''' % (dataset['base_url'],dataset['nbeid'], fields, fields)
    return ProfileFields.getResults(sQobj, qry2)

  @staticmethod
  def percentDuplicate(dataset_stats):
    '''returns the percentage of the dataset that is dupes'''
    if dataset_stats['dupe_record_count'] == 0:
      return 0
    return  round((int(dataset_stats['dupe_record_count'])/int(dataset_stats['record_count'])) *100,  2)


  @staticmethod
  def getTypeCnt(sQobj, dataset, mmdd_fbf, field_types):
    '''returns the a count of fields by type for a dataset'''
    #dataset_stats = {'field_count': dataset['value'], 'datasetid': dataset['datasetid'], 'nbeid': dataset['nbeid'], 'dataset_name': dataset['dataset_name'] 'last_updt_dt_data': }
    #copy the data over from the master dd to keep things si
    dataset_stats = dataset
    dataset_stats['field_count'] = dataset_stats['value']
    del dataset_stats['value']
    field = ''
    qry_num_fields = '''%s%s.json?$query=SELECT field_type, count(*)  as value WHERE privateordeleted != true AND datasetid = '%s' AND field_type = ''' % (dataset['base_url'], mmdd_fbf, dataset['datasetid'])
    for ft in field_types:
      label = ft.lower().replace('geometry: ', '')+ "_count"
      qry = qry_num_fields + "'" + ft.lower()  +"'" + ' GROUP BY field_type'
      dataset_stats[label] = ProfileFields.getResults(sQobj, qry)
    dataset_stats['record_count'] = ProfileFields.getTotal(sQobj, dataset['base_url'], dataset['nbeid'])
    return dataset_stats

  @staticmethod
  def getGlobalFieldCnt(sQobj, dataset, mmdd_fbf):
    '''returns the number of global fields in a dataset'''
    qry =  '''%s%s.json?$query=SELECT count(*) as value WHERE privateordeleted != true AND datasetid = '%s' AND global_field = true ''' % (dataset['base_url'], mmdd_fbf, dataset['datasetid'])
    return ProfileFields.getResults(sQobj, qry)

  @staticmethod
  def getDocumentedFieldCnt(sQobj, dataset, mmdd_fbf):
    '''returns the number of fields  that documented in a dataset'''
    qry =  '''%s%s.json?$query=SELECT count(*) as value WHERE privateordeleted != true AND datasetid = '%s' AND (field_definition IS NOT NULL OR global_field_definition IS NOT NULL) ''' % (dataset['base_url'], mmdd_fbf, dataset['datasetid'])
    return ProfileFields.getResults(sQobj, qry)

  @staticmethod
  def getCntAsPercent(total, cnt):
    if total != 0:
      return round((int(cnt)/int(total))*100, 2)
    return 0

  @staticmethod
  def getNumberOfDaysSinceSomeEvent(event_date, dt_fmt):
    today = DateUtils.get_current_timestamp()
    today_dt_fmt = '%Y-%m-%dT%H:%M:%S.%fZ'
    return DateUtils.days_between(today, today_dt_fmt, event_date, dt_fmt)

  @staticmethod
  def getDatasetStats(sQobj, dataset, mmdd_fbf, field_types, asset_inventory_dict):
    dataset_stats = {}
    dataset_stats = ProfileDatasets.getTypeCnt(sQobj,dataset, mmdd_fbf, field_types)
    dataset_stats['dupe_record_count'] = ProfileDatasets.getDatasetDupes(sQobj,mmdd_fbf, dataset)
    dataset_stats['dupe_record_percent'] = ProfileDatasets.percentDuplicate(dataset_stats)
    dt_fmt = '%Y-%m-%dT%H:%M:%S'
    auxInfo = ProfileDatasets.joinAuxDataSetInfo(dataset_stats, asset_inventory_dict)
    if len(auxInfo.keys()) > 0:
      dataset_stats = DictUtils.merge_two_dicts(dataset_stats, auxInfo)
    else:
      dataset_stats = DictUtils.merge_two_dicts(dataset, dataset_stats)
    dataset_stats['global_field_count'] =  ProfileDatasets.getGlobalFieldCnt(sQobj, dataset, mmdd_fbf)
    dataset_stats['global_field_percentage'] = ProfileDatasets.getCntAsPercent(dataset_stats['field_count'] , dataset_stats['global_field_count'])
    dataset_stats['documented_count'] = ProfileDatasets.getDocumentedFieldCnt(sQobj, dataset, mmdd_fbf)
    dataset_stats['documented_percentage'] = ProfileDatasets.getCntAsPercent(dataset_stats['field_count'], dataset_stats['documented_count'])
    rowInfo = ProfileDatasets.getRowInfo(dataset['datasetid'])
    dataset_stats = DictUtils.merge_two_dicts(dataset_stats,rowInfo)
    if(rowInfo['last_updt_dt_data'] != ''):
      dataset_stats['last_updt_dt_data'] =  rowInfo['last_updt_dt_data']
    dataset_stats['days_since_last_updated'] = ProfileDatasets.getNumberOfDaysSinceSomeEvent(dataset['last_updt_dt_data'], dt_fmt)
    dataset_stats['days_since_first_created'] = ProfileDatasets.getNumberOfDaysSinceSomeEvent(dataset['created_date'], dt_fmt)
    dataset_stats = DictUtils.filterDictOnBlanks(dataset_stats)
    dataset_stats = DictUtils.filterDictOnNans(dataset_stats)
    dataset_stats['publishing_health'] =  ProfileDatasets.calculatePublishingHealth(dataset_stats)
    dataset_stats['profile_last_updt_dt'] = DateUtils.get_current_timestamp()
    if('description' not in dataset_stats.keys() ):
      dataset_stats['description'] = "Description not availible."
    return  dataset_stats

  @staticmethod
  def updt_dtStamp_from_events(sQobj, dataset):
    dataset_stats = {'datasetid': dataset['datasetid']}
    dt_fmt = '%Y-%m-%dT%H:%M:%S'
    dataset_stats['days_since_last_updated'] = ProfileDatasets.getNumberOfDaysSinceSomeEvent(dataset['last_updt_dt_data'], dt_fmt)
    dataset_stats['days_since_first_created'] = ProfileDatasets.getNumberOfDaysSinceSomeEvent(dataset['created_date'], dt_fmt)
    rowInfo = ProfileDatasets.getRowInfo(dataset['datasetid'])
    dataset_stats = DictUtils.merge_two_dicts(dataset_stats,rowInfo)
    return  dataset_stats

  @staticmethod
  def calculatePublishingHealth(dataset_profile):
    healthThresholds = {
    'Streaming': [2,4],
    'Daily' : [2,4],
    'Weekly': [7,21],
    'Monthly': [32, 90],
    'Bi-annually': [60, 180],
    'Annually': [365,500],
    'Quarterly': [90, 270]
    }
    if 'publishing_frequency' not in dataset_profile.keys():
      return 'Unknown'
    pubFreq =  dataset_profile['publishing_frequency']
    dayslastUpdt = dataset_profile['days_since_last_updated']
    if pubFreq in healthThresholds.keys():
      timeIntervals = healthThresholds[pubFreq]
      if int(dayslastUpdt <= timeIntervals[0]):
        return 'On Time'
      elif ((int(dayslastUpdt) > timeIntervals[0]) and (int(dayslastUpdt) <= timeIntervals[1])):
        return 'Delayed'
      elif (int(dayslastUpdt) > timeIntervals[1]):
        return 'Stale'
    return 'On Time'

  @staticmethod
  def removeDiff(scrud, fbfToDelete, first, second):
    def diff(first, second):
        second = set(second)
        return [item for item in first if item not in second]
    diffList =  diff(first, second)
    print "****delete these datasets: "
    print diffList
    print
    if len(diffList) >0:
      print "items to delete: "
      print diffList
      for item in diffList:
        try:
          print "***deleting**: " + item
          print scrud.deleteRow(fbfToDelete, item)
          print "*****"
        except Exception, e:
          print str(e)
          print "ERROR: **Could not delete row***"
    else:
      print "**No items to delete***"
    return True


  @staticmethod
  def removeDeletedDatasets(scrud,  ds_profiles_fbf, asset_inventory_dict, ds_profiles, datasets):
    '''removes datasets from dataset profiles that have been deleted '''
    dataset_keys = [item['datasetid'] for item in datasets]
    asset_inventory_keys = asset_inventory_dict.keys()
    ds_profiles_keys = ds_profiles.keys()
    print "***deleting from asset inventory***"
    ProfileDatasets.removeDiff(scrud, ds_profiles_fbf, ds_profiles_keys, asset_inventory_keys)
    print "***deleting becasue not in master dd***"
    ProfileDatasets.removeDiff(scrud, ds_profiles_fbf, ds_profiles_keys, dataset_keys)

  @staticmethod
  def buildInsertDatasetProfiles(sQobj, scrud, configItems, datasets, ds_profiles, field_types, asset_inventory_dict):
    src_records = 0
    inserted_records = 0
    dt_fmt = '%Y-%m-%dT%H:%M:%S'
    mmdd_fbf = configItems['dd']['master_dd']['fbf']
    ds_profiles_fbf =  configItems['dd']['ds_profiles']['fbf']
    row_id = configItems['dd']['ds_profiles']['row_id']
    base_url =  configItems['baseUrl']
    ds_profile_keys = ds_profiles.keys()
    datasets_chunks = ListUtils.makeChunks(datasets, 5)
    dataset_info = {'Socrata Dataset Name': configItems['dataset_name'], 'SrcRecordsCnt':0, 'DatasetRecordsCnt':0, 'fourXFour': ds_profiles_fbf, 'row_id': row_id}
    for chunk in datasets_chunks:
      datasets_stats = []
      for dataset in chunk:
        #dataset_stats = {}
        if dataset['datasetid'] in ds_profile_keys:
            #print DateUtils.compare_two_timestamps( ds_profiles[dataset['datasetid']],  dataset['last_updt_dt_data'], dt_fmt , dt_fmt, offset_t1=0, offset_t2=0 )
            if ( DateUtils.compare_two_timestamps( ds_profiles[dataset['datasetid']],  dataset['last_updt_dt_data'], dt_fmt , dt_fmt, offset_t1=0, offset_t2=0 )):
              print "more recent; updating: " + dataset['dataset_name']+ "- " + dataset['datasetid']
              dataset_stats = ProfileDatasets.getDatasetStats(sQobj,dataset, mmdd_fbf, field_types, asset_inventory_dict)
              datasets_stats.append(dataset_stats)
        else:
          dataset_stats = ProfileDatasets.getDatasetStats(sQobj, dataset, mmdd_fbf, field_types, asset_inventory_dict)
          print
          print "new dataset!*****"
          print dataset_stats
          print
          datasets_stats.append(dataset_stats)
          #print datasets_stats
          #print
      if len(datasets_stats) > 0:
        dataset_info['DatasetRecordsCnt'] = 0
        dataset_info['SrcRecordsCnt'] = len(datasets_stats)
        try:
          dataset_info = scrud.postDataToSocrata(dataset_info, datasets_stats)
          src_records = src_records + dataset_info['SrcRecordsCnt']
          inserted_records = inserted_records + dataset_info['DatasetRecordsCnt']
        except Exception, e:
          print str(e)
    dataset_info['SrcRecordsCnt'] = src_records
    dataset_info['DatasetRecordsCnt'] = inserted_records
    return dataset_info



if __name__ == "__main__":
    main()
