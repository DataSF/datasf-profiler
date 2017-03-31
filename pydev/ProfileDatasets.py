

# coding: utf-8
#!/usr/bin/env python

from __future__ import division
import pandas as pd
from PandasUtils import *
from Utils import *
from DictUtils import *
from ProfileFields import *
from DictUtils import *

class ProfileDatasets:

  @staticmethod
  def getAssetInventoryInfo(sQobj, base_url, fbf):
    '''returns a nested dict obj of all the datasetids as keys from the asset inventory; values dicts of pertinent asset inventory info'''
    qryCols = '''u_id as datasetid, category, downloads, publishing_frequency, data_change_frequency, visits'''
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
  def getCurrentDatasetProfiles(sQobj, base_url, fbf):
    '''gets dict of the the datasetid and the dt the dataset was updated; used for lookup purposes'''
    qry = '''%s%s.json?$query=SELECT datasetid,  profile_last_updt_dt ''' % (base_url, fbf)
    dictList = PandasUtils.resultsToDictList(sQobj, qry)
    return PandasUtils.getDictListAsMappedDict('datasetid', 'profile_last_updt_dt', dictList)

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
    qry =  '''%s%s.json?$query=SELECT datasetid, nbeid, last_updt_dt_data, dataset_name, department, created_date,  count(*) as value  WHERE privateordeleted != true and nbeid IS NOT NULL GROUP BY datasetid, nbeid, last_updt_dt_data, dataset_name, department, created_date ''' % (base_url, fbf)
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
    dataset_stats['profile_last_updt_dt'] = DateUtils.get_current_timestamp()
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
    print
    print dataset
    print
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
    dataset_stats['days_since_last_updated'] = ProfileDatasets.getNumberOfDaysSinceSomeEvent(dataset['last_updt_dt_data'], dt_fmt)
    dataset_stats['days_since_first_created'] = ProfileDatasets.getNumberOfDaysSinceSomeEvent(dataset['created_date'], dt_fmt)
    dataset_stats = DictUtils.filterDictOnBlanks(dataset_stats)
    dataset_stats = DictUtils.filterDictOnNans(dataset_stats)
    return  dataset_stats

  @staticmethod
  def updt_dtStamp_from_events(sQobj, dataset):
    dataset_stats = {'datasetid': dataset['datasetid']}
    dt_fmt = '%Y-%m-%dT%H:%M:%S'
    dataset_stats['days_since_last_updated'] = ProfileDatasets.getNumberOfDaysSinceSomeEvent(dataset['last_updt_dt_data'], dt_fmt)
    dataset_stats['days_since_first_created'] = ProfileDatasets.getNumberOfDaysSinceSomeEvent(dataset['created_date'], dt_fmt)
    return  dataset_stats


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
        dataset_stats = {}
        if dataset['datasetid'] in ds_profile_keys:
          if dataset['datasetid'] == 'sqj6-g4dr':
          #if ( not ( DateUtils.compare_two_timestamps( ds_profiles[dataset['datasetid']],  dataset['last_updt_dt_data'], dt_fmt , dt_fmt ))):
            dataset_stats = ProfileDatasets.getDatasetStats(sQobj,dataset, mmdd_fbf, field_types, asset_inventory_dict)
            #print "updated_dateset"
            #print
            datasets_stats.append(dataset_stats)
          #else:
          #  #print "just updating timestamp"
          #  dataset_stats = ProfileDatasets.updt_dtStamp_from_events(sQobj, dataset)
          #  datasets_stats.append(dataset_stats)
        else:
          dataset_stats = ProfileDatasets.getDatasetStats(sQobj, dataset, mmdd_fbf, field_types, asset_inventory_dict)
          print "new dataset"
          datasets_stats.append(dataset_stats)
          print datasets_stats
          print
      if len(datasets_stats) > 0:
        dataset_info['DatasetRecordsCnt'] = 0
        dataset_info['SrcRecordsCnt'] = len(datasets_stats)
        dataset_info = scrud.postDataToSocrata(dataset_info, datasets_stats)
        src_records = src_records + dataset_info['SrcRecordsCnt']
        inserted_records = inserted_records + dataset_info['DatasetRecordsCnt']
    dataset_info['SrcRecordsCnt'] = src_records
    dataset_info['DatasetRecordsCnt'] = inserted_records
    return dataset_info
