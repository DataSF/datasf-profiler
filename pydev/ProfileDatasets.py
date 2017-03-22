

# coding: utf-8
#!/usr/bin/env python

from __future__ import division
import pandas as pd
from PandasUtils import *
from Utils import *
from DictUtils import *
from Queries import *

class ProfileDatasets:

  @staticmethod
  def getCurrentDatasetProfiles(sQobj, base_url, fbf):
    all_datasets = {}
    qry = '''%s%s.json?$query=SELECT datasetid, last_updt_dt_data ''' % (base_url, fbf)
    dictList =  PandasUtils.resultsToDictList(sQobj, qry)
    if dictList:
      for item in dictList:
        all_datasets[ item['datasetid'] ] = item['last_updt_dt_data']
    return all_datasets

  @staticmethod
  def getBaseDatasets(sQobj, base_url, fbf):
    qry =  '''%s%s.json?$query=SELECT datasetid, nbeid, last_updt_dt_data, dataset_name, count(*) as value  WHERE privateordeleted != true GROUP BY datasetid, nbeid, last_updt_dt_data, dataset_name''' % (base_url, fbf)
    df = PandasUtils.resultsToDf(sQobj, qry)
    df['base_url'] = base_url
    return PandasUtils.convertDfToDictrows(df)

  @staticmethod
  def getFieldTypes(sQobj, base_url, fieldtype_dd):
    qry =  '''%s%s.json?$query=SELECT field_type  ''' % (base_url, fieldtype_dd)
    results = sQobj.getQryFull(qry)
    df = PandasUtils.makeDfFromJson(results)
    return df['field_type'].tolist()


  @staticmethod
  def getDatasetDupes(sQobj, mmdd_fbf, dataset):
    qry =  '''%s%s.json?$query=SELECT api_key WHERE datasetid = '%s' ''' % (dataset['base_url'],mmdd_fbf, dataset['datasetid'])
    results = sQobj.getQryFull(qry)
    results = DictUtils.consolidateDictList(results, 'api_key')
    fields = ', '.join(results)
    qry2 = '''%s%s.json?$query=SELECT %s, COUNT(*) AS value GROUP BY %s HAVING COUNT(*) > 1 |>  SELECT SUM(value) AS value  ''' % (dataset['base_url'],dataset['nbeid'], fields, fields)
    return ProfileFields.getResults(sQobj, qry2)

  @staticmethod
  def percentDuplicate(dataset_stats):
    if dataset_stats['dupe_record_count'] == 0:
      return 0
    return  round((int(dataset_stats['dupe_record_count'])/int(dataset_stats['record_count'])) *100,  2)


  @staticmethod
  def getTypeCnt(sQobj, dataset, mmdd_fbf, field_types):
    dataset_stats = {'field_count': dataset['value'], 'datasetid': dataset['datasetid'], 'nbeid': dataset['nbeid'], 'dataset_name': dataset['dataset_name']}
    field = ''
    qry_num_fields = '''%s%s.json?$query=SELECT field_type, count(*)  as value WHERE privateordeleted != true AND datasetid = '%s' AND field_type = ''' % (dataset['base_url'], mmdd_fbf, dataset['datasetid'])
    for ft in field_types:
      label = ft.lower().replace('geometry: ', '')+ "_count"
      qry = qry_num_fields + "'" + ft.lower()  +"'" + ' GROUP BY field_type'
      dataset_stats[label] = ProfileFields.getResults(sQobj, qry)
    dataset_stats['record_count'] = ProfileFields.getTotal(sQobj, dataset['base_url'], dataset['nbeid'])
    dataset_stats['last_updt_dt_data'] = DateUtils.get_current_timestamp()
    return dataset_stats

  @staticmethod
  def getDatasetStats(sQobj,dataset, mmdd_fbf, field_types):
    dataset_stats = {}
    dataset_stats = ProfileDatasets.getTypeCnt(sQobj,dataset, mmdd_fbf, field_types)
    dataset_stats['dupe_record_count'] = ProfileDatasets.getDatasetDupes(sQobj,mmdd_fbf, dataset)
    dataset_stats['dupe_record_percent'] = ProfileDatasets.percentDuplicate(dataset_stats)
    return  dataset_stats

#season of the witch
  @staticmethod
  def buildInsertDatasetProfiles(sQobj, scrud, configItems, datasets, ds_profiles, field_types):

    mmdd_fbf = configItems['dd']['master_dd']['fbf']
    ds_profiles_fbf =  configItems['dd']['ds_profiles']['fbf']
    base_url =  configItems['baseUrl']
    ds_profile_keys = ds_profiles.keys()
    dataset_info = {}
    for dataset in datasets[10:11]:
      dataset_stats = {}
      if dataset['datasetid'] in ds_profile_keys:
        dt_fmt = '%Y-%m-%dT%H:%M:%S'
        if ( not ( DateUtils.compare_two_timestamps( ds_profiles[dataset['datasetid']],  dataset['last_updt_dt_data'], dt_fmt , dt_fmt ))):
          dataset_stats = getDatasetStats(sQobj,dataset, mmdd_fbf, field_types)
      else:
        dataset_stats = ProfileDatasets.getDatasetStats(sQobj,dataset, mmdd_fbf, field_types)
      if len(dataset_stats.keys()) > 1 :
        dataset_info = {'Socrata Dataset Name': configItems['dataset_name'], 'SrcRecordsCnt':len([dataset_stats]), 'DatasetRecordsCnt':0, 'fourXFour': ds_profiles_fbf, 'row_id': configItems['row_id']}
        dataset_info = scrud.postDataToSocrata(dataset_info, [dataset_stats])
        print dataset_info
    return dataset_info
