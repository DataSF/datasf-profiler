
# coding: utf-8
#!/usr/bin/env python
from __future__ import division
from Utils import *
from PandasUtils import *
import numpy as np
import pandas as pd
import re
import signal



class ProfileFields:


  @staticmethod
  def getCurrentFieldProfiles(sQobj, base_url, fbf ):
    qry =  '''%s%s.json?$query=SELECT columnid, last_updt_dt''' % (base_url, fbf)
    dictList = PandasUtils.resultsToDictList(sQobj, qry)
    return PandasUtils.getDictListAsMappedDict('columnid', 'last_updt_dt', dictList)

  @staticmethod
  def get_field_lengths(sQobj, base_url, nbeId, fieldName, fieldType):
    minMax = {}
    isGeomField = re.findall('geometry', fieldType)
    if fieldType != 'numeric' and (not(isGeomField)) and fieldType != 'blob':
      qry = '''%s%s?$query=SELECT %s as label WHERE %s IS NOT NULL GROUP BY %s ORDER BY %s ''' % (base_url, nbeId, fieldName, fieldName, fieldName, fieldName)
      results = sQobj.getQryFull(qry)
      if results:
        df = PandasUtils.makeDfFromJson(results)
        items = list(df['label'])
        if fieldType != 'boolean':
          items = [len(str(item.encode('utf-8'))) for item in items]
        else:
          items = [len(str(item)) for item in items]
        minMax['min_field_length'] = min(items)
        minMax['max_field_length'] = max(items)
        minMax['avg_field_length']  = round(np.mean(items),2)
    return minMax

  @staticmethod
  def getBaseDatasetJson(sQobj, configItems, fbf):
    qryCols = '''columnid, datasetid, nbeid, dataset_name, field_type, api_key, last_updt_dt_data WHERE privateordeleted != true AND field_type !='blob' ORDER BY datasetid, field_type '''
    results_json = sQobj.pageThroughResultsSelect(fbf, qryCols)
    return FileUtils.write_json_object(results_json, configItems['pickle_data_dir'], configItems['mm_dd_json_fn'])

  @staticmethod
  def get_dataset_as_dfList(data_dir, json_file, base_url):
    json_obj = FileUtils.loadJsonFile(data_dir, json_file)
    df = PandasUtils.makeDfFromJson(json_obj)
    df['base_url'] = base_url
    df_list = PandasUtils.convertDfToDictrows(df)
    return df_list

  @staticmethod
  def getResults(sQobj, qry):
    '''gets results from portal'''
    results = sQobj.getQryFull(qry)
    if results:
      #if (not(type(results) is dict )):
      if (len(results) > 0) and 'value' in results[0].keys():
        try:
          return  int(results[0]['value'])
        except:
          try:
            return round(float(results[0]['value']),2)
          except:
            return results[0]['value']
      if (len(results) > 0) and 'cnt' in results[0].keys():
        return  int(results[0]['cnt'])
    return 0

  @staticmethod
  def profileField(sQobj,field, dt_format):
    field['total_count'] = ProfileFields.getTotal(sQobj, field['base_url'], field['nbeid'])
    #print "total: " + str(field['total_count'])
    field['null_count'] =  ProfileFields.getNulls(sQobj, field['base_url'], field['nbeid'], field['api_key'])
    #print "null: " + str(field['null_count'])
    field['missing_count'] = ProfileFields.getMissing(sQobj, field['base_url'], field['nbeid'], field['api_key'], field['field_type'])
    #print "missing: " + str(field['missing'])
    field['actual_count'] =ProfileFields.getActuals(sQobj, field['base_url'], field['nbeid'], field['api_key'], field['field_type'],field['missing_count'])
    #print "actual_count: " + str(field['actual_count'])
    field['cardinality'] = ProfileFields.getCardinality(sQobj, field['base_url'], field['nbeid'], field['api_key'], field['field_type'])
    #print "cardinality: " + str(field['cardinality'])
    field['completeness'] = ProfileFields.getCompleteness(field['actual_count'], field['total_count'])
    #print "completeness: " + str(field['completeness'])
    field['uniqueness'] = ProfileFields.getUniqueness(field['cardinality'], field['total_count'])
    #print "uniqueness: " + str(field['uniqueness'])
    field["distinctness"] = ProfileFields.getDistinctness(field['cardinality'], field['actual_count'])
    #print "distinctness: " + str(field['distinctness'])
    field['is_primary_key_candidate'] = ProfileFields.isPrimaryKeyCandidate(field['uniqueness'], field['completeness'])
    #print "is_primary_key_candidate: " + str(field['is_primary_key_candidate'])
    isGeomField = re.findall('geometry',  field['field_type'])
    if (not(isGeomField)) and field['field_type'] != 'boolean':
      field['min_value'] = str(ProfileFields.getMin(sQobj, field['base_url'], field['nbeid'], field['api_key'], field['field_type']))
      #print "min: " + str( field['min'])
      field['max_value'] = str(ProfileFields.getMax(sQobj, field['base_url'], field['nbeid'], field['api_key'], field['field_type']))
      #print "max: " + str(field['max'])
      field['mode'] = ProfileFields.getMode(sQobj, field['base_url'], field['nbeid'], field['api_key'])
      #print "mode: " + str( field['mode'])
      if field['field_type'] == 'timestamp':
        field['range'] = ProfileFields.getRange(field['min_value'], field['max_value'], field['field_type'], dt_format)
        #print "range: " + str(field['range'])
      else:
        field['range'] = ProfileFields.getRange(field['min_value'], field['max_value'], field['field_type'])
        #print "range: " + str(field['range'])
    if (not(isGeomField)):
      minMaxLens = ProfileFields.get_field_lengths(sQobj, field['base_url'], field['nbeid'], field['api_key'], field['field_type'])
      if len(minMaxLens.keys()) > 0:
          field.update(minMaxLens)

    if field['field_type'] == 'numeric':
      field['mean'] = ProfileFields.getMean(sQobj, field['base_url'], field['nbeid'], field['api_key'])
      #print "average: " + str(field['average'])
      field['sum'] = ProfileFields.getSum(sQobj, field['base_url'], field['nbeid'], field['api_key'])
      #print "sum: " + str(field['sum'])
      field['standard_deviation'] = ProfileFields.getStdDev(sQobj, field['base_url'], field['nbeid'], field['api_key'])
      #print "std_dev: " + str(field['std_dev'])
      field['variance'] = ProfileFields.getVariance(field['standard_deviation'])
      #print "variance: " + str(field['variance'])
      #  print "getting stats"
      more_stats = ProfileFields.get_stats(sQobj, field['base_url'], field['nbeid'], field['api_key'], field['field_type'])
      #  print more_stats
      if len(more_stats.keys()) > 0:
        field.update(more_stats)
    field['last_updt_dt'] = DateUtils.get_current_timestamp()
    return field

  @staticmethod
  def getTotal(sQobj, base_url, nbeId):
    '''returns the total number of records in dataset'''
    #'https://data.sfgov.org/resource/93gi-sfd2.json?$query=SELECT coubase_url,nt(*)
    qry = '''%s%s.json?$query=SELECT count(*) as value''' % (base_url, nbeId)
    return ProfileFields.getResults(sQobj, qry)

  @staticmethod
  def getNulls(sQobj,base_url, nbeId, fieldName ):
    '''count of the number of records with a NULL value '''
    #https://data.sfgov.org/resource/aaxw-2cb8?$query=SELECT count(*) AS value WHERE project_owner is NULL
    qry = '''%s%s?$query=SELECT count(*) AS value WHERE %s is NULL''' %( base_url, nbeId, fieldName)
    return ProfileFields.getResults(sQobj, qry)


  @staticmethod
  def getActuals(sQobj, base_url, nbeId, fieldName , field_type, missing_count):
    '''count of the number of records with an actual value (i.e., non-NULL and non-Missing)'''
    #https://data.sfgov.org/resource/e2px-wugd.json?$query=SELECT COUNT(project_status) WHERE project_status IS NOT NULL
    qry = '''%s%s.json?$query=SELECT COUNT(%s) AS value WHERE %s IS NOT NULL ''' %( base_url, nbeId, fieldName, fieldName)
    ##if(field_type) == 'text':
    # qry2 =  '''OR (%s not like '%%25  %%25' or %s != " ") ''' % (fieldName, fieldName)
    #elif (field_type) == 'numeric':
    #  qry2 = '''OR %s != 0 ''' %(fieldName)
    #print
    #print "actuals"
    #print qry+qry2
    results = ProfileFields.getResults(sQobj, qry)
    return results - missing_count

  @staticmethod
  def getMissing(sQobj, base_url, nbeId, fieldName , field_type):
    '''count of the number of records with a missing value- ie zeros and blanks'''
    #get blanks-> only applies to text columns
    #https://data.sfgov.org/resource/e2px-wugd.json?$query=SELECT planning_entitlements, count(*) as cnt WHERE planning_entitlements like "%25 %25"  GROUP By  planning_entitlements
    qry = None
    if(field_type) == 'text':
      qry = '''%s%s.json?$query=SELECT %s, count(*) as value WHERE %s like '%%25  %%25' OR %s = '%%25 %%25' or %s = " "  GROUP By  %s |> SELECT SUM(value) as value ''' %(base_url, nbeId, fieldName, fieldName,fieldName, fieldName, fieldName)
    elif (field_type) == 'numeric':
      #gets the zeros-> only applies to numeric
      #https://data.sfgov.org/resource/e2px-wugd.json?$query=SELECT count(*) as count where sro_units = 0
      qry = '''%s%s.json?$query=SELECT count(*) as value where %s = 0'''% (base_url, nbeId, fieldName)
    if qry:
      return ProfileFields.getResults(sQobj, qry)
    return 0

  @staticmethod
  def getCardinality(sQobj,base_url, nbeId, fieldName , field_type):
    '''Cardinality-> count of the number of distinct actual values'''
    #https://data.sfgov.org/resource/wwmu-gmzc.json?$query=SELECT title, count(*) as count WHERE title not like '%25 %25' and title is NOT NULL GROUP By title |> select title |> select count(title) as value
    if(field_type) == 'text':
      #https://data.sfgov.org/resource/e2px-wugd.json?$query=SELECT planning_entitlements, count(*) as count WHERE planning_entitlements not like '%25 %25' and  planning_entitlements is NOT NULL  GROUP By  planning_entitlements |> SELECT SUM(count) as count
      qry = '''%s%s.json?$query=SELECT %s, count(*) as count WHERE %s not like '%%25  %%25' and %s IS NOT NULL GROUP BY %s |> select %s |> select count(%s) as value''' % (base_url, nbeId, fieldName, fieldName, fieldName, fieldName, fieldName, fieldName)
    elif (field_type) == 'numeric':
      #https://data.sfgov.org/resource/wwmu-gmzc.json?$query=SELECT release_year, count(*) as count WHERE release_year != 0 and release_year is NOT NULL GROUP By release_year |> select release_year |> select count(release_year) as value
      qry = '''%s%s.json?$query=SELECT %s, count(*) as count WHERE %s != 0 and %s IS NOT NULL GROUP BY %s |> select %s |> select count(%s) as value''' % (base_url, nbeId, fieldName, fieldName, fieldName, fieldName, fieldName, fieldName)
    else:
      qry = '''%s%s.json?$query=SELECT %s, count(*) as count WHERE %s IS NOT NULL GROUP BY %s |> select %s |> select count(%s) as value''' % (base_url, nbeId, fieldName, fieldName, fieldName, fieldName, fieldName)
    return ProfileFields.getResults(sQobj, qry)


  @staticmethod
  def getCompleteness(actual_cnt, total):
    '''Completeness: percentage calculated as actual divided by the total number of records'''
    if total != 0:
      return round((int(actual_cnt)/int(total))*100, 2)
    return 0

  @staticmethod
  def getUniqueness(cardinality_cnt, total):
    '''Uniqueness: percentage calculated as Cardinality divided by the total number of records '''
    if total != 0:
      return round((cardinality_cnt/total) *100, 3)
    return 0

  @staticmethod
  def getDistinctness(cardinality_cnt, actual_cnt):
    '''Distinctness: percentage calculated as Cardinality divided by Actual'''
    if actual_cnt != 0:
      return round((cardinality_cnt/actual_cnt)*100, 3)
    return 0

  @staticmethod
  def isPrimaryKeyCandidate(uniquess, completeness):
    if uniquess == 100 and completeness == 100:
      return True
    return False

  @staticmethod
  def getMean(sQobj, base_url, nbeId, fieldName):
    '''gets average for numeric fields'''
    #https://data.sfgov.org/resource/e2px-wugd.json?$select=avg(project_units)
    qry = '''%s%s.json?$select=avg(%s) as value''' % (base_url, nbeId, fieldName)
    return ProfileFields.getResults(sQobj, qry)

  @staticmethod
  def getMax(sQobj, base_url, nbeId, fieldName, field_type):
    '''gets max for field- can work on text and numeric fields'''
    #https://data.sfgov.org/resource/e2px-wugd.json?$select=max(project_units) as value
    qry = '''%s%s.json?$select=max(%s) as value WHERE %s IS NOT NULL''' % (base_url, nbeId, fieldName, fieldName )
    if field_type != 'text':
      return ProfileFields.getResults(sQobj, qry)
    return ''

  @staticmethod
  def getMin(sQobj, base_url, nbeId, fieldName, field_type):
    '''gets max for field- can work on text and numeric fields'''
    #https://data.sfgov.org/resource/e2px-wugd.json?$select=mix(project_units) as value
    qry = '''%s%s.json?$select=min(%s) as value WHERE %s IS NOT NULL''' % (base_url, nbeId, fieldName, fieldName )
    if field_type != 'text':
      return ProfileFields.getResults(sQobj, qry)
    return ''

  @staticmethod
  def getSum(sQobj, base_url, nbeId, fieldName):
    qry = '''%s%s.json?$select=sum(%s) as value''' % (base_url, nbeId, fieldName)
    return ProfileFields.getResults(sQobj, qry)

  @staticmethod
  def getStdDev(sQobj, base_url, nbeId, fieldName):
    '''returns the population standard deviation of a numeric field'''
    #https://data.sfgov.org/resource/e2px-wugd.json?$select=stddev_pop(project_units) as value
    qry = '''%s%s.json?$select=stddev_pop(%s) as value''' % (base_url, nbeId, fieldName)
    return round(ProfileFields.getResults(sQobj, qry), 2)

  @staticmethod
  def getVariance(stdDev):
    '''Standard Deviation is just the square root of variance, so we can just square the std '''
    return round(stdDev*stdDev, 2)

  @staticmethod
  def getMode(sQobj, base_url, nbeId, fieldName):
    '''gets the mode of a field'''
    #https://data.sfgov.org/resource/e2px-wugd.json?$query=SELECT project_units, count(*) as value GROUP BY project_units ORDER BY count(*) desc limit 1'''
    qry = '''%s%s?$query=SELECT %s, count(*) as value WHERE %s IS NOT NULL GROUP BY %s ORDER BY count(*) desc limit 1 |> select %s as value ''' % (base_url, nbeId, fieldName, fieldName, fieldName, fieldName)
    return ProfileFields.getResults(sQobj, qry)


  @staticmethod
  def getRange(myMin, myMax, field_type, dt_format=None):
    '''returns the range of a field'''
    if field_type == 'numeric':
      return round( (float(myMax)-float(myMin)), 2)
    if field_type == 'timestamp' and dt_format:
      try:
        myMin = DateUtils.strToDtObj(myMin, dt_format)
        myMax = DateUtils.strToDtObj(myMax, dt_format)
      except:
        dt_format = '%Y-%m-%dT%H:%M:%S.000Z'
        try:
          myMin = DateUtils.strToDtObj(myMin, dt_format)
          myMax = DateUtils.strToDtObj(myMax, dt_format)
        except Exception,e :
          print str(e)
      return abs((myMax - myMin).days)
    return ''

  @staticmethod
  def pretty_name(x):
    x *= 100
    if x == int(x):
        return '%.0f%%' % x
    else:
        return '%.1f%%' % x


  @staticmethod
  def get_stats(sQobj, base_url, nbeId, fieldName, fieldType):

    def timeout_handler(signum, frame):   # Custom signal handler
      raise TimeoutException

    signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(15)
    stats = {}
    results_obj = []
    if fieldType  == 'numeric':
      try:
        #print "issuing qrys"
        qry_cols = '''%s as label WHERE %s IS NOT NULL ORDER BY %s''' % (fieldName, fieldName, fieldName)
        results_obj =  sQobj.pageThroughResultsSelect(nbeId, qry_cols)
      except Exception, e:
          print "time out! Numeric Field Stats- Qry too long to profile numeric"
          return stats
      #print results_obj
      if len(results_obj) > 0:
        signal.alarm(0)
        results = [float(result['label']) for result in results_obj]
        lst = pd.Series(results)
        for x in np.array([0.05, 0.25, 0.5, 0.75, 0.95]):
          stats[ProfileFields.pretty_name(x)] = round(lst.quantile(x),2)
          #find the middle 50% of values
        stats['iqr'] = round((stats['75%'] - stats['25%']),2)
        stats['kurtosis'] = round(lst.kurt(skipna=True),2)
        if math.isnan(stats['kurtosis']):
          del stats['kurtosis']
        stats['skewness'] = round(lst.skew(),2)
        if math.isnan(stats['skewness']):
          del stats['skewness']
        #returns mean absolute deviation of the values for the requested axis
        stats['mean_absolute_deviation'] = round(lst.mad(),2)
        stats['median'] =  round(lst.median(),2)
        results_str =  [ len(result['label']) for result in results_obj]
        stats['min_field_length'] = min(results_str)
        stats['max_field_length'] = max(results_str)
        stats['avg_field_length']  = round(np.mean(results_str),2)
        return stats
    return stats

  @staticmethod
  def buildInsertFieldProfiles(sQobj, scrud, configItems, master_dfList, current_field_profiles):
    src_records = 0
    inserted_records = 0
    dt_fmt = '%Y-%m-%dT%H:%M:%S'
    dt_fmt_fields = '%Y-%m-%dT%H:%M:%S.000'
    field_profile_fbf =  configItems['dd']['field_profiles']['fbf']
    row_id = configItems['dd']['field_profiles']['row_id']
    base_url =  configItems['baseUrl']
    profile_keys = current_field_profiles.keys()
    field_chunks = ListUtils.makeChunks(master_dfList, 5)
    dataset_info = {'Socrata Dataset Name': configItems['dataset_name'], 'SrcRecordsCnt':0, 'DatasetRecordsCnt':0, 'fourXFour': field_profile_fbf, 'row_id': row_id}
    for chunk in field_chunks:
      new_field_profiles = []
      for field in chunk:
        field_profile = {}
        if field['columnid'] in profile_keys:
          if ( not ( DateUtils.compare_two_timestamps( current_field_profiles[field['columnid']],  field['last_updt_dt_data'], dt_fmt , dt_fmt ))):
            field_profile = ProfileFields.profileField(sQobj,field, dt_fmt_fields)
            if len(field_profile.keys()) > 1 :
              new_field_profiles.append(field_profile)
        else:
          #if field['datasetid'] == 'vw6y-z8j6':
          #if field['columnid'] == '2ehv-6arf_geom':
          #if field['columnid'] == 'zfw6-95su_medium':
          print "*****"
          print field
          field_profile = ProfileFields.profileField(sQobj,field, dt_fmt_fields)
          print
          print field_profile
          print "*****"
          new_field_profiles.append(field_profile)
      if len(new_field_profiles) > 0:
        dataset_info['DatasetRecordsCnt'] = 0
        dataset_info['SrcRecordsCnt'] = len(new_field_profiles)
        dataset_info = scrud.postDataToSocrata(dataset_info, new_field_profiles)
        #print dataset_info
        src_records = src_records + dataset_info['SrcRecordsCnt']
        inserted_records = inserted_records + dataset_info['DatasetRecordsCnt']
    dataset_info['SrcRecordsCnt'] = src_records
    dataset_info['DatasetRecordsCnt'] = inserted_records
    return dataset_info




if __name__ == "__main__":
    main()

