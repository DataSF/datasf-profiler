
# coding: utf-8
#!/usr/bin/env python
from __future__ import division
from Utils import *
from PandasUtils import *
import numpy as np
import pandas as pd

class ProfileFields:

  @staticmethod
  def getCurrentFieldProfiles(sQobj, base_url, fbf ):
    qry =  '''%s%s.json?$query=SELECT columnid, last_updt_dt  WHERE privateordeleted != true ''' % (base_url, fbf)
    dictList = PandasUtils.resultsToDictList(sQobj, qry)
    return PandasUtils.getDictListAsMappedDict('columnid', 'last_updt_dt', dictList)

  @staticmethod
  def getBaseDatasetJson(sQobj, configItems, fbf):
    qryCols = '''columnid, datasetid, nbeid, dataset_name, field_type, api_key, last_updt_dt_data WHERE privateordeleted != true  '''
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
      print results
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
  def profileField(sQobj,field, dt_format=None):
    field['total'] = ProfileFields.getTotal(sQobj, field['base_url'], field['nbeid'])
    print "total: " + str(field['total'])
    field['null'] =  ProfileFields.getNulls(sQobj, field['base_url'], field['nbeid'], field['api_key'])
    print "null: " + str(field['null'])
    field['actual'] =ProfileFields.getActuals(sQobj, field['base_url'], field['nbeid'], field['api_key'], field['field_type'])
    print "actual: " + str(field['actual'])
    field['missing'] = ProfileFields.getMissing(sQobj, field['base_url'], field['nbeid'], field['api_key'], field['field_type'])
    print "missing: " + str(field['missing'])
    field['cardinality'] = ProfileFields.getCardinality(sQobj, field['base_url'], field['nbeid'], field['api_key'], field['field_type'])
    print "cardinality: " + str(field['cardinality'])
    field['completeness'] = ProfileFields.getCompleteness(field['actual'], field['total'])
    print "completeness: " + str(field['completeness'])
    field['uniqueness'] = ProfileFields.getUniqueness(field['cardinality'], field['total'])
    print "uniqueness: " + str(field['uniqueness'])
    field["distinctness"] = ProfileFields.getDistinctness(field['cardinality'], field['actual'])
    print "distinctness: " + str(field['distinctness'])
    field['is_primary_key_candidate'] = ProfileFields.isPrimaryKeyCandidate(field['uniqueness'], field['completeness'])
    print "is_primary_key_candidate: " + str(field['is_primary_key_candidate'])

    field['max'] = ProfileFields.getMax(sQobj, field['base_url'], field['nbeid'], field['api_key'])
    print "max: " + str(field['max'])
    field['min'] = ProfileFields.getMin(sQobj, field['base_url'], field['nbeid'], field['api_key'])
    print "min: " + str( field['min'])
    field['mode'] = ProfileFields.getMode(sQobj, field['base_url'], field['nbeid'], field['api_key'])
    print "mode: " + str( field['mode'])

    if field['field_type'] == 'timestamp':
      field['range'] = ProfileFields.getRange(field['min'], field['max'], field['field_type'], dt_format)
      print "range: " + str(field['range'])
    else:
      field['range'] = ProfileFields.getRange(field['min'], field['max'], field['field_type'])
      print "range: " + str(field['range'])
    if field['field_type'] == 'numeric':
      field['average'] = ProfileFields.getAvg(sQobj, field['base_url'], field['nbeid'], field['api_key'])
      print "average: " + str(field['average'])
      field['sum'] = ProfileFields.getSum(sQobj, field['base_url'], field['nbeid'], field['api_key'])
      print "sum: " + str(field['sum'])
      field['std_dev'] = ProfileFields.getStdDev(sQobj, field['base_url'], field['nbeid'], field['api_key'])
      print "std_dev: " + str(field['std_dev'])
      field['variance'] = ProfileFields.getVariance(field['std_dev'])
      print "variance: " + str(field['variance'])
      lst = ProfileFields.get_stats(sQobj, field['base_url'], field['nbeid'], field['api_key'])



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
  def getActuals(sQobj, base_url, nbeId, fieldName , field_type):
    '''count of the number of records with an actual value (i.e., non-NULL and non-Missing)'''
    #https://data.sfgov.org/resource/e2px-wugd.json?$query=SELECT COUNT(project_status) WHERE project_status IS NOT NULL
    qry = '''%s%s.json?$query=SELECT COUNT(%s) AS value WHERE %s IS NOT NULL ''' %( base_url, nbeId, fieldName, fieldName)
    qry2 = ''
    if(field_type) == 'text':
      qry2 =  '''AND %s not like '%%25  %%25' ''' % (fieldName)
    elif (field_type) == 'numeric':
      qry2 = '''AND %s != 0 ''' %(fieldName)
    return ProfileFields.getResults(sQobj, qry+qry2)

  @staticmethod
  def getMissing(sQobj, base_url, nbeId, fieldName , field_type):
    '''count of the number of records with a missing value- ie zeros and blanks'''
    #get blanks-> only applies to text columns
    #https://data.sfgov.org/resource/e2px-wugd.json?$query=SELECT planning_entitlements, count(*) as cnt WHERE planning_entitlements like "%25 %25"  GROUP By  planning_entitlements
    qry = None
    if(field_type) == 'text':
      qry = '''%s%s.json?$query=SELECT %s, count(*) as value WHERE %s like '%%25  %%25' OR %s = '%%25 %%25' GROUP By  %s |> SELECT SUM(value) as value ''' %(base_url, nbeId, fieldName, fieldName,fieldName, fieldName)
    elif (field_type) == 'numeric':
      #gets the zeros-> only applies to numeric
      #https://data.sfgov.org/resource/e2px-wugd.json?$query=SELECT count(*) as count where sro_units = 0
      qry = '''%s%s.json?$query=SELECT count(*) as value  where %s = 0'''% (base_url, nbeId, fieldName)
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
    return round(int(actual_cnt)/int(total), 2)*100

  @staticmethod
  def getUniqueness(cardinality_cnt, total):
    '''Uniqueness: percentage calculated as Cardinality divided by the total number of records '''
    return round((cardinality_cnt/total) *100, 2)

  @staticmethod
  def getDistinctness(cardinality_cnt, actual_cnt):
    '''Distinctness: percentage calculated as Cardinality divided by Actual'''
    return round((cardinality_cnt/actual_cnt)*100, 2)

  @staticmethod
  def isPrimaryKeyCandidate(uniquess, completeness):
    if uniquess == 100 and completeness == 100:
      return True
    return False

  @staticmethod
  def getAvg(sQobj, base_url, nbeId, fieldName):
    '''gets average for numeric fields'''
    #https://data.sfgov.org/resource/e2px-wugd.json?$select=avg(project_units)
    qry = '''%s%s.json?$select=avg(%s) as value''' % (base_url, nbeId, fieldName)
    return ProfileFields.getResults(sQobj, qry)


  @staticmethod
  def getMax(sQobj, base_url, nbeId, fieldName):
    '''gets max for field- can work on text and numeric fields'''
    #https://data.sfgov.org/resource/e2px-wugd.json?$select=max(project_units) as value
    qry = '''%s%s.json?$select=max(%s) as value WHERE %s IS NOT NULL''' % (base_url, nbeId, fieldName, fieldName )
    return ProfileFields.getResults(sQobj, qry)

  @staticmethod
  def getMin(sQobj, base_url, nbeId, fieldName):
    '''gets max for field- can work on text and numeric fields'''
    #https://data.sfgov.org/resource/e2px-wugd.json?$select=mix(project_units) as value
    qry = '''%s%s.json?$select=min(%s) as value WHERE %s IS NOT NULL''' % (base_url, nbeId, fieldName, fieldName)
    return ProfileFields.getResults(sQobj, qry)

  @staticmethod
  def getSum(sQobj, base_url, nbeId, fieldName):
    qry = '''%s%s.json?$select=sum(%s) as value''' % (base_url, nbeId, fieldName)
    return ProfileFields.getResults(sQobj, qry)

  @staticmethod
  def getStdDev(sQobj, base_url, nbeId, fieldName):
    '''returns the population standard deviation of a numeric field'''
    #https://data.sfgov.org/resource/e2px-wugd.json?$select=stddev_pop(project_units) as value
    qry = '''%s%s.json?$select=stddev_pop(%s) as value''' % (base_url, nbeId, fieldName)
    return ProfileFields.getResults(sQobj, qry)

  @staticmethod
  def getVariance(stdDev):
    '''Standard Deviation is just the square root of variance, so we can just square the std '''
    return stdDev*stdDev

  @staticmethod
  def median(lst):
    if len(lst) < 1:
      return None
    if len(lst) %2 == 1:
      return lst[((len(lst)+1)/2)-1]
    else:
      return float(sum(lst[(len(lst)/2)-1:(len(lst)/2)+1]))/2.0

  @staticmethod
  def getMode(sQobj, base_url, nbeId, fieldName):
    '''gets the mode of a field'''
    #https://data.sfgov.org/resource/e2px-wugd.json?$query=SELECT project_units, count(*) as value GROUP BY project_units ORDER BY count(*) desc limit 1'''
    qry = '''%s%s?$query=SELECT %s, count(*) as value WHERE %s IS NOT NULL GROUP BY %s ORDER BY count(*) desc limit 1 |> select %s as value ''' % (base_url, nbeId, fieldName, fieldName, fieldName, fieldName)
    return ProfileFields.getResults(sQobj, qry)

  # @staticmethod
  #def getMedian(base_url, nbeId, fieldName):
  #  '''Gets the median of a numeric field'''
  #   #https://data.sfgov.org/resource/e2px-wugd.json?$query=SELECT project_units as value GROUP BY project_units ORDER BY project_units
  #   median =  np.median(np.array(lst))
  #   return median

  @staticmethod
  def getRange(myMin, myMax, field_type, dt_format=None):
    '''returns the range of a field'''
    if field_type == 'numeric':
      return myMax-myMin
    if field_type == 'timestamp' and dt_format:
      myMin = DateUtils.strToDtObj(myMin, dt_format)
      myMax = DateUtils.strToDtObj(myMax, dt_format)
      return abs((myMax - myMin).days)
    return None

  @staticmethod
  def pretty_name(x):
    x *= 100
    if x == int(x):
        return '%.0f%%' % x
    else:
        return '%.1f%%' % x

  @staticmethod
  def get_stats(sQobj, base_url, nbeId, fieldName):
    qry_cols = '''%s  WHERE %s IS NOT NULL ORDER BY %s''' % (fieldName, fieldName, fieldName)
    results =  sQobj.pageThroughResultsSelect(nbeId, qry_cols)
    results = [float(result[fieldName]) for result in results]
    lst = pd.Series(results)
    stats = {}
    for x in np.array([0.05, 0.25, 0.5, 0.75, 0.95]):
      stats[ProfileFields.pretty_name(x)] = lst.quantile(x)
    stats['iqr'] = stats['75%'] - stats['25%']
    stats['kurtosis'] = lst.kurt()
    stats['skewness'] = lst.skew()
    #returns mean absolute deviation of the values for the requested axis
    stats['mean_absolute_deviation'] = lst.mad()
    stats['median'] =  lst.median()
    print stats
    return stats



if __name__ == "__main__":
    main()

