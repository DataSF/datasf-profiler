
# coding: utf-8
#!/usr/bin/env python

from Utils import *
from PandasUtils import *

class ProfileFields:

  @staticmethod
  def getBaseDatasetJson(sQobj, configItems, fbf):
    #qry =  '''%s%s.json?$query=SELECT columnid, datasetid, nbeid, dataset_name, field_type, last_updt_dt_data  WHERE privateordeleted != true ''' % (base_url, fbf)
    qryCols = '''columnid, datasetid, nbeid, dataset_name, field_type, last_updt_dt_data WHERE privateordeleted != true  '''
    results_json = sQobj.pageThroughResultsSelect(fbf, qryCols)
    return FileUtils.write_json_object(results_json, configItems['pickle_data_dir'], configItems['mm_dd_json_fn'])

  @staticmethod
  def get_dataset_as_dfList(data_dir, json_file, base_url):
    json_obj = FileUtils.loadJsonFile(data_dir, json_file)
    df = PandasUtils.makeDfFromJson(json_obj)
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
        return  int(results[0]['value'])
      if (len(results) > 0) and 'cnt' in results[0].keys():
        return  int(results[0]['cnt'])
    return 0

  @staticmethod
  def getTotal(sQobj, base_url, nbeId):
    '''returns the total number of records in dataset'''
    #'https://data.sfgov.org/resource/93gi-sfd2.json?$query=SELECT coubase_url,nt(*)
    dataset_cnt_total_qry = '''%s%s.json?$query=SELECT count(*) as value''' % (base_url, nbeId)
    return ProfileFields.getResults(sQobj, dataset_cnt_total_qry)

  @staticmethod
  def getNulls(base_url, nbeId, fieldName ):
    '''count of the number of records with a NULL value '''
    #https://data.sfgov.org/resource/93gi-sfd2.json?$query=SELECT supervisor_district |> SELECT supervisor_district is NULL as is_null, count(*) as count where is_null = true  GROUP BY supervisor_district
    field_is_null_cnt_qry = '''%s%s?$query=SELECT %s |> SELECT %s is NULL AS is_null, count(*) as value where is_null = true  GROUP BY %s | SELECT value''' %( base_url, nbeId, fieldName, fieldName,fieldName)

  @staticmethod
  def getActuals(base_url, nbeId, fieldName , fieldType):
    '''count of the number of records with an actual value (i.e., non-NULL and non-Missing)'''
    #https://data.sfgov.org/resource/e2px-wugd.json?$query=SELECT COUNT(project_status) WHERE project_status IS NOT NULL
    field_actual_cnt_qry = '''%s%s.json?$query=SELECT COUNT(%s) AS value WHERE %s IS NOT NULL''' %( base_url, nbeId, fieldName)
    field_actual_cnt_qry_str =  field_actual_cnt_qry + '''AND %s not like '%25 %25' ''' %(fieldName)
    field_actual_cnt_qry_num =  field_actual_cnt_qry + '''AND %s != 0 ''' %(fieldName)

  @staticmethod
  def getMissing(base_url, nbeId, fieldName , fieldType):
    '''count of the number of records with a missing value- ie zeros and blanks'''
    #get blanks-> only applies to text columns
    #https://data.sfgov.org/resource/e2px-wugd.json?$query=SELECT planning_entitlements, count(*) as cnt WHERE planning_entitlements like "%25 %25"  GROUP By  planning_entitlements
    field_is_blank_cnt_qry = '''%s%s.json?$query=SELECT %s, count(*) as count WHERE %s like '%25 %25' GROUP By  %s |> SELECT SUM(count) as value ''' %(base_url, nbeId, fieldName, fieldName, fieldName)
    #gets the zeros-> only applies to numeric
    #https://data.sfgov.org/resource/e2px-wugd.json?$query=SELECT count(*) as count where sro_units = 0
    field_is_zero_cnt_qry = '''%s%s.json?$query=SELECT count(*) as value where %s = 0'''% (base_url, nbeId, fieldName)

  @staticmethod
  def getCardinality(base_url, nbeId, fieldName , fieldType):
    '''Cardinality-> count of the number of distinct actual values'''
    #https://data.sfgov.org/resource/93gi-sfd2.json?$query=SELECT supervisor_district, count(*) as cnt WHERE supervisor_district IS NOT NULL GROUP BY supervisor_district |> select cnt, count(*) as cnt2  group by cnt  |> select sum(cnt2) as count
    #for bool + date
    field_distinct_vals_cnt_qry = '''%s%s.json?$query=SELECT %s, count(*) as cnt WHERE %s IS NOT NULL GROUP BY %s |> select cnt, count(*) as cnt2  group by cnt  |> select sum(cnt2) as value''' % (base_url, nbeId, fieldName, fieldName)
    #for numbers
    #https://data.sfgov.org/resource/e2px-wugd.json?$query=SELECT sro_units, count(*) as cnt WHERE sro_units IS NOT NULL AND sro_units != 0  GROUP BY sro_units |> select cnt, count(*) as cnt2  group by cnt  |> select sum(cnt2) as count
    field_distinct_vals_cnt_nums_qry = '''%s%s.json?$query=SELECT %s, count(*) as cnt WHERE %s IS NOT NULL AND %s != 0 GROUP BY %s |> select cnt, count(*) as cnt2  group by cnt  |> select sum(cnt2) as value''' % (base_url, nbeId, fieldName, fieldName, fieldName)
    #for strings
    #https://data.sfgov.org/resource/e2px-wugd.json?$query=SELECT planning_entitlements, count(*) as count WHERE planning_entitlements not like '%25 %25' and  planning_entitlements is NOT NULL  GROUP By  planning_entitlements |> SELECT SUM(count) as count
    field_distinct_vals_cnt_str_qry = '''%s%s.json?$query=SELECT %s, count(*) as count WHERE %s not like '%25 %25' and %s is NOT NULL GROUP By %s |> SELECT SUM(count) as value ''' % (base_url, nbeId, fieldName, fieldName, fieldName,fieldName)

  @staticmethod
  def getCompleteness(actual_cnt, total):
    '''Completeness: percentage calculated as actual divided by the total number of records'''
    return round(actual_cnt/total, 2)*100

  @staticmethod
  def getUniqueness(cardinality_cnt, total):
    '''Uniqueness: percentage calculated as Cardinality divided by the total number of records '''
    return round(cardinality_cnt/total, 2)*100

  @staticmethod
  def distinctness(cardinality_cnt, actual_cnt):
    '''Distinctness: percentage calculated as Cardinality divided by Actual'''
    return reround(cardinality_cnt/actual_cnt, 2)*100

  @staticmethod
  def isPrimaryKeyCandidate(uniquess, completeness):
    if unique == 100 and completeness == 100:
      return True
    return False

  @staticmethod
  def getAvg(base_url, nbeId, fieldName):
    '''gets average for numeric fields'''
    #https://data.sfgov.org/resource/e2px-wugd.json?$select=avg(project_units)
    avg_qry = '''%s%s.json?$select=avg(%s) as value''' % (base_url, nbeId, fieldName)

  @staticmethod
  def getMax(base_url, nbeId, fieldName):
    '''gets max for field- can work on text and numeric fields'''
    #https://data.sfgov.org/resource/e2px-wugd.json?$select=max(project_units) as value
    max_qry = '''%s%s.json?$select=max(%s) as value''' % (base_url, nbeId, fieldName)

  @staticmethod
  def getMin(base_url, nbeId, fieldName):
    '''gets max for field- can work on text and numeric fields'''
    #https://data.sfgov.org/resource/e2px-wugd.json?$select=mix(project_units) as value
    min_qry = '''%s%s.json?$select=min(%s) as value''' % (base_url, nbeId, fieldName)

  @staticmethod
  def getSum(base_url, nbeId, fieldName):
    sum_qry = '''%s%s.json?$select=sum(%s) as value''' % (base_url, nbeId, fieldName)

  @staticmethod
  def getStdDev(base_url, nbeId, fieldName):
    '''returns the population standard deviation of a numeric field'''
    #https://data.sfgov.org/resource/e2px-wugd.json?$select=stddev_pop(project_units) as value
    stdDev_qry = '''%s%s.json?$select=stddev_pop(%s) as value''' % (base_url, nbeId, fieldName)

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
  def getMode(base_url, nbeId, fieldName):
    '''gets the mode of a field'''
    #https://data.sfgov.org/resource/e2px-wugd.json?$query=SELECT project_units, count(*) as value GROUP BY project_units ORDER BY count(*) desc limit 1'''
    get_mode_qry = '''%s%s?$query=SELECT %s, count(*) as value GROUP BY %s ORDER BY count(*) desc limit 1 |> select %s as value ''' % (base_url, nbeId, fieldName, fieldName, fieldName)

  @staticmethod
  def getMedian(base_url, nbeId, fieldName):
    '''Gets the median of a numeric field'''
    #https://data.sfgov.org/resource/e2px-wugd.json?$query=SELECT project_units as value GROUP BY project_units ORDER BY project_units
    get_values_qry = '''%s%s.json?$query=SELECT %s as value GROUP BY %s ORDER BY %s ''' % (base_url, nbeId, fieldName, fieldName)
    median =  np.median(np.array(lst))
    return median

  @staticmethod
  def getRange(min, max):
    '''returns the range of a numeric field'''
    return max-min

  @staticmethod
  def pretty_name(x):
    x *= 100
    if x == int(x):
        return '%.0f%%' % x
    else:
        return '%.1f%%' % x

  @staticmethod
  def get_stats(lst):
    lst = pd.Series(lst)
    for x in np.array([0.05, 0.25, 0.5, 0.75, 0.95]):
      stats[pretty_name(x)] = lst.dropna().quantile(x)
    stats['iqr'] = stats['75%'] - stats['25%']
    stats['kurtosis'] = lst.kurt()
    stats['skewness'] = lst.skew()
    #returns mean absolute deviation of the values for the requested axis
    stats['mean_absolute_deviation'] = lst.mad()
    stats['median'] =  lst.median()
    return stats



  #gets uniq records -> only applies to text and date columns
  #https://data.sfgov.org/resource/93gi-sfd2.json?$query=SELECT file_date, count(*) as cnt GROUP By file_date  |> SELECT SUM(cnt) as count where cnt = 1
  #field_cardinality_qry = '''%s%s.json?$query=SELECT %s, count(*) as cnt GROUP By %s  HAVING count(*) = 1 |> SELECT SUM(cnt)''' % (base_url, nbeId, fieldName, fieldName)


if __name__ == "__main__":
    main()

