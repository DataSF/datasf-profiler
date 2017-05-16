# coding: utf-8
#!/usr/bin/env python

import itertools
import math

class DictUtils:

  @staticmethod
  def remove_blanks_from_dict_on_key(mydict, key_of_blank):
    return dict((k, v) for k, v in mydict.iteritems() if (v!='' and k != key_of_blank) )

  @staticmethod
  def removeKeys( mydict, keysToRemove):
    for key in keysToRemove:
      try:
          remove_columns = mydict.pop(key, None)
      except:
          noKey = True
      return mydict

  @staticmethod
  def filterDictList( dictList, keysToKeep):
        return  [ {key: x[key] for key in keysToKeep if key in x.keys() } for x in dictList]

  @staticmethod
  def filterDict(mydict, keysToKeep):
    mydictKeys = mydict.keys()
    return {key: mydict[key] for key in keysToKeep  if key in mydictKeys }

  @staticmethod
  def filterDictListOnKeyVal(dictlist, key, valuelist):
      #filter list of dictionaries with matching values for a given key
    return [dictio for dictio in dictlist if dictio[key] in valuelist]

  @staticmethod
  def filterDictListOnKeyValExclude(dictlist, key, excludelist):
      #filter list of dictionaries that aren't in an excludeList for a given key
    return [dictio for dictio in dictlist if dictio[key] not in excludelist]

  @staticmethod
  def filterDictOnVals(some_dict, value_to_exclude):
    return {k: v for k, v in some_dict.items() if v != value_to_exclude}

  @staticmethod
  def is_nan(x):
    return isinstance(x, float) and math.isnan(x)

  @staticmethod
  def is_blank(x):
    blank = False
    if( (x == "") or (x == " ") or (x is None)):
      blank = True
    return blank

  @staticmethod
  def filterDictOnNans(some_dict):
    '''excludes all k,v in a dict with v = NaN'''
    return {k: v for k, v in some_dict.items() if not(DictUtils.is_nan(v))}

  @staticmethod
  def filterDictOnBlanks(some_dict):
    return {k: v for k, v in some_dict.items() if not(DictUtils.is_blank(v))}

  @staticmethod
  def merge_two_dicts(x, y):
    """Given two dicts, merge them into a new dict as a shallow copy."""
    z = x.copy()
    z.update(y)
    return z

  @staticmethod
  def consolidateDictList(results, key):
    return [result[key] for result in results ]

  @staticmethod
  def merge_two_dicts(x, y):
    """Given two dicts, merge them into a new dict as a shallow copy."""
    z = x.copy()
    z.update(y)
    return z

if __name__ == "__main__":
    main()
