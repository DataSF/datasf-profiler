

# coding: utf-8
#!/usr/bin/env python

#from __future__ import division
from ConfigUtils import *
import urllib2


class WebTasks:

  @staticmethod
  def runWebTask(configItems):
    cItemsWebtask =  ConfigUtils(configItems['inputConfigDir'], configItems['webtask_config_fn'] )
    configItemsWebTasks = cItemsWebtask.getConfigs()
    url = configItemsWebTasks['webtask_url']
    attempts = 0
    while attempts < 3:
      try:
        response = urllib2.urlopen(url, timeout = 15)
        content = response.read()
        if(content):
          f = open( configItems['log_dir'] + "algolia-sync-results.json", 'w' )
          f.write( content )
          f.close()
          return True
          break

      except urllib2.URLError as e:
        attempts += 1
        print type(e)
        return False
    return False








if __name__ == "__main__":
    main()
