
# coding: utf-8
#!/usr/bin/env python

from optparse import OptionParser
from ConfigUtils import *
from SocrataStuff import *
from PandasUtils import *
from PyLogger import *
from Queries import *

def parse_opts():
  helpmsgConfigFile = 'Use the -c to add a config yaml file. EX: fieldConfig.yaml'
  parser = OptionParser(usage='usage: %prog [options] ')
  parser.add_option('-c', '--configfile',
                      action='store',
                      dest='configFn',
                      default=None,
                      help=helpmsgConfigFile ,)

  helpmsgConfigDir = 'Use the -d to add directory path for the config files. EX: /home/ubuntu/configs'
  parser.add_option('-d', '--configdir',
                      action='store',
                      dest='configDir',
                      default=None,
                      help=helpmsgConfigDir ,)

  (options, args) = parser.parse_args()

  if  options.configFn is None:
    print "ERROR: You must specify a config yaml file!"
    print helpmsgConfigFile
    exit(1)
  elif options.configDir is None:
    print "ERROR: You must specify a directory path for the config files!"
    print helpmsgConfigDir
    exit(1)
  config_inputdir = None
  fieldConfigFile = None
  fieldConfigFile = options.configFn
  config_inputdir = options.configDir
  return fieldConfigFile, config_inputdir



def main():
  fieldConfigFile, config_inputdir = parse_opts()
  cI =  ConfigUtils(config_inputdir,fieldConfigFile  )
  configItems = cI.getConfigs()
  lg = pyLogger(configItems)
  logger = lg.setConfig()
  #logger.info("****************JOB START******************")
  sc = SocrataClient(config_inputdir, configItems, logger)
  client = sc.connectToSocrata()
  clientItems = sc.connectToSocrataConfigItems()
  scrud = SocrataCRUD(client, clientItems, configItems, logger)
  sQobj = SocrataQueries(clientItems, configItems, logger)

  mmdd_fbf = configItems['dd']['master_dd']['fbf']
  ds_profiles_fbf =  configItems['dd']['ds_profiles']['fbf']
  base_url =  configItems['base_url']
  field_type_fbf =  configItems['dd']['field_type']['fbf']

  datasets = ProfileDatasets.getBaseData(sQobj, base_url,  mmdd_fbf)
  ds_profiles = ProfileDatasets.getCurrentDatasetProfiles(sQobj, base_url, ds_profiles_fbf )
  field_types = ProfileDatasets.getFieldTypes(sQobj, base_url, field_type_fbf)
  datasets_stats =  ProfileDatasets.buildInsertDatasetProfiles(sQobj,  datasets, ds_profiles, mmdd_fbf, field_types)
  print datasets_stats
  #for dataset in datasets[0:1]:
  #  if dataset['datasetid'] in ds_profile_keys:
  #    print 'in here'
  #  else:
  #    dataset_stats = ProfileDatasets.getTypeCnt(sQobj,dataset, mmdd_fbf, field_types)



if __name__ == "__main__":
    main()
