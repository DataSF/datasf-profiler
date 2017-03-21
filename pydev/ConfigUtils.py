# coding: utf-8
#!/usr/bin/env python


import yaml


class ConfigUtils:
    def __init__(self, inputdir, fieldConfigFile):
        self.inputdir = inputdir
        self.fieldConfigFile = fieldConfigFile

    def getConfigs(self):
        configItems = 0
        with open(self.inputdir + self.fieldConfigFile ,  'r') as stream:
            try:
                configItems = yaml.load(stream)
            except yaml.YAMLError as exc:
                print(exc)
        return configItems


    @staticmethod
    def setConfigs(config_dir, config_file):
        '''returns contents of yaml config file'''
        with open( config_dir + config_file ,  'r') as stream:
            try:
                config_items = yaml.load(stream)
                return config_items
            except yaml.YAMLError as exc:
                print(exc)
        return 0


if __name__ == "__main__":
    main()
