# coding: utf-8
#!/usr/bin/env python

import time
import datetime
import logging
import os

class pyLogger:
    def __init__(self, configItems):
        self.logfn = configItems['exception_logfile']
        self.log_dir = configItems['log_dir']
        self.logfile_fullpath = self.log_dir+self.logfn
        self.app_name = configItems['app_name']

    def setConfig(self):
        #open a file to clear log
        #fo = open(self.logfile_fullpath, "w")
        #fo.close
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        logger = logging.getLogger(self.app_name)
        logger.setLevel(logging.INFO)
        # create the logging file handler
        fh = logging.FileHandler(self.logfile_fullpath)
        #formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        # add handler to logger object
        logger.addHandler(fh)
        return logger

if __name__ == "__main__":
    main()
