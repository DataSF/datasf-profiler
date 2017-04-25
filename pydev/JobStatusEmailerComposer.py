from Emailer import *
from DictUtils import *


class JobStatusEmailerComposer:
    '''
    util class to get job status- aka check to make sure that records were inserted; also emails results to receipients
    '''
    def __init__(self, configItems,logger, jobNameType):
        self.keysToRemove = ['columns', 'tags']
        self.log_dir = configItems['log_dir']
        self.dataset_base_url = configItems['base_url']
        self.failure =  False
        self.job_name = configItems['jobs'][jobNameType]['job_name']
        self.logfile_fname = self.job_name.replace(" ", "_").lower() + ".csv"
        self.logfile_fullpath = self.log_dir + self.job_name.replace(" ", "_").lower() + ".csv"
        self.configItems =  configItems
        self.inputdir = configItems['inputConfigDir']
        self.rowsInserted = configItems['dataset_records_cnt_field']
        self.source_records_cnt = configItems['src_records_cnt_field']
        self.fourXFour = configItems['fourXFour']
        self.datasetNameField = configItems['dataset_name_field']
        self._logger = logger

    def sucessStatus(self, dataset):
        dataset = DictUtils.removeKeys(dataset, self.keysToRemove)
        #if dataset[self.rowsInserted ] == dataset[self.source_records_cnt]:
        #      dataset['jobStatus'] = "SUCCESS"
        #else:
        #    dataset['jobStatus'] = "FAILURE"
        #    self.failure =  True
        dataset['jobStatus'] = dataset['isLoaded'].upper()
        if dataset['isLoaded'] != 'success':
            self.failure = True
        return dataset

    def makeJobStatusAttachment(self,  finishedDataSets ):
        with open(self.logfile_fullpath, 'w') as csvfile:
            fieldnames = finishedDataSets[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for dataset in finishedDataSets:
                try:
                    writer.writerow({ s:str(v).encode("ascii",  'ignore') for s, v in dataset.iteritems()  } )
                except Exception, e:
                    self._logger.info(str("could not write row"))
                    self._logger.error(str(e))
                    print "could not write row"

    def getJobStatus(self):
        if self.failure:
            return  "FAILED: " + self.job_name
        else:
            return  "SUCCESS: " + self.job_name

    def makeJobStatusMsg( self,  dataset  ):
        msg = dataset['jobStatus'] + ": " + dataset[self.datasetNameField] + "-> Total Rows:" + str(dataset[self.source_records_cnt]) + ", Rows Inserted: " + str(dataset[self.rowsInserted])  + ", Link: http://"  + self.dataset_base_url + "/resource/" + dataset[self.fourXFour] + " \n\n\n "
        return msg

    def sendJobStatusEmail(self, finishedDataSets):
        msgBody  = ""
        for i in range(len(finishedDataSets)):
            #remove the column definitions, check if records where inserted
            dataset = self.sucessStatus( DictUtils.removeKeys(finishedDataSets[i], self.keysToRemove))
            msg = self.makeJobStatusMsg( finishedDataSets[i]  )
            msgBody  = msgBody  + msg
        subject_line = self.getJobStatus()
        email_attachment = self.makeJobStatusAttachment(finishedDataSets)
        e = Emailer(self.configItems)
        emailconfigs = e.setConfigs()
        if os.path.isfile(self.logfile_fullpath):
            e.sendEmails( subject_line, msgBody, self.logfile_fname, self.logfile_fullpath)
        else:
            e.sendEmails( subject_line, msgBody)
        print "****************JOB STATUS******************"
        print subject_line
        print "Email Sent!"
        self._logger.info("****************JOB STATUS******************")
        self._logger.info( subject_line)
        self._logger.info("Email Sent!")
        self._logger.info("****************JOB END******************")

if __name__ == "__main__":
    main()
