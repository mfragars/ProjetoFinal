from JobDAO import JobDAO
from Job import Job
from ThreadWorker import ThreadWorker
import configparser
import os
import sys
import threading
import time
from datetime import datetime

config = configparser.ConfigParser()
config.read('./config.properties')
pentahoPath=config['PentahoSection']['pentahoPath']
jobsPath=config['PentahoSection']['jobsPath']
threadLimit=int(config['ExecutionSection']['thread.limit'])
sleeptime=float(config['ExecutionSection']['sleep.time'])


class RunJobs():
    jDao = JobDAO()
    threadExecuting=0

    

    def cleanupExecutionTable(self):
        # Insert data from last execution on history table and truncate execution table
        try:
            self.jDao.insertHistoryTable()
        except Exception as e:
            print('Error to insert in history table - {}'.format(e))

        try:
            self.jDao.truncateExecuteTable()
        except Exception as e:
            print(e)

    def jobList(self):
        # Get job list and insert job list in execution table
        try:
            joblist = self.jDao.getJobList()
        except Exception as e:
            print(e)
            

        for j in joblist:
            job = Job(j[0], j[1], str(j[2]), str(j[3]), str(j[4]), j[5], j[6])
            try:
                self.jDao.insertExecutionTable(job)
            except Exception as e:
                print(e)

    def executeJob(self, jobId):
        ExecutionCounter = 1
        jThread = JobDAO()
        # Check Pre Requiriment
        preReq = False
        chkPreReqCount=0
        while not preReq:
            date = datetime.now()
            print("{} - Checking Pre Requiriment to job {}".format(date, jobId))
            try:
                preReq = jThread.checkPreRequirement(jobId)
            except Exception as e:
                print('Error checking pre requiriment for job {}'.format(jobId))
                print(e)

            if preReq:
                date = datetime.now()
                print('{} - Check Pre Requirimento to job {} return True'.format(date, jobId))
            else:
                if chkPreReqCount < 3:
                    print('{} - Check Pre Requirimento to job {} return False - Waintg {} sec to new check'.format(date, jobId, sleeptime))
                    chkPreReqCount += 1
                    time.sleep(sleeptime)
                else:
                    date = datetime.now()
                    print('{} - Fail pre requiriment for job {}'.format(date, jobId))
                    jThread.updateJobStatus(jobId, 'LACK_OF_PREREQ', 'endDate')
                    break

        # Run kettle command to execute job
        # Check if job pass on Pre Requiriment check
        jobStatus = jThread.getJobStatus(jobId)
        print('Job {} status {}'.format(jobId, jobStatus))
        if jobStatus != 'LACK_OF_PREREQ':
            date = datetime.now()
            print('{} - Starting job execution JobId: {}'.format(date, jobId))
            jThread.updateJobStatus(jobId, 'RUNNING')
            jobPath = jThread.getJobPath(jobId)
            kettleCMD =('cd {}/\n'.format(pentahoPath))
            kettleCMD +=('{}/kettle.sh {}/{} Basic'.format(pentahoPath, jobsPath, jobPath))
            result = os.system(kettleCMD)

            if result == 0:
                jThread.updateJobStatus(jobId, 'SUCCESS', 'endDate')
            else:
                ExecutionCounter = jThread.getJobExecutionCounter(jobId)
                if ExecutionCounter < 3:
                    print('Execution Counter: {} para o job {}'.format(ExecutionCounter, jobId))
                    jThread.updateJobStatus(jobId, 'RETRY', 'endDate')
                else:
                    jThread.updateJobStatus(jobId, 'ERROR', 'endDate')

            self.threadExecuting -= 1
        else:
            self.threadExecuting -= 1
            print('Dicreasing ThreadExecution: {}'.format(self.threadExecuting))


        


def main():
    r = RunJobs()
    jDAO = JobDAO()
    print(r.threadExecuting)
    
    # Cleanup execution table
    date = datetime.now()
    print('{} -  Executing Table cleanup'.format(date))
    r.cleanupExecutionTable()

    # Get Job List and inserting in execution table
    date = datetime.now()
    print('{} -  Getting job list'.format(date))
    r.jobList()

    # Checking if there are jobs to execute
    date = datetime.now()
    print('{} -  Looking for jobs to execute'.format(date))
    while jDAO.checkJobToExecute() > 0:
        print('Jobs to execute: {}'.format(jDAO.checkJobToExecute()))
        # Get first job to execute
        firstJob = jDAO.getFirstJob()
        print('Next job to execute: {}'.format(firstJob))
        
        # Check thread limit
        print('Thread Running: {}'.format(r.threadExecuting))
        if r.threadExecuting < threadLimit:
            r.threadExecuting += 1
            threadname = 'Thread' + str(r.threadExecuting) + "_" + str(firstJob)

            # Update job status and Start Date or End Date as parameter
            print('Update status job: {}'.format(firstJob))
            jDAO.updateJobStatus(firstJob, 'STARTED', 'startDate')

            print('Starting thread {} tagert executeJob args {}'.format(threadname, firstJob))
            try:
                t = threading.Thread(name=threadname, target=r.executeJob, args=(firstJob, ))
                t.start()
            except Exception as e:
                errorMessage = ('Error starting thread {} with args {}'.format(threadname, firstJob))
                print(errorMessage)
                print(e)
            
        else:
            time.sleep(sleeptime)


if __name__ == '__main__':main()