from JobDAO import JobDAO
from Job import Job

job = JobDAO()

jobList = job.getJobList()

print(job.checkJobToExecute())

for job in jobList:
    jobId = job[0]
    ExecutionTime = job[1]
    status = job[2]
    startDate = job[3]
    endDate = job[4]
    amountRead = job[5]
    AmountWrite = job[6]
    #print("INSERT INTO tab_dw_copy_run_board (JobID, ExecutionTimeStamp, STATUS, StartDate, EndDate, AmountRead, AmountWrite) VALUES ({}, '{}', {}, {}, {}, {}, {});".format(jobId, ExecutionTime, status, startDate, endDate, amountRead, AmountWrite))
    
    

