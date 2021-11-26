import configparser
from Job import Job
from PDOFactory import DBConnection
from datetime import datetime
config = configparser.ConfigParser()
config.read('./config.properties')
dbAnalitycs_host = config['DataBaseSection']['dbAnalitycs.host']
dbAnalitycs_user = config['DataBaseSection']['dbAnalitycs.user']
dbAnalitycs_password = config['DataBaseSection']['dbAnalitycs.password']
dbAnalitycs_dbName = config['DataBaseSection']['dbAnalitycs.dbname']


class JobDAO:
    def __init__(self):
        self.dbConn = DBConnection(dbAnalitycs_host, dbAnalitycs_dbName, dbAnalitycs_user, dbAnalitycs_password)
        self.dbConn.getConn()

    # Method to get Job list
    def getJobList(self):
        query = "SELECT a.jobID,"
        query += "NOW() ExecutionTimeStamp,"
        query += "IF(b.STATUS IS NOT NULL, IF(b.STATUS=\'SUCCESS\',b.STATUS,\'NOT_STARTED\'), \'NOT_STARTED\') STATUS,"
        query += "IF(b.StartDate IS NOT NULL AND b.STATUS = \'SUCCESS\', b.StartDate, \'0000-00-00 00:00:00\') StartDate,"
        query += "IF(b.EndDate  IS NOT NULL AND b.STATUS = \'SUCCESS\', b.EndDate , \'0000-00-00 00:00:00\') EndDate,"
        query += "IF(b.AmountRead IS NOT NULL, b.AmountRead, 0) AmountRead,"
        query += "IF(b.AmountWrite IS NOT NULL, b.AmountWrite, 0) AmountWrite"
        query += " FROM tab_dw_copy_jobs a"
        query += " LEFT JOIN ( SELECT a.JobID, a.STATUS, a.StartDate , a.EndDate, a.AmountRead, a.AmountWrite FROM tab_dw_copy_run_board_history a"
        query += " WHERE DATE_FORMAT(NOW(), \'%Y-%m-%d\' ) = DATE_FORMAT(a.StartDate , \'%Y-%m-%d\' )"
        query += " AND CONCAT(a.JobID, a.StartDate) IN  ( SELECT CONCAT(JobID, MAX(StartDate))"
        query += " FROM tab_dw_copy_run_board_history GROUP BY JobID)) b"
        query += " ON a.jobID = b.jobID AND DATE_FORMAT(NOW(), \'%Y-%m-%d\' ) = DATE_FORMAT(b.StartDate , \'%Y-%m-%d\' )"
        query += " WHERE a.JobStatus = 'ENABLE'"
        cursor = self.dbConn.getCursor()
        cursor.execute(query)
        result = cursor.fetchall()
        return result

    # Method to truncate execution table
    def truncateExecuteTable(self):
        query = "TRUNCATE TABLE tab_dw_copy_run_board"
        cursor = self.dbConn.getCursor()
        cursor.execute(query)
        self.dbConn.getCommit()


    # Method to get data from last execution and insert in history table
    def insertHistoryTable(self):
        query = "INSERT INTO tab_dw_copy_run_board_history (JobID, JobRunner, STATUS, ExecutionTimeStamp, StartDate, EndDate, AmountWrite, AmountRead, ExecutionCounter, Retry) "
        query += "SELECT JobID, JobRunner, STATUS, ExecutionTimeStamp, StartDate, EndDate, AmountWrite, AmountRead, ExecutionCounter, Retry "
        query += "FROM tab_dw_copy_run_board"
        cursor = self.dbConn.getCursor()
        cursor.execute(query)
        self.dbConn.getCommit()


    # Method to insert job list to execute in execution table
    def insertExecutionTable(self, obj):
        self.Job = obj
        query = "INSERT INTO tab_dw_copy_run_board (JobID, STATUS, ExecutionTimeStamp, StartDate, EndDate, AmountWrite, AmountRead) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        parameters = (self.Job.getJobId(), self.Job.getStatus(), self.Job.getExecutionTimeStamp(), self.Job.getStartDate(), self.Job.getEndDate(), self.Job.getAmountWrite(), self.Job.getAmountRead())
        cursor = self.dbConn.getCursor()
        cursor.execute(query, parameters)
        self.dbConn.getCommit()

    # Method to check if there are jobs to execute
    def checkJobToExecute(self):
        query = "SELECT COUNT(0) total FROM tab_dw_copy_run_board WHERE STATUS NOT IN ('RUNNING','ERROR','LACK_OF_PREREQ','VERIFIER_ERROR','SUCCESS', 'STARTED')"
        cursor = self.dbConn.getCursor()
        cursor.execute(query)
        result = cursor.fetchone()[0]
        return result

    # Method to get the first job from execution table
    def getFirstJob(self):
        query = "SELECT rb.JobID FROM tab_dw_copy_run_board rb INNER JOIN tab_dw_copy_jobs j ON rb.JobID = j.JobID WHERE STATUS IN ('NOT_STARTED', 'RETRY') ORDER BY priority LIMIT 1"
        cursor = self.dbConn.getCursor()
        cursor.execute(query)
        result = cursor.fetchone()[0]
        return result

    # Method to get job path
    def getJobPath(self, jobId):
        query = "SELECT JobPath FROM tab_dw_copy_jobs WHERE JobID = %s"
        parameters = int(jobId)
        cursor = self.dbConn.getCursor()
        cursor.execute(query, (parameters,))
        result = cursor.fetchone()[0]
        return result

    # Method to update job status
    def updateJobStatus(self, jobId, status, typeUpd=None):
        if typeUpd != None:
            if typeUpd == 'startDate':
                query = "UPDATE tab_dw_copy_run_board SET STATUS = %s, StartDate = %s WHERE JobID = %s"
            elif status == 'RETRY':
                query = "UPDATE tab_dw_copy_run_board SET STATUS = %s, EndDate = %s , Retry = Retry+1 WHERE JobID = %s"
            else:
                query = "UPDATE tab_dw_copy_run_board SET STATUS = %s, EndDate = %s WHERE JobID = %s"
                
            date = datetime.now()
            parameters = (str(status), str(date), int(jobId))
            cursor = self.dbConn.getCursor()
            cursor.execute(query, parameters)
            self.dbConn.getCommit()

        else:
            query = "UPDATE tab_dw_copy_run_board SET STATUS = %s WHERE JobID = %s"
            parameters = (str(status), int(jobId))
            cursor = self.dbConn.getCursor()
            cursor.execute(query, parameters)
            self.dbConn.getCommit()


    # Method to check if job has pre requiriment
    def checkPreRequirement(self, jobId):
        query = "SELECT COUNT(0) TOTAL FROM tab_dw_copy_dependencies WHERE JobID = %s"
        queryStatus = "SELECT COUNT(0) TOTAL FROM tab_dw_copy_run_board WHERE JobID IN (SELECT JobDependent FROM tab_dw_copy_dependencies WHERE JobID = %s) AND STATUS != 'SUCCESS'"
        parameters = int(jobId)
        cursor = self.dbConn.getCursor()
        cursor.execute(query, (parameters,))
        result = cursor.fetchone()[0]
        
        

        if result > 0:
            cursor.execute(queryStatus, (parameters,))
            resultStatus = cursor.fetchone()[0]
            if resultStatus > 0:
                return False
            else:
                return True
        else:
            return True

    # Method to get Job execution counter
    def getJobExecutionCounter(self, jobId):
        query = "SELECT Retry FROM tab_dw_copy_run_board WHERE JobID = %s"
        parameters = int(jobId)
        cursor = self.dbConn.getCursor()
        cursor.execute(query, (parameters,))
        result = cursor.fetchone()[0]
        return result

    # Method to get job status
    def getJobStatus(self, jobId):
        query = "SELECT Status FROM tab_dw_copy_run_board WHERE JobID = %s"
        parameters = int(jobId)
        cursor = self.dbConn.getCursor()
        cursor.execute(query, (parameters, ))
        result = cursor.fetchone()[0]
        return result



        
