import configparser
from Job import Job
from PDOFactory import DBConnection
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

    def truncateExecuteTable(self):
        query = "TRUNCATE TABLE tab_dw_copy_run_board"
        cursor = self.dbConn.getCursor()()
        cursor.execute(query)
        self.dbConn.getCommit()

    def insertHistoryTable(self):
        query = "INSERT INTO tab_dw_copy_run_board_history (JobID, JobRunner, STATUS, ExecutionTimeStamp, StartDate, EndDate, AmountWrite, AmountRead, ExecutionCounter, Retry) "
        query += "SELECT JobID, JobRunner, STATUS, ExecutionTimeStamp, StartDate, EndDate, AmountWrite, AmountRead, ExecutionCounter, Retry "
        query += "FROM tab_dw_copy_run_board"
        cursor = self.dbConn.getCursor()
        cursor.execute(query)
        self.dbConn.getCommit()

    def insertExecutionTable(self, obj):
        self.Job = obj
        query = "INSERT INTO tab_dw_copy_run_board (JobID, STATUS, ExecutionTimeStamp, StartDate, EndDate, AmountWrite, AmountRead, ExecutionCounter, Retry) VALUES (%s, %s, %s, %s, %s, %s, %s, 1,0)"
        parameters = (self.Job.jobId, self.Job.status, self.Job.executionTimeStamp, self.Job.startDate, self.Job.endDate, self.Job.amountWrite, self.Job.amountRead)
        cursor = self.dbConn.getCursor()
        cursor.execute(query, parameters)
        self.dbConn.getCommit()

    def checkJobToExecute(self):
        query = "SELECT COUNT(0) total FROM tab_dw_copy_run_board WHERE STATUS NOT IN ('RUNNING','ERROR','LACK_OF_PREREQ','VERIFIER_ERROR','SUCCESS','RETRY')"
        cursor = self.dbConn.getCursor()
        cursor.execute(query)
        result = cursor.fetchone()[0]
        return result



        
