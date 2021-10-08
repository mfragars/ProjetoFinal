from datetime import datetime


class Job:
    def __init__(self, jobId = None, executionTimeStamp = None, status = None , startDate = None, endDate = None, amountRead =  None, amountWrite = None):
        self.__jobId = jobId
        self.__executionTimeStamp = executionTimeStamp
        self.__status = status
        self.__startDate = startDate
        self.__endDate = endDate
        self.__amountRead = amountRead
        self.__amountWrite = amountWrite

    # getters method
    def getJobId(self):
        return self.__jobId

    def getExecutionTimeStamp(self):
        return self.__executionTimeStamp

    def getStatus(self):
        return self.__status

    def getStartDate(self):
        return self.__startDate

    def getEndDate(self):
        return self.__endDate
    
    def getAmountRead(self):
        return self.__amountRead

    def getAmountWrite(self):
        return self.__amountWrite

    # Setters Method
    def setJobId(self, jobId):
        if type(jobId) == int:
            self.__jobId = jobId
        else:
            return "Invalid value for JobId"

    def setExecutionTimeStamp(self, ExecutionTimeStamp):
        date = datetime.strptime(ExecutionTimeStamp, '%Y-%m-%d %H:%M:%S')
        self.__executionTimeStamp = date

    def setStatus(self, status):
        self.__status

    def setStartDate(self, startDate):
        date = datetime.strptime(startDate, '%Y-%m-%d %H:%M:%S')
        self.__startDate = date

    def setEndDate(self, endDate):
        date = datetime.strptime(endDate, '%Y-%m-%d %H:%M:%S')
        self.__endDate = endDate

    def setAmountRead(self, amountRead):
        if amountRead >= 0:
            self.__amountRead = amountRead
        else:
            return "Invalid value"

    def setAmountWrite(self, amountWrite):
        if amountWrite >= 0:
            self.__amountWrite = amountWrite
        else:
            return "Invalid value"