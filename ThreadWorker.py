import configparser
import threading

config = configparser.ConfigParser()
config.read('./config.properties')
threadLimit=int(config['ExecutionSection']['thread.limit'])
sleeptime=float(config['ExecutionSection']['sleep.time'])


class ThreadWorker(threading.Thread):
    def __init__(self, threadname, threadargs):
        self.threadname = threadname
        self.threadtarget = executeJob
        self.threadargs = threadargs


        print('Starting thread {} tagert {} args {}'.format(self.threadname, self.threadtarget, self.threadargs))

        try:
            
            self.thread = threading.Thread(self, name=self.threadname, target=self.threadtarget, args=(self.threadargs, ))
            self.thread.start()
        except Exception as e:
            errorMessage = ('Error starting thread {} with args {}'.format(self.threadname, self.threadargs))
            print(errorMessage)
            print(e)