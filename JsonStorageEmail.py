from json import dumps
from logging import getLogger,basicConfig,DEBUG,INFO,WARN,ERROR
from  os import makedirs
from os.path import exists,join
from  sys import *

_author__ = 'Xavier Garcia Cabellos'
__date__ = '20180101'
__version__ = 0.01
__description__ = 'This scripts write emails in datasource json file'


class json_storage_email():
    """abstact class for reading emails from different services"""
    driver = None
    open_db = False
    directory = None
    file_name = None
    filename = None
    initialized = False
    logger = None
    def __init__(self, log_file,level,log_directory,driver=None ):
        self.driver = driver
        self.active_log(log_file,level,log_directory)


    def storage_type(self):
        """"Return a string representing the type of connection this is."""
        return 'file'

    def store_file(self, directory, file_name):
        if self.driver != None:
            self.driver.close()
        self.directory = directory
        self.file_name = file_name
        if self.directory:
            if not exists(self.directory):
                makedirs(self.directory)
        else:
            raise Exception("Error storing the json files. directory unable to create:  %s",directory)

        self.filename = join(self.directory, self.file_name)
        self.driver = open(self.filename, 'w+')
        self.initialized = True
        return self.driver

    def store(self, jsonified_messages, directory=None, file_name=None):
        """"store the list of message in file in json format."""
        try:

            if directory is not None and file_name is not None:
                self.store_file(directory, file_name)
            elif (self.driver == None):
                raise Exception('error using file descriptor. it has not been initialized')
            self.driver.write(dumps(jsonified_messages))
            if self.initialized == True:
                self.driver.close()
            self.logger.info('Data written in ' + self.filename)
        except Exception as writingError:
            self.logger.exception('Error writing json in file:' + file_name + ': %s', writingError)
        return jsonified_messages

    def __del__(self):
        self.driver = None

    def active_log(self, log_name='json_storage_email.log', level=INFO, log_directory='./logs/'):

        if log_directory:
            if not exists(log_directory):
                makedirs(log_directory)
            log_path = join(log_directory, log_name)
        else:
            log_path = log_name

        basicConfig(filename=log_path, level=level,
                            format='%(asctime)s | %(levelname)s | %(name)s | %(process)d | %(message)s', filemode='a')

        self.logger = getLogger("jsonStorageEmail")
        self.logger.debug('Starting jsonStorageEmail logger using v.' +  str(__version__) + '  System ' + platform+'  Version ' + version)

        return self.logger
