import json
import logging
import os

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

    def __init__(self, driver):
        self.driver = driver

    def storage_type(self):
        """"Return a string representing the type of connection this is."""
        return 'file'

    def store_file(self, directory, file_name):
        if self.driver != None:
            self.driver.close()
        self.directory = directory
        self.file_name = file_name
        self.filename = os.path.join(self.directory, self.file_name)
        self.driver = open(self.filename, 'w')
        self.initialized = True
        return self.driver

    def store(self, jsonified_messages, directory=None, file_name=None):
        """"store the list of message in file in json format."""

        if directory is not None and file_name is not None:
            self.store_file(directory, file_name)
        elif (self.driver == None):
            raise Exception('error using file descriptor. it has not been initialized')
        self.driver.write(json.dumps(jsonified_messages))
        if self.initialized == True:
            self.driver.close()
        logging.info('Data written out to ' + self.filename)
        return jsonified_messages

    def __del__(self):
        self.driver = None
