# version v1.1
import logging
import os
import re
import sys
from abc import ABCMeta, abstractmethod

_author__ = 'Xavier Garcia Cabellos'
__date__ = '20180101'
__version__ = 0.01
__description__ = 'Abstract class for storage emails'

class storage_email():
    """abstact class for reading emails from different services"""
    driver = None
    open_db = False
    level = logging.DEBUG
    __metaclass__ = ABCMeta

    def __init__(self, driver):
        self.driver = driver

    @abstractmethod
    def storage_type(self):
        """"Return a string representing the type of connection this is."""
        raise RuntimeError('email_type abstact class')

    @abstractmethod
    def store(self, msg_list):
        """"Return a string representing the type of connection this is."""
        raise RuntimeError('store_email abstact class')

    def __del__(self):
        self.driver = None

    def unify_name(self, name_raw):
        """clean names for use dem as unic names"""
        if name_raw != None:
            name_period = str.replace(name_raw, '.', '_')
            name_period = str.replace(name_period, '@', '_at_')
            name_period = str.replace(name_period, '-', '__')
            name_period = str.replace(name_period, '"', ' ')
            # if (str.find(name_period, 'alvaro') != -1):
            #     print(name_period)
            return name_period
        else:
            return None

    def clear_email(self, sender):
        """clean email form caracters and text not emailed"""
        i = 0
        for n in sender:
            if n.find('@') == -1:
                sender[i] = None
                i = i + 1
                continue
            n = str.replace(str(n), '"', ' ')
            n = str.replace(str(n), '\n', ' ')
            n = str.replace(str(n), '\t', ' ')
            index = str.find(n, '<')
            if index != -1:
                n = n[index:]
            nn = n.split()[-1]
            if nn.find('@') != -1:
                sender[i] = re.sub(r'[<>]', '', nn.split()[-1])

            else:
                # print('eliminando: '+sender[i])
                sender[i] = None
            i = i + 1
        return sender

    def active_log(self, log_name='email_process.log', level=logging.INFO, log_directory='./logs/'):

        if log_directory:
            if not os.path.exists(log_directory):
                os.makedirs(log_directory)
            log_path = os.path.join(log_directory, log_name)
        else:
            log_path = log_name
        logging.basicConfig(filename=log_path, level=level,
                            format='%(asctime)s | %(levelname)s | %(message)s', filemode='a')

        logging.info('Starting PST_process v.' + str(__version__))
        logging.info('System ' + sys.platform)
        logging.info('Version ' + sys.version)

        logging.debug('Starting object...' + str(self.__class__))
