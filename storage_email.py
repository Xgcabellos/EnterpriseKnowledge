# version v1.1
from abc import ABCMeta, abstractmethod
from logging import getLogger, basicConfig, INFO, debug
from os import path, makedirs
from re import sub

_author__ = 'Xavier Garcia Cabellos'
__date__ = '20180101'
__version__ = 0.01
__description__ = 'Abstract class for storage emails'


class StorageEmail():
    """abstact class for reading emails from different services"""
    driver = None
    open_db = False
    level = INFO
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
                sender[i] = sub(r'[<>]', '', nn.split()[-1])

            else:
                # print('eliminando: '+sender[i])
                sender[i] = None
            i = i + 1
        return sender

    def active_log(self, log_name='email_process.log', level=INFO, log_directory='./logs/'):

        if log_directory:
            if not path.exists(log_directory):
                makedirs(log_directory)
            log_path = path.join(log_directory, log_name)
        else:
            log_path = log_name
        basicConfig(filename=log_path, level=level,
                    format='%(asctime)s | %(levelname)s | %(name)s | %(process)d | %(message)s', filemode='a')

        logger = getLogger(str(self.storage_type()))
        # debug('Starting storing_process v.' + str(__version__) + '  System ' + sys.platform+'  Version ' + sys.version)

        debug('Starting object...' + str(self.storage_type()))
        return logger
