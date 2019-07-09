# version v1.0
from  logging import getLogger,DEBUG,INFO,WARNING,WARN,ERROR,error,basicConfig,debug,info
import os
import re
import sys
from abc import ABCMeta, abstractmethod

import ConnectionProperties

__author__ = 'Xavier Garcia Cabellos'
__date__ = '20180101'
__version__ = 0.01
__description__ = 'This scripts handles processing and output of different Email Containers'


class abstract_email(object):
    """abstact class for reading emails from different services"""
    conx = ConnectionProperties.connexion_properties
    log_directory = ""
    __metaclass__ = ABCMeta

    def __init__(self, con):
        self.conx = con

    @abstractmethod
    def email_type(self):
        """"Return a string representing the type of connection this is."""
        raise RuntimeError('email_type abstact class')

    @abstractmethod
    def read(self, start, stop, folder):
        """"Return a string representing the type of connection this is."""
        raise RuntimeError('read_email abstact class')

    def __del__(self):
        self.conx = None

    @abstractmethod
    def jsonizer_emails(self, msg_list, directory, sufix):
        """"Return a  json string representing the mapping of email this is."""
        raise RuntimeError('jsonizer abstact class')

    @staticmethod
    def clear_email(sender):
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

    @staticmethod
    def email_filter(msg_list):
        """Rfiltering different kind of message"""
        for msg in msg_list:

            if 'from' in msg:
                if (msg['from'].find("no-reply") != -1) or (msg['from'].find("noreply") != -1):
                    msg_list.remove(msg)
                    continue
            elif 'From' in msg:
                if (msg['From'].find("no-reply") != -1) or (msg['From'].find("noreply") != -1):
                    msg_list.remove(msg)
                    continue
            else:
                error('FROM DOEST EXIST')
                continue
            if 'Message-Id' in msg:
                if (msg['Message-Id'] is None):
                    continue
            elif 'Message-ID' in msg:
                if (msg['Message-ID'] is None):
                    continue
        return msg_list

    @staticmethod
    def getEmail(emailAsString):
        # for email in emailAsString.split(','):
        return emailAsString.split(',')

    @staticmethod
    def active_log(log_name='email_process.log', level=INFO, log_directory='./logs/'):

        if log_directory:
            if not os.path.exists(log_directory):
                os.makedirs(log_directory)
            log_path = os.path.join(log_directory, log_name)
        else:
            log_path = log_name
        basicConfig(filename=log_path, level=level,
                            format='%(asctime)s | %(levelname)s | %(message)s', filemode='a')

        debug('Starting mail_process logger using v.' +  + str(__version__) + '  System ' + sys.platform+'  Version ' + sys.version)
