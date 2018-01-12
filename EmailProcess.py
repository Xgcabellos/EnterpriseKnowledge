# version v1.0

import email
import email.utils
import imaplib
import quopri
import time
from abc import ABCMeta, abstractmethod

from bs4 import BeautifulSoup
from dateutil.parser import parse
from py._xmlgen import unicode

import ConnectionProperties

__author__ = 'Xavier Garcia Cabellos'
__date__ = '20180101'
__version__ = 0.01
__description__ = 'This scripts handles processing and output of different Email Containers'


class abstract_email(object):
    """abstact class for reading emails from different services"""
    conx = ConnectionProperties.connexion_properties

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


class gmail(abstract_email):
    """ Class for read email from google Mapi"""
    mail = imaplib.IMAP4
    connected = False
    folder_list = []
    folder_dict_size = {}
    folder_dict_data = {}
    status = 0

    def __init__(self, con, email=None):
        """init with connection data and email object inicialided"""
        self.conx = con
        self.mail = email
        if email == None:
            self.mail = imaplib.IMAP4_SSL(self.conx.SMTP_SERVER)
            self.mail.login(self.conx.FROM_EMAIL, self.conx.FROM_PWD)
            self.connected = True
        if self.mail.state == 'NONAUTH':
            mail = imaplib.IMAP4_SSL(self.conx.SMTP_SERVER)
            mail.login(self.conx.FROM_EMAIL, self.conx.FROM_PWD)
            self.connected = True
        status, self.folder_list = self.mail.list()
        print(self.folder_list)
        for folder in self.folder_list:
            name_folder = str(folder).split('"')[-2]

            try:
                self.mail.select(name_folder)
                result, data = self.mail.uid('search', None, "ALL")
                size_folder = len(data[0].split())
                self.folder_dict_size[name_folder] = size_folder
                self.folder_dict_data[name_folder] = data
                print("Reading (" + str(name_folder) + "):" + str(size_folder) + " email inside")
            except Exception as read_exp:
                print("error in reading (" + str(name_folder) + ") for prepare the dict of size and data :" + str(
                    read_exp))

    def __del__(self):
        """closing the email connection"""
        if (self.mail != None and self.connected == True):
            self.mail.logout()
        else:
            pass

    def read(self, start, stop, folder):
        try:
            counter = stop - start
            msg_list = []
            try:
                self.mail.select(folder)
            except Exception as read_exp:
                print(str(read_exp))
                print("error in mail.select(" + folder + ")")
                print(str(read_exp.__class__))
                return msg_list
            result, data = self.mail.uid('search', None, "ALL")
            # search and return uids instead

            i = len(data[0].split())  # data[0] is a space separate string
            # stop=counter
            if stop > i: stop = i
            for x in range(start, stop):
                counter = counter - 1
                latest_email_uid = data[0].split()[x]  # unique ids wrt label selected
                result, email_data = self.mail.uid('fetch', latest_email_uid, '(RFC822)')
                # fetch the email body (RFC822) for the given ID
                for response_part in email_data:
                    try:
                        if isinstance(response_part, tuple):
                            # continue inside the same for loop as above
                            try:
                                response_part_string = response_part[1].decode('utf-8')
                            except Exception as read_exp:
                                print(str(read_exp))
                                print('error in response part')
                                print(str(read_exp.__class__))
                                response_part_string = response_part[1].decode('utf-8', errors='ignore')
                            id_message_string = response_part[0].decode('utf-8')
                            msg = email.message_from_string(response_part_string)
                            msg_list.append(msg)
                            print(id_message_string)
                    except Exception as read_exp:
                        print(str(read_exp))
                        print(str(read_exp.__class__))

            return msg_list
        except Exception as e:
            print(str(e))
            print(str(e.__class__))

    def read_all(self, size_blocks):
        msg_list = []
        counter = 0
        try:
            print(self.folder_list)

            for folder in self.folder_list:
                name_folder = str(folder).split('"')[-2]
                if (name_folder.upper() != 'INBOX') and (name_folder.upper() != 'ENVIADO'):  ## be careful TEMPORAL
                    continue
                print(" mail.select(" + name_folder + ")")
                size = self.folder_dict_size[name_folder]
                start_email = 0
                counter = 0
                while counter < size:
                    msg_list += self.read(start_email, start_email + size_blocks, name_folder)
                    counter += len(msg_list)
                    start_email += size_blocks
            return msg_list
        except Exception as e:
            print(str(e))

    def jsonizer_emails(self, msg_list):
        jsonified_messages = [self.jsonifyMessage(m) for m in msg_list]
        content = [p['content'] for m in jsonified_messages for p in m['parts']]
        # Content can still be quite messy and contain line breaks and other quirks.

        return jsonified_messages

    def jsonifyMessage(self, msg):
        """
            Method for create Json Messages
            :msg  email message
            :return json_msg. A json string message
        """
        json_msg = {'parts': []}
        for (k, v) in msg.items():
            json_msg[k] = v  # .decode('utf-8', 'ignore')

        # The To, Cc, and Bcc fields, if present, could have multiple items.
        # Note that not all of these fields are necessarily defined.

        for k in ['To', 'Cc', 'Bcc']:
            if not json_msg.get(k):
                continue
            json_msg[k] = json_msg[k].replace('\n', '').replace('\t', '').replace('\r', '').replace(' ', '').split(
                ',')  # .decode('utf-8', 'ignore').split(',')
        for part in msg.walk():
            json_part = {}

            # print('content type: ' + part.get_content_maintype())
            if part.get_content_maintype() == 'multipart':
                continue

            json_part['contentType'] = part.get_content_type()

            if part.get_content_type() == 'text/plain':
                json_part['content'] = unicode(part.get_payload(decode=True))
            else:
                if part.get_content_type() == 'text/html':
                    json_part['content'] = self.cleanContent(part.get_payload(decode=True))
                else:
                    json_part['content'] = part.get_content_maintype() + ' deleted for mapping json'

            json_msg['parts'].append(json_part)

        # Finally, convert date from asctime to milliseconds since epoch using the
        # $date descriptor so it imports "natively" as an ISODate object in MongoDB.
        try:
            if json_msg['Date'] != None:
                then = parse(json_msg['Date'])
                millis = int(time.mktime(then.timetuple()) * 1000 + then.microsecond / 1000)
                json_msg['Date'] = {'$date': millis}
        except Exception as e:
            print("error parsing Date :" + str(json_msg['Date']) + " Error:" + str(e))

        return json_msg

    def cleanContent(self, msg):
        # Decode message from "quoted printable" format
        try:
            msg = quopri.decodestring(msg)
        except Exception as decode_exp:
            print("error in decode email msg:" + str(decode_exp))
            # print(msg)

        # Strip out HTML tags, if any are present.
        # Bail on unknown encodings if errors happen in BeautifulSoup.
        try:
            soup = BeautifulSoup(msg, "lxml")
        except:
            return 'error BeautifulSoup'
        return ''.join(soup.findAll(text=True))

