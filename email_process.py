#! /usr/bin/env python
# -*- coding: utf-8 -*-
# version v1.0
import configparser
import os
from collections import Counter
from email import message_from_string, policy
from imaplib import IMAP4
from imaplib import IMAP4_SSL
from logging import *
# from os.path import exists,join
from quopri import *
from re import sub
from sys import *

import bson.json_util
import unicodecsv as csv
from bs4 import BeautifulSoup
# from bson import json_util
from dateutil.parser import parse
from jinja2 import Template
from psutil._compat import xrange
from py._xmlgen import unicode

from abstract_process import AbstractEmail
from connection_properties import email_DUMMY_connexionProperties
from text_process import TextProcess, log_level_conversor

# from os import makedirs,getoid

__author__ = 'Xavier Garcia Cabellos'
__date__ = '20190101'
__version__ = 0.01
__description__ = 'This scripts handles processing and output of Gmail and  Email Containers'

config_name = '../input/config.ini'

config = configparser.ConfigParser()
config.read(config_name)

log_name = config['LOGS']['LOG_FILE']
log_directory = config['LOGS']['LOG_DIRECTORY']
log_level = log_level_conversor(config['LOGS']['log_level_email'])

module_logger = getLogger('EmailProcess')

date_dict = {x: 0 for x in xrange(1, 25)}
date_list = [date_dict.copy() for x in xrange(7)]

FORBIDDEN_TAGS = config['FILTERS']['EMAIL_HEADER_FIELDS_NOT_STORED']


class Gmail(AbstractEmail):
    """ Class for read email from google Mapi"""
    mail = IMAP4
    connected = False
    folder_list = []
    folder_dict_size = {}
    folder_dict_data = {}
    status = 0
    logger = None

    output_directory = config['OUTPUT']['DIRECTORY_OUTPUT']  # './output/'
    json_directory = config['OUTPUT']['DIRECTORY_JSON']

    json_store = None
    date_dict = {x: 0 for x in xrange(1, 25)}
    date_list = [date_dict.copy() for x in xrange(7)]

    def __init__(self, con, foldersSize=True, logger=None, email=None):
        """init with connection data and email object inicialided"""

        if self.logger is None and logger is None:
            self.logger = self.active_log(log_name, log_level, log_directory)
        if logger is not None:
            self.logger = logger
        self.conx = con
        self.mail = email
        if self.conx.DUMMY == True:
            return
        if con is not None:
            self.connect()
        else:
            raise Exception("Error. the connexion object that describe the type of connexion doesn't exist")

        try:
            status, self.folder_list = self.mail.list()
        except Exception as read_exp:
            self.logger.warning("error  reading list of folders  %s ", read_exp)
            self.connect()
            status, self.folder_list = self.mail.list()

        if foldersSize:
            self.logger.debug('List of folders read in {}'.format(str(self.folder_list)))
            for folder in self.folder_list:
                name_folder = str(folder).split('"/"')[-1]  # ('"')[-2]
                # be careful - with google is not exactly the same. verify.

                name_folder = name_folder.replace('\'', '').replace('"', '').strip()
                try:
                    self.mail.select('"' + name_folder + '"')
                    result, data = self.mail.uid('search', None, "ALL")
                    size_folder = len(data[0].split())
                    # Size of the folder. means number of messages
                    self.folder_dict_size[name_folder] = size_folder
                    # data is the number of the message, not the message.
                    self.folder_dict_data[name_folder] = data
                    self.logger.debug("Reading (" + str(name_folder) + "):" + str(size_folder) +
                                      " email inside. Forbidden= %s", forbidden(folder))
                except Exception as read_exp:
                    self.logger.error(
                        "error in reading (" + str(name_folder) + ") for prepare the dict of size and data- %s ",
                        read_exp)

    def __del__(self):
        """closing the email connection"""
        self.logger.debug('closing connexions')
        if (self.mail != None and self.connected == True):
            self.mail.logout()
        else:
            pass

    def connect(self):
        if self.conx is not None:
            if self.mail == None:
                if self.conx.CRYPTED:
                    # for SSL. Gmail need SSL, the initial server for testint doesn't need it.
                    self.mail = IMAP4_SSL(self.conx.SMTP_SERVER)
                else:
                    # Without SSL. Not very realistic
                    self.mail = IMAP4(self.conx.SMTP_SERVER)
                # autentication in email server. with a admin user is possible to reed all
                self.mail.login(self.conx.FROM_EMAIL, self.conx.FROM_PWD)
                self.connected = True
            # maybe you have a object IMAP4 but not connected or authenticated
            if self.mail.state == 'NONAUTH' or self.mail.state == 'CLOSE' or self.mail.state == 'LOGOUT':
                if self.conx.CRYPTED:
                    mail = IMAP4_SSL(self.conx.SMTP_SERVER)
                else:
                    mail = IMAP4(self.conx.SMTP_SERVER)
                mail.login(self.conx.FROM_EMAIL, self.conx.FROM_PWD)
                self.connected = True
        return self.mail

    def get_type(self):
        return 'Gmail'

    def read(self, start, stop, folder):
        counter = stop - start
        number_messages = 0
        try:

            msg_list = []
            try:
                number_messages = self.mail.select('"' + folder + '"')
            except Exception as read_exp:
                self.logger.exception("error in mail.select(" + folder + ") " + str(number_messages))
                return msg_list

            result, data = self.mail.uid('search', None, "ALL")
            # search and return uids instead
            list_of_uids = data[0].split()  # data[0] is a space separate string
            i = len(list_of_uids)
            # stop=counter
            if stop > i: stop = i
            if i == 0:
                return msg_list
            else:
                latest_email_uid = list_of_uids[stop - 1]  # unique ids wrt label selected
            list_of_uids2 = ''
            try:
                for y in range(start, stop):
                    if y > start:
                        list_of_uids2 += ','
                    list_of_uids2 += str(list_of_uids[y].decode('ascii'))
                # result, email_data = self.mail.uid('fetch', latest_email_uid, '(RFC822)')
                result, email_data = self.mail.uid('fetch', list_of_uids2, '(RFC822)')
                # fetch the email body (RFC822) for the given ID
            except Exception as fechError:
                self.logger.error(
                    'error uid fetching ' + str(latest_email_uid) + ' with number of  ' + str(stop - start) + ' of '
                    + str(number_messages) + ' messages in folder '
                    + folder + ' Error Message:' + str(fechError))
                try:
                    self.connect()

                except Exception as connError:
                    self.logger.error(
                        'error uid fetching reconnexion' + str(latest_email_uid) + ' with number of  ' + str(
                            stop - start) + ' of ' + str(number_messages) + ' messages in folder ' + folder +
                        ' Error Message:' + str(connError))
                    return msg_list

                try:
                    result, email_data = self.mail.uid('fetch', list_of_uids2, '(RFC822)')
                    # re-fetch the email body (RFC822) for the given ID
                except Exception as fechError:
                    self.logger.error(
                        'error uid fetching ' + str(latest_email_uid) + ' with number of  ' + str(stop - start) + ' of '
                        + str(number_messages) + ' messages in folder '
                        + folder + ' Error Message:' + str(fechError))
                    return msg_list

            for response_part in email_data:
                counter = counter - 1
                try:
                    if isinstance(response_part, tuple):
                        # continue inside the same for loop as above
                        try:
                            response_part_string = response_part[1].decode('utf-8', errors='ignore')  # 'strict'
                        except Exception as read_exp:
                            self.logger.error(
                                'error in response part ' + str(latest_email_uid) + ' with number of  ' + str(
                                    stop - start) +
                                ' of ' + str(number_messages) + ' messages in folder '
                                + folder + ' Error Message:' + str(read_exp))
                            response_part_string = response_part[1].decode('utf-8', errors='ignore')

                        id_message_string = response_part[0].decode('utf-8', errors='ignore')

                        msg = message_from_string(response_part_string, policy=policy.default)
                        msg_list.append(msg)
                        self.logger.debug(folder + ':' + id_message_string)
                except Exception as read_exp:
                    self.logger.exception("Error inside responde_part reading emails in " + folder)

            return msg_list
        except Exception as e:
            self.logger.exception("Error reading emails in " + folder + " counter " + str(counter))
            return msg_list

    # def read_all(self, size_blocks):
    #     msg_list = [ ]
    #     counter = 0
    #     try:
    #         self.logger.debug(self.folder_list)
    #
    #         for folder in self.folder_list:
    #             name_folder = str(folder).split('"')[ -2 ]
    #             if (name_folder.upper() != 'INBOX') and (name_folder.upper() != 'ENVIADO') and (
    #                     name_folder.upper() != 'SENT'):  ## be careful TEMPORAL
    #                 continue
    #             self.logger.info(" mail.select(" + name_folder + ")")
    #             size = self.folder_dict_size[ name_folder ]
    #             start_email = 0
    #             counter = 0
    #             while counter < size:
    #                 msg_list += self.read(start_email, start_email + size_blocks, name_folder)
    #                 counter += len(msg_list)
    #                 start_email += size_blocks
    #         return msg_list
    #     except Exception as e:
    #         self.logger.exception("Global error in read_all " + size_blocks)

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

        FORBIDDEN_TAGS_LIST = FORBIDDEN_TAGS.split(',')
        json_msg = {'parts': []}
        # with this i'm going to eliminate almos all microsoft internal tags.
        for (k, v) in msg.items():
            if any(FORBIDL.upper() in k.upper() for FORBIDL in FORBIDDEN_TAGS_LIST):
                continue
            else:
                if 'Message-Id' in k:
                    if v is not None:
                        json_msg['Message-Id'] = clean_id(v)
                elif 'Message-ID' in k:
                    if v is not None:
                        json_msg['Message-Id'] = clean_id(v)
                elif 'In-Reply-To' in k:
                    if v is not None:
                        json_msg['In-Reply-To'] = clean_id(v)
                elif 'Thread-Index' in k:
                    if v is not None:
                        json_msg['Thread-Index'] = clean_id(v)
                else:
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
                text = unicode(part.get_payload(decode=True).decode('utf-8', 'replace'))
                json_part['content'] = TextProcess.clean_email(text)
            else:
                if part.get_content_type() == 'text/html':
                    html = self.cleanContent(part.get_payload(decode=True))
                    json_part['content'] = TextProcess.clean_email(
                        html)  # .replace('\n', '').replace('\t', '').replace('\r', '')
                else:
                    json_part['content'] = part.get_content_maintype() + ' deleted due the type:{}'.format(
                        part.get_content_type())

            json_msg['parts'].append(json_part)

        # Finally, convert date from asctime to milliseconds since epoch using the
        # $date descriptor so it imports "natively" as an ISODate object in MongoDB.
        try:
            if 'Received' in json_msg:
                if json_msg['Received'] != None:
                    if 'Date' not in json_msg:
                        json_msg['Date'] = json_msg['Received'].split(';')[1]

            if 'Date' in json_msg:
                if json_msg['Date'] != None:
                    date = None

                    # avoid problems with the day of week in text
                    try:
                        date = json_msg['Date'].split(',')[1]
                    except Exception as e:
                        self.logger.debug("error splitting Date :" + str(json_msg['Date']))
                        date = None

                    # if the weekday in text does exist...
                    if date is None or date == '':
                        date = json_msg['Date']

                    then = parse(date)
                    # millis = int(time.mktime(then.timetuple()) * 1000 + then.microsecond / 1000)
                    # json_msg[ 'Date' ] = {'$date': millis}
                    json_msg['Date'] = bson.json_util.dumps(then, default=bson.json_util.default)


        except Exception as e:
            self.logger.error("error parsing Date :" + str(json_msg['Date']) + ' : ' + str(e))

        # The final point is to process the paraphs
        proc = TextProcess()
        proc.clean_json_message(json_msg)

        return json_msg

    # def cleanContentInfo(self,text)
    # function for clean the content of the messages.

    def cleanContent(self, msg):
        # Decode message from "quoted printable" format
        try:
            # text = unicode(msg.decode('utf-8', 'replace'))
            msg = decodestring(msg)
        except Exception as decode_exp:
            self.logger.exception("error in decode email: %s", decode_exp)
            # print(msg)

        # Strip out HTML tags, if any are present.
        # Bail on unknown encodings if errors happen in BeautifulSoup.
        try:
            # text = TextProcess.parse_reply(text)
            soup = BeautifulSoup(msg, "lxml")

        except Exception as e:
            return 'error BeautifulSoup %s'.format(e)
        return ''.join(soup.prettify())

    def processMessage(self, message):
        """
        The processMessage function processes multi-field messages to simplify collection of information
        :param message: pypff.Message object
        :return: A dictionary with message fields (values) and their data (keys)
        """

        message_processed = {}  # {'subject':None,'sender':None,'header':None,'body':None,'From':None,'To':None, 'Cc':None, 'Bcc':None, 'Return-Path':None, 'Date':None,
        # 'Message-Id':None, 'Content-type':None,'Content-Transfer-Encoding':None}

        message_processed["subject"] = message['Subject']
        message_processed["header"] = message._headers

        for (k, v) in message.items():
            for k2 in ['From', 'To', 'Cc', 'Bcc', 'Return-Path', 'Date', 'Message-Id', 'Content-type',
                       'Content-Transfer-Encoding']:
                if not str.lower(k) == str.lower(k2):
                    continue
                message_processed[k] = v

        message_processed["sender"] = message_processed['From']

        for part in message.walk():
            # print('content type: ' + part.get_content_maintype())
            if part.get_content_maintype() == 'multipart':
                continue

            message_processed['contentType'] = part.get_content_type()
            if part.get_content_type() == 'text/plain':
                text = unicode(part.get_payload(decode=True).decode('utf-8', 'replace'))
                text = TextProcess.parse_reply(text)
                message_processed['content'] = str(text)

            else:
                if part.get_content_type() == 'text/html':
                    message_processed['content'] = self.cleanContent(part.get_payload(decode=True))

                else:
                    message_processed[
                        'content'] = 'Content  deleted because it is a content type ' + part.get_content_type()

        # try:
        #    message_processed["creation_time"] = message.creation_time
        # except Exception as creation_time_exp:
        #    logging.warning("creation_time not read"+str(creation_time_exp))
        message_processed["creation_time"] = None
        # tr:
        #    message_processed["submit_time"] = message.client_submit_time
        # except Exception as submit_time_exp:
        #    logging.warning("submit_time not read"+str(submit_time_exp))
        message_processed["submit_time"] = None
        # try:
        #    message_processed["delivery_time"] = message.delivery_time
        # except Exception as delivery_time_exp:
        #    logging.warning("delivery_time not read"+str(delivery_time_exp))
        message_processed["delivery_time"] = None

        if 'Received' in message:
            if message['Received'] != None:
                if 'Date' not in message:
                    message_processed['Date'] = message['Received'].split(';')[1]
                    message_processed["delivery_time"] = message['Received'].split(';')[1]
            else:
                message_processed["delivery_time"] = None
                self.logger.error('Delivery time doesnt exist in %s', str(message))
        message_processed["attachment_count"] = 0  # be careful. not real
        return message_processed

    def checkForMessages(self, start_email_num, finish_email_num, name_folder):
        """
        The checkForMessages function reads folder messages if present and passes them to the report function
        :param folder: pypff.Folder object
        :return: None
        """

        email_list = self.read(start_email_num, finish_email_num, name_folder)
        if email_list is not None:
            if len(email_list) > 0 and self.json_store is not None:
                # folder can have slash in meddle of the name.
                file = self.conx.FROM_EMAIL.split('@')[0] + '.' + self.conx.FROM_EMAIL.split('@')[1] + '.' + \
                       str(start_email_num) + '.' + str(name_folder).replace('/', '.') + '.json'
                self.logger.debug('Read ' + str(len(email_list)) + ' email messages in ' + name_folder)
                jsonified_messages = self.jsonizer_emails(email_list)
                self.logger.debug('prepared ' + str(len(jsonified_messages)) + ' json messages in ' + name_folder)

                if self.json_store.storage_type() == 'file':
                    self.json_store.store(jsonified_messages, self.json_directory, file)
                else:
                    self.json_store.store(jsonified_messages, self.conx.FROM_EMAIL.split('@')[1].split('.')[0])
        return email_list

    def folderReport(self, message_list, name=""):
        """
        The folderReport function generates a report per  folder
        :param message_list: A list of messages discovered during scans
        :folder_name: The name of an Outlook folder within a PST
        :return: None
        """
        if not len(message_list):
            # self.logger.debug("Empty message not processed")
            return
        message_list = Gmail.email_filter(message_list)
        # CSV Report
        fout_path = Gmail.makePath(name + "_report.csv", Gmail.output_directory)
        fout = open(fout_path, 'wb')
        header = ['creation_time', 'submit_time', 'delivery_time', 'sender', 'subject', 'attachment_count']
        csv_fout = csv.DictWriter(fout, fieldnames=header, extrasaction='ignore')
        csv_fout.writeheader()
        csv_fout.writerows(message_list)
        fout.close()

        # HTML Report Prep
        global date_list  # Allow access to edit global variable
        body_out = open(Gmail.makePath(name + "_message_body.txt", Gmail.output_directory), 'a')
        senders_out = open(Gmail.makePath(name + "_senders_names.txt", Gmail.output_directory), 'a')
        for m in message_list:
            if m['content']:
                body_out.write(str(m['content']) + "\n\n")

            if m['sender']:
                senders_out.write(m['sender'] + '\n')
                # if 'Message-Id' in m:
                #     self.logger.debug("writing " + str(m['Message-Id'])+":"+m['sender'])
                # else:
                #     self.logger.debug("writing " + str(m['Message-ID']) + ":" + m['sender'])
            # Creation Time
            if m['creation_time'] != None:
                dateCT = parse(m['creation_time'])
                day_of_week = dateCT.weekday()
                hour_of_day = dateCT.hour + 1
                date_list[day_of_week][hour_of_day] += 1
                if len(name) > 0:
                    self.date_list[day_of_week][hour_of_day] += 1
            # Submit Time
            if m['submit_time'] != None:
                dateST = parse(m['submit_time'])
                day_of_week = dateST.weekday()
                hour_of_day = dateST.hour + 1
                date_list[day_of_week][hour_of_day] += 1
                if len(name) > 0:
                    self.date_list[day_of_week][hour_of_day] += 1
            # Delivery Time
            if m['delivery_time'] != None:
                # date=time.strptime(m[ 'delivery_time' ])
                date = parse(m['delivery_time'])
                day_of_week = date.weekday()
                hour_of_day = date.hour + 1
                date_list[day_of_week][hour_of_day] += 1
                if len(name) > 0:
                    self.date_list[day_of_week][hour_of_day] += 1

        body_out.close()
        senders_out.close()

    def wordStats(self, name=""):
        """
        The wordStats function reads and counts words from a file
        :param raw_file: The path to a file to read
        :return: A list of word frequency counts
        """
        word_list = Counter()
        for line in open(Gmail.makePath(name + "_message_body.txt", Gmail.output_directory), 'r').readlines():
            for word in line.split():
                # Prevent too many false positives/common words
                if word.isalnum() and len(word) > 4:
                    word_list[word] += 1

        return self.wordReport(word_list, name)

    def wordReport(self, word_list, name=""):
        """
        The wordReport function counts a list of words and returns results in a CSV format
        :param word_list: A list of words to iterate through
        :return: None or html_report_list, a list of word frequency counts
        """
        if not word_list:
            self.logger.debug('Message body statistics not available')
            return

        fout = open(Gmail.makePath(name + "_frequent_words.csv", Gmail.output_directory), 'w')
        fout.write('Count,Word\\n')
        for e in word_list.most_common():
            if (len(e) > 1):
                fout.write(str(e[1]) + "," + str(e[0]) + "\n")

        fout.close()

        html_report_list = []
        for e in word_list.most_common(10):
            html_report_list.append({"word": str(e[0]), "count": str(e[1])})

        return html_report_list

    def senderReport(self, name=""):
        """
        The senderReport function reports the most frequent_senders
        :param raw_file: The file to read raw information
        :return: html_report_list, a list of the most frequent senders
        """
        sn = name + "_senders_names.txt"
        sender_list = Counter(open(Gmail.makePath(sn, Gmail.output_directory), 'r').readlines())

        fout = open(Gmail.makePath(sn.split('.')[0] + '.csv', Gmail.output_directory), 'w')
        fout.write("Count,Sender\n")
        for e in sender_list.most_common():
            if len(e) > 1:
                fout.write(str(e[1]) + "," + str(e[0]))
        fout.close()

        html_report_list = []
        for e in sender_list.most_common(5):
            html_report_list.append({"label": str(e[0]), "count": str(e[1])})

        return html_report_list

    def dateReport(self, name=""):
        """
        The dateReport function writes date information in a TSV report. No input args as the filename
        is static within the HTML dashboard
        :return: None
        """
        csv_out = open(Gmail.makePath(name + "_heatmap.tsv", Gmail.output_directory), 'w')
        if name == "":
            dl = date_list
        else:
            dl = self.date_list
        csv_out.write("day\thour\tvalue\n")
        for date, hours_list in enumerate(dl):
            for hour, count in hours_list.items():
                to_write = str(date + 1) + "\t" + str(hour) + "\t" + str(count) + "\n"
                csv_out.write(to_write)
            csv_out.flush()
        csv_out.close()

    def HTMLReport(self, report_title, pst_name, top_words, top_senders):
        """
        The HTMLReport function generates the HTML report from a Jinja2 Template
        :param report_title: A string representing the title of the report
        :param pst_name: A string representing the file pst_name of the user
        :param top_words: A list of the top 10 words
        :param top_senders: A list of the top 10 senders
        :return: None
        """
        open_template = open("stats_template.html", 'r').read()
        html_template = Template(open_template)

        context = {"report_title": report_title, "pst_name": pst_name,
                   "word_frequency": top_words, "percentage_by_sender": top_senders}
        new_html = html_template.render(context)

        html_report_file = open(Gmail.makePath(report_title + "_" + pst_name + ".html", Gmail.output_directory), 'w')
        html_report_file.write(new_html)
        html_report_file.close()

    def summary(self, message_list, name, blocks=100):
        messages = []
        try:

            for m in message_list:
                messages.append(self.processMessage(m))
            self.folderReport(messages, name)
            self.logger.debug("Generating Reports...")
            top_word_list = self.wordStats(name)
            top_sender_list = self.senderReport(name)
            self.dateReport(name)

            self.HTMLReport("Eknowedge_report", name, top_word_list, top_sender_list)
            self.logger.debug(self.folder_list)
        except Exception as e:
            self.logger.error(str(e))

    @staticmethod
    def makePath(file_name, output_directory):
        """
        The makePath function provides an absolute path between the output_directory and a file
        :param file_name: A string representing a file name
        :return: A string representing the path to a specified file
        """
        return os.path.abspath(os.path.join(output_directory, file_name))

    @staticmethod
    def active_log(log_name='email_process.log', level=INFO, log_directory='./logs/'):

        if log_directory:
            if not os.path.exists(log_directory):
                os.makedirs(log_directory)
            log_path = os.path.join(log_directory, log_name)
        else:
            log_path = log_name

        basicConfig(filename=log_path, level=level,
                    format='%(asctime)s | %(levelname)s | %(name)s | %(processName)s | %(message)s', filemode='a')
        logger = getLogger("EmailProcess.Gmail")
        logger.debug('Starting email_process logger using v.' + str(
            __version__) + '  System ' + platform + '  Version ' + version)
        return logger


def summary(message_list):
    try:
        messages = []
        if len(message_list) > 0:
            name = ""
            m = Gmail(email_DUMMY_connexionProperties())
            for message in message_list:
                messages.append(m.processMessage(message))
            m.folderReport(messages, name)
            module_logger.debug("Generating global Report...")
            top_word_list = m.wordStats(name)
            top_sender_list = m.senderReport(name)
            m.dateReport(name)

            m.HTMLReport("Eknowedge_report", name, top_word_list, top_sender_list)
            module_logger.debug('Summary prepared')
        else:
            module_logger.error('No messages for prepare the summary')
    except Exception as e:
        module_logger.exception('Error preparing the global summary. %s', str(e))


FORBIDDEN_FOLDER = {'NOSELECT', 'TRASH', 'JUNK', 'SPAM', 'DRAFT', 'ALL', 'CHATS', 'SCHEDULED', 'MAKED', 'TEMPLATE',
                    'RSS'}
FORBIDDEN_FOLDER_BY_LANGUAGE = {'BORRADORES', 'PAPELERA', 'CORREO ELECTR&APM-NICO NO DESEADO', 'CALENDARIO',
                                'TODOS', 'IMPORTANTES', 'PROGRAMADOS', 'DESTACADOS', 'CONTACTOS', 'NO DESEADO',
                                'ELIMINADOS', 'TASKS', 'TAREAS', 'BORRADORES', 'SINCRONI'}
TEMPORAL_RUN_FOLDER = {'INBOX'}  # , 'SENT', 'ENVIADOS', 'ARCHIVO', 'HISTORY'}


def forbidden(folder):
    # name_folder = str(folder).split('"')[ -2 ]
    name_folder = str(folder).split('"/"')[-1]
    # be careful - with google is not exactly the same. verify.

    name_folder = name_folder.replace('\'', '').replace('"', '').strip()
    # Standards element to leave without read
    words = name_folder.split('/')
    forbidden = False
    for w in words:
        # module_logger.debug('Folder:'+str(w))
        if any(FORBID in w.upper() for FORBID in FORBIDDEN_FOLDER):  # (w.upper() in FORBIDDEN_FOLDER)
            forbidden = True
            # continue
        # by language. BE CAREFUL it must to know the language
        if any(FORBIDL in w.upper() for FORBIDL in
               FORBIDDEN_FOLDER_BY_LANGUAGE):  # (w.upper() in FORBIDDEN_FOLDER_BY_LANGUAGE)
            forbidden = True
            # continue
            # by language. BE CAREFUL it must to know the language
        if ('[' in w.upper()):
            continue
        ########################################################################################
        ## be careful TEMPORAL. just for minimize the number of folders...
        # if any(FORBIDT   not in w.upper()  for FORBIDT in TEMPORAL_RUN_FOLDER):
        #    forbidden = True
        # else:
        #     forbidden = False
        #     #continue
        ########################################################################################
        # module_logger.debug('Folder:' + str(w) + ' :forbidden=' + str(forbidden))
        if forbidden == True:
            return True

    return forbidden


def clean_id(id):
    """clean email id form caracters and text not managed"""
    i = 0

    id = str.replace(str(id), '"', ' ')
    id = str.replace(str(id), '\n', ' ')
    id = str.replace(str(id), '\t', ' ')
    id = id.lstrip()
    id = sub(r'[<>]', '', id)

    return id
