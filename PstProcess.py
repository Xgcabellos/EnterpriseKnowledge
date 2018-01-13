# version v1.0

import logging
import os
import pypff
import quopri
import sys
import time
from collections import Counter
from email.parser import HeaderParser

import jinja2
import unicodecsv as csv
from bs4 import BeautifulSoup
from dateutil.parser import parse
from py._xmlgen import unicode

import EmailProcess

__author__ = 'Xavier Garcia Cabellos'
__date__ = '20180101'
__version__ = 0.01
__description__ = 'This scripts handles processing and output of different Email Containers'

from psutil._compat import xrange

date_dict = {x: 0 for x in xrange(1, 25)}
date_list = [date_dict.copy() for x in xrange(7)]


# date_list = [date_dict.copy() for x in xrange(7)]
class pst_file(EmailProcess.abstract_email):
    """ Class for read email from google Mapi"""

    folder_list = []
    folder_dict_size = {}
    folder_dict_data = {}
    status = 0
    message_list = []
    output_directory = ""

    opst = None
    root = None
    report_name = ""
    pst_name = ""

    def __init__(self, pst_file, output_dir, report_name, log_directory=None):
        """
            The main function opens a PST and calls functions to parse and report data from the PST
            :param pst_file: A string representing the path to the PST file to analyze
            :param output_dir: A string representing the path to the PST file to analyze
            :param report_name: Name of the report title (if supplied by the user)
            :return: None
            """

        self.output_directory = os.path.abspath(output_dir)

        if not os.path.exists(self.output_directory):
            os.makedirs(self.output_directory)

        if log_directory:
            if not os.path.exists(log_directory):
                os.makedirs(log_directory)
            log_path = os.path.join(log_directory, 'pst_indexer.log')
        else:
            log_path = 'pst_indexer.log'
        logging.basicConfig(filename=log_path, level=logging.DEBUG,
                            format='%(asctime)s | %(levelname)s | %(message)s', filemode='a')

        logging.info('Starting PST_Indexer v.' + str(__version__))
        logging.debug('System ' + sys.platform)
        logging.debug('Version ' + sys.version)

        logging.info('Starting Script...')

        logging.debug("Opening PST for processing...")
        self.pst_name = os.path.split(pst_file)[1]
        self.report_name = report_name
        self.opst = pypff.open(pst_file)
        self.root = self.opst.get_root_folder()

    def __del__(self):
        """closing the pts connection"""
        logging.info('Script Complete')
        pass

    def read(self, start, stop, folder):

        if folder == "":
            folder = self.root
        logging.debug("Starting traverse of PST structure...")
        self.folderTraverse(folder)
        return self.message_list

    def read_all(self, folder=""):
        self.message_list = []
        counter = 0
        if folder == "":
            folder = self.root
        try:
            logging.debug("Starting traverse of PST structure...")
            self.folderTraverse(folder)
            self.summary()
        except Exception as e:
            logging.error(str(e))

    def summary(self):
        try:
            logging.debug("Generating Reports...")
            top_word_list = self.wordStats()
            top_sender_list = self.senderReport()
            self.dateReport()

            self.HTMLReport(self.report_name, self.pst_name, top_word_list, top_sender_list)
            logging.debug(self.folder_list)
        except Exception as e:
            logging.error(str(e))

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

        for k3 in ['To', 'Cc', 'Bcc']:
            if k3 in json_msg:
                json_msg[k3] = json_msg[k3].replace('\n', '').replace('\t', '').replace('\r', '').replace(' ', '')
        json_part = {}

        # print('content type: ' + part.get_content_maintype())
        if 'Content-Type' or 'Content-type' in json_msg:
            try:
                json_part['contentType'] = json_msg['Content-Type']
            except KeyError as ve:
                json_part['contentType'] = json_msg['Content-type']

            if json_part['contentType'] == 'text/plain':
                json_part['content'] = unicode(json_msg['body'])
            else:
                if json_part['contentType'] == 'text/html':
                    json_part['content'] = self.cleanContent(json_msg['body'])
                else:
                    json_part['content'] = json_part['contentType'] + ' deleted for mapping json'

        json_msg['parts'].append(json_part)

        # Finally, convert date from asctime to milliseconds since epoch using the
        # $date descriptor so it imports "natively" as an ISODate object in MongoDB.
        try:
            if json_msg['Date'] != None:
                then = parse(json_msg['Date'])
                millis = int(time.mktime(then.timetuple()) * 1000 + then.microsecond / 1000)
                json_msg['Date'] = {'$date': millis}
        except logging.error("error parsing Date :" + json_msg['Date'] + " Error:" + str(e)) as e:
            logging.error("error parsing Date :" + json_msg['Date'] + " Error:" + str(e))

        for k2 in ['sender', 'header', 'body', 'Content-Type']:

            try:
                del json_msg[k2]
            except KeyError as ke:
                logging.debug("Deleting.  Error:" + str(ke))
                del json_msg['Content-type']

        return json_msg

    def cleanContent(self, msg):
        # Decode message from "quoted printable" format
        try:
            msg = quopri.decodestring(msg)
        except Exception as decode_exp:
            logging.warning("error in decode email msg:" + str(decode_exp))
            # print(msg)

        # Strip out HTML tags, if any are present.
        # Bail on unknown encodings if errors happen in BeautifulSoup.
        try:
            soup = BeautifulSoup(msg, "lxml")
        except:
            return 'error BeautifulSoup'
        return ''.join(soup.findAll(text=True))

    def makePath(self, file_name):
        """
        The makePath function provides an absolute path between the output_directory and a file
        :param file_name: A string representing a file name
        :return: A string representing the path to a specified file
        """
        return os.path.abspath(os.path.join(self.output_directory, file_name))

    def folderTraverse(self, base):
        """
        The folderTraverse function walks through the base of the folder and scans for sub-folders and messages
        :param base: Base folder to scan for new items within the folder.
        :return: None
        """
        for folder in base.sub_folders:
            if folder.number_of_sub_folders:
                self.folderTraverse(folder)  # Call new folder to traverse:
            self.checkForMessages(folder)

    def checkForMessages(self, folder):
        """
        The checkForMessages function reads folder messages if present and passes them to the report function
        :param folder: pypff.Folder object
        :return: None
        """
        logging.debug("Processing Folder: " + str(folder.name))
        # message_list = []
        for message in folder.sub_messages:
            message_dict = self.processMessage(message)
            if not message_dict == None:
                self.message_list.append(message_dict)
        self.folderReport(self.message_list, folder.name)

    def processMessage(self, message):
        """
        The processMessage function processes multi-field messages to simplify collection of information
        :param message: pypff.Message object
        :return: A dictionary with message fields (values) and their data (keys)
        """
        message_processed = {}
        message_processed["Subject"] = message.subject
        message_processed["sender"] = message.sender_name
        message_processed["header"] = message.transport_headers
        if message.transport_headers == None:
            return None
        message_processed["body"] = message.plain_text_body
        if not message.transport_headers is None:
            parser = HeaderParser()
            msg = parser.parsestr(message.transport_headers)
            for (k, v) in msg.items():
                for k2 in ['From', 'To', 'Cc', 'Bcc', 'Return-Path', 'Date', 'Message-Id', 'Content-type',
                           'Content-Transfer-Encoding']:
                    if not str.lower(k) == str.lower(k2):
                        continue
                    message_processed[k] = v
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
        message_processed["attachment_count"] = message.number_of_attachments
        return message_processed

    def folderReport(self, message_list, folder_name):
        """
        The folderReport function generates a report per PST folder
        :param message_list: A list of messages discovered during scans
        :folder_name: The name of an Outlook folder within a PST
        :return: None
        """
        if not len(message_list):
            logging.warning("Empty message not processed")
            return

        # CSV Report
        fout_path = self.makePath("folder_report_" + folder_name + ".csv")
        fout = open(fout_path, 'wb')
        header = ['creation_time', 'submit_time', 'delivery_time',
                  'sender', 'subject', 'attachment_count']
        csv_fout = csv.DictWriter(fout, fieldnames=header, extrasaction='ignore')
        csv_fout.writeheader()
        csv_fout.writerows(message_list)
        fout.close()

        # HTML Report Prep
        global date_list  # Allow access to edit global variable
        body_out = open(self.makePath("message_body.txt"), 'a')
        senders_out = open(self.makePath("senders_names.txt"), 'a')
        for m in message_list:
            if m['body']:
                body_out.write(str(m['body']) + "\n\n")
            if m['sender']:
                senders_out.write(m['sender'] + '\n')
            # Creation Time
            if m['creation_time'] != None:
                day_of_week = m['creation_time'].weekday()
                hour_of_day = m['creation_time'].hour + 1
                date_list[day_of_week][hour_of_day] += 1
            # Submit Time
            if m['submit_time'] != None:
                day_of_week = m['submit_time'].weekday()
                hour_of_day = m['submit_time'].hour + 1
                date_list[day_of_week][hour_of_day] += 1
            # Delivery Time
            if m['delivery_time'] != None:
                day_of_week = m['delivery_time'].weekday()
                hour_of_day = m['delivery_time'].hour + 1
                date_list[day_of_week][hour_of_day] += 1
        body_out.close()
        senders_out.close()

    def wordStats(self, raw_file="message_body.txt"):
        """
        The wordStats function reads and counts words from a file
        :param raw_file: The path to a file to read
        :return: A list of word frequency counts
        """
        word_list = Counter()
        for line in open(self.makePath(raw_file), 'r').readlines():
            for word in line.split():
                # Prevent too many false positives/common words
                if word.isalnum() and len(word) > 4:
                    word_list[word] += 1
        return self.wordReport(word_list)

    def wordReport(self, word_list):
        """
        The wordReport function counts a list of words and returns results in a CSV format
        :param word_list: A list of words to iterate through
        :return: None or html_report_list, a list of word frequency counts
        """
        if not word_list:
            logging.debug('Message body statistics not available')
            return

        fout = open(self.makePath("frequent_words.csv"), 'w')
        fout.write('Count,Word')
        for e in word_list.most_common():
            if (len(e) > 1):
                fout.write(str(e[1]) + "," + str(e[0]) + "\n")

        fout.close()

        html_report_list = []
        for e in word_list.most_common(10):
            html_report_list.append({"word": str(e[0]), "count": str(e[1])})

        return html_report_list

    def senderReport(self, raw_file="senders_names.txt"):
        """
        The senderReport function reports the most frequent_senders
        :param raw_file: The file to read raw information
        :return: html_report_list, a list of the most frequent senders
        """
        sender_list = Counter(open(self.makePath(raw_file), 'r').readlines())

        fout = open(self.makePath("frequent_senders.csv"), 'w')
        fout.write("Count,Sender\n")
        for e in sender_list.most_common():
            if len(e) > 1:
                fout.write(str(e[1]) + "," + str(e[0]))
        fout.close()

        html_report_list = []
        for e in sender_list.most_common(5):
            html_report_list.append({"label": str(e[0]), "count": str(e[1])})

        return html_report_list

    def dateReport(self):
        """
        The dateReport function writes date information in a TSV report. No input args as the filename
        is static within the HTML dashboard
        :return: None
        """
        csv_out = open(self.makePath("heatmap.tsv"), 'w')
        csv_out.write("day\thour\tvalue\n")
        for date, hours_list in enumerate(date_list):
            for hour, count in hours_list.items():
                to_write = str(date + 1) + "\t" + str(hour) + "\t" + str(count) + "\n"
                csv_out.write(to_write)
            csv_out.flush()
        csv_out.close()

    def HTMLReport(self, report_title, pst_name, top_words, top_senders):
        """
        The HTMLReport function generates the HTML report from a Jinja2 Template
        :param report_title: A string representing the title of the report
        :param pst_name: A string representing the file name of the PST
        :param top_words: A list of the top 10 words
        :param top_senders: A list of the top 10 senders
        :return: None
        """
        open_template = open("stats_template.html", 'r').read()
        html_template = jinja2.Template(open_template)

        context = {"report_title": report_title, "pst_name": pst_name,
                   "word_frequency": top_words, "percentage_by_sender": top_senders}
        new_html = html_template.render(context)

        html_report_file = open(self.makePath(report_title + ".html"), 'w')
        html_report_file.write(new_html)
        html_report_file.close()
