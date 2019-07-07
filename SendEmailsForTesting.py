#
#pm install -g maildev # Utilisez sudo si nécessaire
#maildev

#######
# Notas: # html2text.html2text(html) para pasar de html a text


import sys
from EmployeeEmailWorker import emailworker, message_html, read_knowlege_info, read_users_info
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import logging,csv
from typing import Dict, Any, List, Union
from collections import deque
import os

_author__ = 'Xavier Garcia Cabellos'
__date__ = '20190404'
__version__ = 0.01
__description__ = 'this class will send emails'

module_logger = logging.getLogger('Send_Email')
log_name = 'Sending_Emails.log'
log_level=logging.DEBUG

users_file = "../input/users.csv"
books_directory = '../input/books/'
knowledge_file = "../input/knowledges.csv"
receiver_email = "@eknowedge.com"
email_server='192.168.1.108'

smtpObj = smtplib.SMTP(email_server, 25,'xgarcia_latitude')
smtpObj.login('xgarcia',"Gandalf6981")

def send_emails(ist_ofmessage_proceesed,smtpObj):
    m_error="None"
    try:

        #smtpObj = smtplib.SMTP('192.168.1.110', 25)
        for m in list_ofmessage_proceesd:
            message = MIMEMultipart("alternative")
            if m.answer:
                message[ "Subject" ] = 'Re:'+m.title
            else:
                message["Subject"] =m.title
            message["From"] =m.send
            message["To"] = m.to
            message[ "Cc" ]=m.cc
            message[ "Co" ]= m.co


            # Create HTML version of your message
            html = str(m.html)
            # Turn these into plain/html MIMEText objects
            part1 = MIMEText(html, "html")
            # The email client will try to render the last part first
            message.attach(part1)
           # message.attach(part2)
            m_error=m
            smtpObj.sendmail(m.send, m.to, message.as_string())
            module_logger.debug(m)
    except Exception as SMTPException:
        module_logger.error( "Error: unable to send email from: "+m_error.send+" to: "+m_error.to+ " %s, \\n %s " , SMTPException,m_error)

def active_log(log_name='process.log', level=logging.INFO, log_directory='./logs/'):

    if log_directory:
        if not os.path.exists(log_directory):
            os.makedirs(log_directory)
        log_path = os.path.join(log_directory, log_name)
    else:
        log_path = log_name
    logging.basicConfig(filename=log_path, level=level,
                        format='%(asctime)s | %(levelname)s | %(message)s', filemode='a')

    logging.info('Starting mail_process logger using v.' + str(__version__)+' System ' + sys.platform+' Version ' + sys.version)


active_log(log_name,log_level)
usersinfo = read_users_info(users_file)
list_workers: List[ Any ] = [ ]
knowledge = read_knowlege_info(4, books_directory, knowledge_file)
number_of_messages_prepared=0
for know in knowledge:
    list_ofmessage_proceesd = [ ]
    knowledge_name = know[ 'knowledge' ]
    list_ofmessage = deque()
    module_logger.debug('starting knowledge: ' + str(knowledge_name) )
    listofbooks = know[ 'books' ]
    for book in listofbooks:
        title = book[ 'title' ]
        htmls = book[ 'htmls' ]
        module_logger.debug('starting book: ' + str(title) )
        for html in htmls:
            list_ofmessage.append(message_html(title, html))
            number_of_messages_prepared+=1
        a = emailworker(usersinfo, knowledge_name, list_ofmessage, list_workers, "user2_0_STEERING@eknowedge.com")
        list_ofmessage_proceesd.extend(a.processor(usersinfo, knowledge_name))
        module_logger.debug('Finish book: ' + title+'. Messages processed: '+str(len(list_ofmessage_proceesd)) + ' messages of the ' + str(
            number_of_messages_prepared) + ' prepared')

    try:
        module_logger.debug('Finish. send ' + str(len(list_ofmessage_proceesd)) + ' messages of the '+str(number_of_messages_prepared)+' prepared')
        send_emails(list_ofmessage_proceesd,smtpObj)

    except Exception as BookException:
        logging.exception( "Error: problems reading the book")
        exit(-1)