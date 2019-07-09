import logging, csv
from typing import Dict, Any, List, Union
import os

import random

import EbookProcess
import sys
import pandas as pd
from collections import deque
import numpy as np

_author__ = 'Xavier Garcia Cabellos'
__date__ = '20190326'
__version__ = 0.01
__description__ = 'this class will create a message to dend'

#######
# Notas: # html2text.html2text(html) para pasar de html a text

module_logger = logging.getLogger('EmployeeEmailworker')
log_name = 'Sending_Emails.log'
log_level=logging.INFO

class message_html:
    title = None
    html = None
    type = None
    send = None
    to = None
    cc = None
    fw = None
    co = None
    answer = None

    def __init__(self, title, html, type=None):
        self.title = title
        self.html = html
        self.type = type

    def __str__(self):
        return 'title:' + str(self.title) + ', html:' + str(len(self.html)) + '. send:' + str(
            self.send) + ', to:' + str(self.to) + ', cc:' + str(self.cc) + ' co:' + str(self.co) + ' fw:' + str(
            self.fw) + ' answer:' + str(self.answer)


class emailworker:
    _messages_send = None
    _messages_cc = None
    _messages_fw = None
    _messages_co = None
    _messages_answer = None
    list_ofmessage_processed =None
    list_ofmessage = None
    list_workers = None
    know = None
    max_number_of_chats = 3
    message_by_chat=4
    total_chats_by_users=6
    index=0
    level=log_level
    log_name=log_name

    def __init__(self, allusersinfo, know, list_ofmessage, list_workers, name, log_directory="./output/"):

        self.logger = self.active_log(self.log_name , self.level ,log_directory)



        try:
            # allusersinfo[ allusersinfo[ 'mail' ].str.match(name) ]
            userRow = allusersinfo[ allusersinfo[ 'email' ].str.match(name) ].iloc[ 0 ]
            self.know = know
            self.user = userRow[ 'user' ]
            self.level = userRow[ 'level' ]
            self.department = userRow[ 'department' ]
            self.knowledge = userRow[ 'knowledge' ]
            self.knowledge2 = userRow[ 'knowledge2' ]
            self.knowledge3 = userRow[ 'knowledge3' ]
            self.structured_name = userRow[ 'sername' ]
            self.email = userRow[ 'email' ]
            assert self.email == name
            self.list_ofmessage = list_ofmessage
            self.list_workers = list_workers
            # self.logger.debug((self.user, self.level, self.department, self.knowledge, self.knowledge2, self.knowledge3,
            #                  self.structured_name, self.email))
            list_workers.append(self)
            self.list_ofmessage_processed=[]
        except Exception as emailworkerException:
            logging.error("Error looking for info of " + name + ". Error reading data:  %s",emailworkerException)

    def processor(self, allusersinfo, know, index=0,list_ofmessage=None, list_workers=None):
        if list_workers is None:
            list_workers = self.list_workers
        if list_ofmessage is None:
            list_ofmessage = self.list_ofmessage
        array = [ know ]
        lastLevel = False
        self.index=index
        userslevel1low_with_knowledge = False
       # self.logger.debug((self.user, self.level, self.department, self.knowledge, self.knowledge2, self.knowledge3,
       #                    self.structured_name, self.email, self.max_number_of_chats))

        userslevel1low = allusersinfo.loc[
            (allusersinfo[ 'level' ] == self.level - 1) & (allusersinfo[ 'knowledge' ].isin(array))  ]

        if (userslevel1low is None or len(userslevel1low.index) == 0):
            userslevel1low = allusersinfo.loc[ (allusersinfo[ 'level' ] == self.level - 1) ]
            if (userslevel1low is None or len(userslevel1low.index) == 0):
                lastLevel = True
        else:
            userslevel1low_with_knowledge = True
        userssamelevel = allusersinfo.loc[
            (allusersinfo[ 'level' ] == self.level) & (allusersinfo[ 'knowledge' ].isin(array))]
        if (len(userssamelevel.index) <= 1):
            if not lastLevel:
                userssamelevel = allusersinfo.loc[ (allusersinfo[ 'level' ] == self.level) ]
            else:
                return self.list_ofmessage_processed


        chats = self.message_by_chat
        total_chats = self.total_chats_by_users
        nobody_know = len(userslevel1low.index)


        if self.max_number_of_chats > 0:
            if self.knowledge == know or (nobody_know > 0 and userslevel1low_with_knowledge):

                count = len(list_ofmessage)
                if nobody_know > 0:
                    for index, row_level1 in userslevel1low.iterrows():
                        if count>0:
                            if (row_level1[ 'knowledge' ] == know ) :
                                for index2, row_same_level in userssamelevel.iterrows():
                                    if row_same_level[ 'email' ] == self.email or count==0:
                                        continue
                                    else:
                                        list_ofmessage2 = deque()
                                        if count > chats:
                                            if (count % chats) != 0:
                                                for i in range(0, chats):
                                                    list_ofmessage2.append(list_ofmessage.pop())
                                                    count -= 1
                                            else:
                                                count -= 1
                                                continue

                                        else:
                                            for i in range(0, count):
                                                list_ofmessage2.append(list_ofmessage.pop())
                                                count -= 1
                                        self.make_conversation(list_ofmessage2, self.email, row_level1[ 'email' ],
                                                               row_same_level[ 'email' ], row_same_level[ 'email' ],
                                                               row_same_level[ 'email' ])
                                        self.list_ofmessage_processed.extend(list_ofmessage2)
                                        self.logger.debug('elements in list of message processed: Count '+str(count)+' row_level1 '+ str(len(self.list_ofmessage_processed))+ ' of: '+row_level1[ 'email' ]+' and '+row_same_level[ 'email' ] + ' in the object of '+self.email)
                                        # for m in list_ofmessage2:
                                        #     self.logger.debug('1--->'+str(m))
                                        total_chats -= 1
                                        self.max_number_of_chats -= 1
                                        if count == 0 or total_chats == 0 or self.max_number_of_chats == 0:
                                            #nobody_know = 1
                                            break
                            else:
                                nobody_know -= 1
                            # self.logger.debug('I dont know about it:' +row_level1[ 'knowledge' ])
                        else:
                            return self.list_ofmessage_processed
                else:
                    return self.list_ofmessage_processed

            if nobody_know == 0 and userslevel1low_with_knowledge == False and len(list_ofmessage)!=0:
                count = len(list_ofmessage)
                for index, row_same_level in userssamelevel.iterrows():
                    if count > 0:
                        if (row_same_level[ 'knowledge' ] == know or row_same_level[ 'knowledge2' ] == know):
                            if row_same_level[ 'email' ] == self.email or count==0:
                                continue
                            else:
                                list_ofmessage2 = deque()
                                if count > chats:
                                    if (count % chats) != 0:
                                        for i in range(0, chats):
                                            list_ofmessage2.append(list_ofmessage.pop())
                                            count -= 1
                                    else:
                                        count -= 1
                                        continue

                                else:
                                    for i in range(0, count):
                                        list_ofmessage2.append(list_ofmessage.pop())
                                        count -= 1
                                self.make_conversation(list_ofmessage2, self.email, row_same_level[ 'email' ], None, None,
                                                       None)
                                self.list_ofmessage_processed.extend(list_ofmessage2)
                                self.logger.debug(
                                    'elements in list of message processed: Count '+str(count)+' row_same_level ' + str(len(self.list_ofmessage_processed))+ ' of: '+row_same_level[ 'email' ] + ' in the object of '+self.email)
                                # for m in list_ofmessage2:
                                #     self.logger.debug('2--->'+str(m))
                                total_chats -= 1
                                self.max_number_of_chats -= 1
                                if count == 0 or total_chats == 0 or self.max_number_of_chats == 0:
                                    #nobody_know = 1
                                    break
                        else:
                            nobody_know -= 1
                    else:
                        return self.list_ofmessage_processed
        # else:
        #     return []

        # end if kow=knowledge
        size = len(list_ofmessage)
        if size != 0:
            self.logger.debug('List_of Message: ' + str(size) + ' messages of ' + know + ' of: '+self.email)
            if lastLevel or nobody_know == 0:
                for index, row_same_level in userssamelevel.iterrows():
                    self.logger.debug(
                        'going to new object: userssamelevel ' + str(
                            len(self.list_ofmessage_processed)) + ' nobody_know ' + str(nobody_know) + ' of: ' +
                        row_same_level[ 'email' ] + ' in the object of ' + self.email)
                    if not (row_same_level[ 'email' ] == self.email) and not (index<self.index):
                        if len(list_ofmessage)>0:
                            self.list_ofmessage_processed.extend(emailworker(allusersinfo, know, list_ofmessage, list_workers,row_same_level[ 'email' ]).processor(allusersinfo, know,index))
                            self.logger.debug(
                                'elements in list of message processed: userssamelevel ' + str(len(self.list_ofmessage_processed))+' nobody_know '+str(nobody_know)+ ' of: '+row_same_level[ 'email' ] + ' in the object of '+self.email)
                            # for m in self.list_ofmessage_processed:
                            #     module_logger.debug('3--->'+str(m))
                        else:
                            return self.list_ofmessage_processed
                return self.list_ofmessage_processed
            else:
                for index, row_level1 in userslevel1low.iterrows():
                    if (len(list_ofmessage) > 0 and index>=self.index):
                        self.list_ofmessage_processed.extend(emailworker(allusersinfo, know, list_ofmessage, list_workers, row_level1[ 'email' ]).processor(allusersinfo, know,index))
                        self.logger.debug(
                            'elements in list of message processed: userlevel1low ' + str(len(self.list_ofmessage_processed)) + ' of: '+row_level1[ 'email' ] + ' in the object of '+self.email)
                        # for m in self.list_ofmessage_processed:
                        #     module_logger.debug('4--->'+str(m))
                    else:
                        return self.list_ofmessage_processed
                return self.list_ofmessage_processed
        # else:
        #     self.logger.debug('knowledge:' + know + ' finished')
        return self.list_ofmessage_processed

    def make_conversation(self, list_ofmessages, origen, to, cc, co, fw):
        count = 0;

        _to = None
        _cc = None
        _fw = None
        _co = None
        answer = None
        try:
            if list_ofmessages is not None:
                for message in list_ofmessages:
                    if not (count % 2):
                        message.send = origen
                        message.to = to
                        message.answer = False

                    else:
                        message.send = to
                        message.to = origen
                        message.answer = True
                    count += 1
                    if _cc is None and cc is not None:
                        if random.randrange(1, 100) >= 80:
                            message.cc = cc
                            _cc = cc
                        else:
                            if _co is None and co is not None:
                                if random.randrange(1, 100) > 90:
                                    message.co = co
                                    _co = co
                                else:
                                    if _fw is None and cc is not None:
                                        if random.randrange(1, 100) > 80:
                                            message.fw = fw
                                            _fw = fw
                                    else:
                                        message.fw = _fw

                            else:
                                message.co = _co
                    else:
                        message.cc = _cc
                    #self.logger.debug(message)
        except Exception as emailworkerException:
            logging.exception("Error preparing messages for " + origen + ". Error reading data  ")
        return list_ofmessages

    def active_log(self, log_name='ebook_process.log', level=logging.INFO, log_directory='./logs/'):

        if log_directory:
            if not os.path.exists(log_directory):
                os.makedirs(log_directory)
            log_path = os.path.join(log_directory, log_name)
        else:
            log_path = log_name

        logging.basicConfig(filename=log_path, level=level,
                            format='%(asctime)s | %(levelname)s | %(name)s | %(message)s', filemode='a')

        self.logger = logging.getLogger("EmployeeEmailworker.emailworker")
        self.logger.debug('Starting emailWorker logger using v.' + str(__version__)+ ' System ' + sys.platform +' Version ' + sys.version )
        return  self.logger

users_file = "../input/users.csv"
books_directory = '../input/books/'
knowledge_file = "../input/knowledges.csv"


def read_users_info(users_file=users_file):
    _infousers = pd.read_csv(users_file)

    module_logger.debug((_infousers.tail(10)))

    #
    # with open(users_file) as file:
    #     reader = csv.reader(file)
    #     next(reader)  # Skip header row
    #     for user, Level, Department, knowledge, knowledge2, knowledge3, sername, email in reader:
    #         infouser = [ user, Level, Department, knowledge, knowledge2, knowledge3, sername, email ]
    #         _infousers.append(infouser)
    #

    return _infousers


def read_knowlege_info(paragraphs_by_html, books_directory=books_directory, knowledge_file=knowledge_file):
    # books readed
    books = [ ]

    # kind of knowledges
    _knowledge = [ ]

    # dictionary knowledge->books associated.
    _knowledge_books: List[ Dict[ str, Any ] ] = [ ]

    real_books_directory = os.path.abspath(books_directory)
    if not os.path.exists(real_books_directory):
        raise ValueError("the book or the directory of the bok doesn't exist:", real_books_directory)

    with open(knowledge_file) as file:
        reader = csv.reader(file)
        next(reader)  # Skip header row
        for knowledge1, books in reader:
            books_list_file = books.split('|')
            books_list: List[ Dict[ str, Any ] ] = [ ]
            for book_file in books_list_file:
                try:
                    epubbook = EbookProcess.ebook(real_books_directory + "/" + book_file)
                    htmls: List[ object ] = epubbook.create_multiple_htmls(paragraphs_by_html)
                    book_html: Dict[ str, Any ] = {'title': str(epubbook.title), 'htmls': htmls}
                    books_list.append(book_html)
                except Exception as BookException:
                    logging.exception("Error reading " + real_books_directory + "/" + book_file)

            info: Dict[ str, Any ] = {'knowledge': knowledge1, 'books': books_list}
            _knowledge_books.append(info)
            books_list = None
    return _knowledge_books


# list_ofmessage_proceesd = [ ]
# usersinfo = read_users_info(users_file)
# list_workers: List[ Any ] = [ ]
# knowledge = read_knowlege_info(4, books_directory, knowledge_file)
# number_of_messages_prepared=0
# for know in knowledge:
#     knowledge_name = know[ 'knowledge' ]
#     list_ofmessage = deque()
#     module_logger.debug('starting knowledge: ' + str(knowledge_name) )
#     listofbooks = know[ 'books' ]
#     for book in listofbooks:
#         title = str(book[ 'title' ]).split('/')[0]
#         htmls = book[ 'htmls' ]
#         module_logger.debug('starting book: ' + str(title) )
#         for html in htmls:
#             list_ofmessage.append(message_html(title, html))
#             number_of_messages_prepared+=1
#         a = emailworker(usersinfo, knowledge_name, list_ofmessage, list_workers, "user2_0_STERNCO@eknowedge.com")
#         list_ofmessage_proceesd.extend(a.processor(usersinfo, knowledge_name))
#         module_logger.debug('Finish book: ' + title+'. Messages proccesed: '+str(len(list_ofmessage_proceesd)) + ' messages of the ' + str(
#             number_of_messages_prepared) + ' prepared')
#
# module_logger.debug('Finish. send ' + str(len(list_ofmessage_proceesd)) + ' messages of the '+str(number_of_messages_prepared)+' prepared')
#
# for m in list_ofmessage_proceesd:
#     module_logger.debug(m)
