# version v1.1
import smtplib
import time
import imaplib
import email
import re
import os
import sys
import mailbox
import email
import quopri
import json
import time

import EmailProcess
from abc import ABCMeta, abstractmethod
import imaplib
import os


from bs4 import BeautifulSoup
import email
import quopri
import json
import time

from py._xmlgen import unicode

import ConnectionProperties
from dateutil.parser import parse

from neo4j.v1 import GraphDatabase, unicode

class storage_email():
    """abstact class for reading emails from different services"""
    driver = None
    open_db = False
    __metaclass__ = ABCMeta

    def __init__(self, driver):
        self.driver = driver

    @abstractmethod
    def storage_type(self):
        """"Return a string representing the type of connection this is."""
        raise RuntimeError('email_type abstact class')

    @abstractmethod
    def store(self,msg_list):
        """"Return a string representing the type of connection this is."""
        raise RuntimeError('store_email abstact class')

    def __del__(self):
        self.driver = None

    def unify_name(self,name_raw):
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


#driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "Gandalf"))

class neo4j_storage_email(storage_email):
    def __init__(self, driver):
        self.driver = driver

    def storage_type(self):
        """"Return a string representing the type of connection this is."""
        return 'neo4j'

    def connect(self,url,login,pw):
        self.driver=GraphDatabase.driver(url, auth=(login, pw))
        return self.driver
    def __del__(self):
        if self.open_db==True:
            self.driver = self.driver.close()


    @staticmethod
    def define_emailUser_constraint(tx):
        send_text= "CREATE CONSTRAINT ON (user:EmailUser) ASSERT user.username IS UNIQUE  "
        " CREATE CONSTRAINT ON (email:Emai) ASSERT email.id IS UNIQUE"
        tx.run(send_text)

    @staticmethod
    def add_email_to(tx, email_id, content, front, to):
        tx.run("CREATE (email:Email {id:$email_id, content:$content})"
               "MERGE (b:EmailUser {username:$front})"
               " ON CREATE  SET b.username = $front, b.counter=1, b.accessTime = timestamp() "
               " ON MATCH SET b.counter = coalesce(b.counter, 0) + 1,  b.accessTime = timestamp() "
               "MERGE (b)-[:SENT]->(email)"
               "MERGE (c:EmailUser {username:$to})"
               "MERGE (email)-[:TO]->(c)"
               " ON CREATE  SET c.username = $to, c.counter=1, c.accessTime = timestamp() "
               " ON MATCH SET c.counter = coalesce(c.counter, 0) + 1,  c.accessTime = timestamp() "
               "MERGE (b)-[s:SEND]->(c)"
               " ON CREATE  SET s.weith=3 , s.counter=1, s.accessTime = timestamp() "
               " ON MATCH SET s.counter = coalesce(s.counter, 0) + 1, s.weith=coalesce(s.weith, 0) + 3, s.accessTime = timestamp() ",
               email_id=email_id, content=content,front=front, to=to)

    @staticmethod
    def add_email_cc(tx, email_id, content, front, cc):
        tx.run("CREATE (email:Email {id:$email_id, content:$content})"
               "MERGE (b:EmailUser {username:$front})"
               " ON CREATE  SET b.username = $front, b.counter=1, b.accessTime = timestamp() "
               " ON MATCH SET b.counter = coalesce(b.counter, 0) + 1,  b.accessTime = timestamp() "
               "MERGE (b)-[:SENT]->(email)"
               "MERGE (c:EmailUser {username:$cc})"
               "MERGE (email)-[:CC]->(c)"
               " ON CREATE  SET c.username = $cc, c.counter=1, c.accessTime = timestamp() "
               " ON MATCH SET c.counter = coalesce(c.counter, 0) + 1,  c.accessTime = timestamp() "
               "MERGE (b)-[s:SEND]->(c)"
               " ON CREATE  SET s.weith=1 , s.counter=1, s.accessTime = timestamp() "
               " ON MATCH SET s.counter = coalesce(s.counter, 0) + 1, s.weith=coalesce(s.weith, 0) + 1, s.accessTime = timestamp() ",
               email_id=email_id, content=content,front=front, cc=cc)

    @staticmethod
    def add_email_bcc(tx, email_id, content, front, co):
        tx.run("CREATE (email:Email {id:$email_id, content:$content})"
               "MERGE (b:EmailUser {username:$front})"
               " ON CREATE  SET b.username = $front, b.counter=1, b.accessTime = timestamp() "
               " ON MATCH SET b.counter = coalesce(b.counter, 0) + 1,  b.accessTime = timestamp() "
               "MERGE (b)-[:SENT]->(email)"
               "MERGE (c:EmailUser {username:$co})"
               "MERGE (email)-[:BCC]->(c)"
               " ON CREATE  SET c.username = $co, c.counter=1, c.accessTime = timestamp() "
               " ON MATCH SET c.counter = coalesce(c.counter, 0) + 1,  c.accessTime = timestamp() "
               "MERGE (b)-[s:SEND]->(c)"
               " ON CREATE  SET s.weith=4 , s.counter=1, s.accessTime = timestamp() "
               " ON MATCH SET s.counter = coalesce(s.counter, 0) + 1, s.weith=coalesce(s.weith, 0) + 4, s.accessTime = timestamp() ",
               email_id=email_id, content=content,front=front, co=co)

    def store(self,msg_list):
       """Return the tumber of message stored"""
       i=0
       for msg in msg_list:
            try:
                i = i + 1
                email_to=None
                email_cc=None
                email_bcc=None
                if (msg['from'] is None):
                    print('FROM DOESNT EXIST')
                    break
                sender = self.clear_email( msg['from'].split(',')) #msg['from'].split(',')
                email_subject = msg['subject']
                if (email_subject is None):
                    email_subject="NA"
                if not (msg['To'] is None):
                    email_to = self.clear_email(msg['To'].split(','))
                else:
                     if not (msg['Delivered-To'] is None):
                        email_to = self.clear_email(msg['Delivered-To'].split(','))
                     else:
                         print('TO DOESNT EXIST')

                if (msg['Message-Id'] is None):
                    print('Message-Id DOESNT EXIST')
                    return
                email_id=re.sub(r'[<>]', '', msg['Message-Id'])
                if not (msg['CC'] is None):
                    email_cc = self.clear_email(msg['CC'].split(','))
                if not (msg['CO'] is None):
                    email_bcc = self.clear_email( msg['Bcc'].split(','))

                with self.driver.session() as session:
                    session.write_transaction(self.define_emailUser_constraint)
                    for front in sender:
                        for to in email_to:
                            if front!=None and to!=None:
                                session.write_transaction(self.add_email_to, email_id, email_subject, front.lower(), to.lower())
                                print(' Id: ' + email_id + ' From:' +front.lower() + ' TO:' +to.lower())
                        if not (msg['CC'] is None):
                            for cc in email_cc:
                                if front != None and cc != None:
                                    session.write_transaction(self.add_email_cc, email_id, email_subject, front.lower(), cc.lower())
                                    print(' Id: ' + email_id + ' From:' + front.lower() + ' CC:' + cc.lower())
                        if not (msg['Bcc'] is None):
                            for bcc in email_bcc:
                                if front != None and bcc != None:
                                    session.write_transaction(self.add_email_bcc, email_id, email_subject, front.lower(), bcc.lower())
                                    print(' Id: ' + email_id + ' From:' + front.lower() + ' Bcc:' + bcc.lower())

            except Exception as read_exp:
                print(str(read_exp))


       return i



class file_storage_email():
    """abstact class for reading emails from different services"""
    driver = None
    open_db = False
    directory=None
    file_name=None
    filename=None
    initialized=False

    def __init__(self, driver):
        self.driver = driver

    def storage_type(self):
        """"Return a string representing the type of connection this is."""
        return 'file'

    def store_file (self,directory,file_name):
        if self.driver!=None:
            self.driver.close()
        self.directory=directory
        self.file_name=file_name
        self.filename = os.path.join(self.directory, self.file_name)
        self.driver = open(self.filename, 'w')
        self.initialized=True
        return self.driver


    def store(self,jsonified_messages,directory=None,file_name=None ):
        """"store the list of message in file in json format."""

        if directory is not None and file_name is not None:
            self.store_file(directory, file_name)
        elif (self.driver==None):
            raise Exception('error using file descriptor. it has not been initialized')
        self.driver.write(json.dumps(jsonified_messages))
        if self.initialized==True:
            self.driver.close()
        print("Data written out to", self.driver.name)
        return jsonified_messages


    def __del__(self):
        self.driver = None

