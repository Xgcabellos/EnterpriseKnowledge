# version v1.1
import configparser
from logging import getLogger

import neotime
from dateutil.parser import parse
from neo4j.v1 import GraphDatabase

from EmailProcess import clean_id
from StorageEmail import storage_email
from TextProcess import log_level_conversor

_author__ = 'Xavier Garcia Cabellos'
__date__ = '20180101'
__version__ = 0.01
__description__ = 'This scripts write emails in datasource Neo4J'

config_name = '../input/config.ini'

config = configparser.ConfigParser()
config.read(config_name)

log_name = config[ 'LOGS' ][ 'LOG_FILE' ]
log_directory = config[ 'LOGS' ][ 'LOG_DIRECTORY' ]
log_level = log_level_conversor(config[ 'LOGS' ][ 'log_level_graph' ])

module_logger = getLogger('Neo4jStorageEmail')
# driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "Gandalf"))

class neo4j_storage_email(storage_email):
    def __init__(self,driver,log_file,log_level,log_directory):
        self.driver = driver
        log = getLogger("neo4j.bolt")
        log.setLevel(log_level_conversor(config[ 'LOGS' ][ 'log_level_neo4j' ]))
        self.logger =self.active_log(log_file,log_level, log_directory)

    def storage_type(self):
        """"Return a string representing the type of connection this is."""
        return 'neo4j'

    def connect(self, url, login, pw):
        self.driver = GraphDatabase.driver(url, auth=(login, pw))
        self.self.open_db = True
        self.logger = self.active_log(log_name, log_level, log_directory)
        return self.driver

    def __del__(self):
        if self.open_db == True:
            self.driver = self.driver.close()

    @staticmethod
    def define_emailUser_constraint(tx):
        send_text = "CREATE CONSTRAINT ON (user:User) ASSERT user.username IS UNIQUE  "
        " CREATE CONSTRAINT ON (email:Email) ASSERT email.id IS UNIQUE"
        tx.run(send_text)

    @staticmethod
    def add_email_to(tx, email_id, content, _from, to, email_date, thread_index='', thread_topic=''):
        tx.run("MERGE (email:Email {id:$email_id})"
               " ON CREATE  SET email.id=$email_id,email.content= $content, email.thread_index= $thread_index,"
               " email.thread_topic= $thread_topic, email.email_date=$email_date "
               " ON MATCH SET  email.content = $content, email.thread_index= $thread_index,"
               " email.thread_topic= $thread_topic, email.email_date=$email_date "
               "MERGE (b:User {username:$_from})"
               " ON CREATE  SET b.username = $_from, b.counter=1, b.accessTime = timestamp() "
               " ON MATCH SET b.counter = coalesce(b.counter, 0) + 1,  b.accessTime = timestamp() "
               "MERGE (b)-[:FROM]->(email)"
               "MERGE (c:User {username:$to})"
               "MERGE (email)-[:TO]->(c)"
               " ON CREATE  SET c.username = $to, c.counter=1, c.accessTime = timestamp() "
               " ON MATCH SET c.counter = coalesce(c.counter, 0) + 1,  c.accessTime = timestamp() "
               "MERGE (b)-[s:SEND]->(c)"
               " ON CREATE  SET s.weight=3 , s.counter=1, s.accessTime = timestamp() "
               " ON MATCH SET s.counter = coalesce(s.counter, 0) + 1, s.weight=coalesce(s.weight, 0) + 3,"
               " s.accessTime = timestamp() ",
               email_id=email_id, content=content, _from=_from, to=to, email_date=email_date, thread_index=thread_index,
               thread_topic=thread_topic)

    @staticmethod
    def add_email_cc(tx, email_id, content, _from, cc, email_date, thread_index='', thread_topic=''):
        tx.run("MERGE (email:Email {id:$email_id})"
               " ON CREATE  SET email.id=$email_id,email.content= $content, email.thread_index= $thread_index,"
               " email.thread_topic= $thread_topic, email.email_date=$email_date "
               " ON MATCH SET  email.content = $content, email.thread_index= $thread_index,"
               " email.thread_topic= $thread_topic, email.email_date=$email_date "
               "MERGE (b:User {username:$_from})"
               " ON CREATE  SET b.username = $_from, b.counter=1, b.accessTime = timestamp() "
               " ON MATCH SET b.counter = coalesce(b.counter, 0) + 1,  b.accessTime = timestamp() "
               "MERGE (b)-[:FROM]->(email)"
               "MERGE (c:User {username:$cc})"
               "MERGE (email)-[:CC]->(c)"
               " ON CREATE  SET c.username = $cc, c.counter=1, c.accessTime = timestamp() "
               " ON MATCH SET c.counter = coalesce(c.counter, 0) + 1,  c.accessTime = timestamp() "
               "MERGE (b)-[s:SEND]->(c)"
               " ON CREATE  SET s.weight=1 , s.counter=1, s.accessTime = timestamp() "
               " ON MATCH SET s.counter = coalesce(s.counter, 0) + 1, s.weight=coalesce(s.weight, 0) + 1,"
               " s.accessTime = timestamp() ",
               email_id=email_id, content=content, _from=_from, cc=cc, email_date=email_date, thread_index=thread_index,
               thread_topic=thread_topic)

    @staticmethod
    def add_email_bcc(tx, email_id, content, _from, co, email_date, thread_index='', thread_topic=''):
        tx.run("MERGE (email:Email {id:$email_id})"
               " ON CREATE  SET email.id=$email_id,email.content= $content, email.thread_index= $thread_index,"
               " email.thread_topic= $thread_topic, email.email_date=$email_date "
               " ON MATCH SET  email.content = $content, email.thread_index= $thread_index,"
               " email.thread_topic= $thread_topic, email.email_date=$email_date "
               "MERGE (b:User {username:$_from})"
               " ON CREATE  SET b.username = $_from, b.counter=1, b.accessTime = timestamp() "
               " ON MATCH SET b.counter = coalesce(b.counter, 0) + 1,  b.accessTime = timestamp() "
               "MERGE (b)-[:FROM]->(email)"
               "MERGE (c:User {username:$co})"
               "MERGE (email)-[:BCC]->(c)"
               " ON CREATE  SET c.username = $co, c.counter=1, c.accessTime = timestamp() "
               " ON MATCH SET c.counter = coalesce(c.counter, 0) + 1,  c.accessTime = timestamp() "
               "MERGE (b)-[s:SEND]->(c)"
               " ON CREATE  SET s.weight=4 , s.counter=1, s.accessTime = timestamp() "
               " ON MATCH SET s.counter = coalesce(s.counter, 0) + 1, s.weight=coalesce(s.weight, 0) + 4,"
               " s.accessTime = timestamp() ",
               email_id=email_id, content=content, _from=_from, co=co, email_date=email_date, thread_index=thread_index,
               thread_topic=thread_topic)

    @staticmethod
    def add_email_to_reply(tx, email_id, content, _from, to, email_date, reply_id, thread_index='', thread_topic=''):
        tx.run("MERGE (email:Email {id:$email_id})"
               " ON CREATE  SET email.id=$email_id,email.content= $content, email.thread_index= $thread_index,"
               " email.thread_topic= $thread_topic, email.email_date=$email_date "
               " ON MATCH SET  email.content = $content, email.thread_index= $thread_index,"
               " email.thread_topic= $thread_topic, email.email_date=$email_date "
               "MERGE (b:User {username:$_from})"
               " ON CREATE  SET b.username = $_from, b.counter=1, b.accessTime = timestamp() "
               " ON MATCH SET b.counter = coalesce(b.counter, 0) + 1,  b.accessTime = timestamp() "
               "MERGE (b)-[:FROM]->(email) "
               "MERGE (c:User {username:$to}) "
               "MERGE (email)-[:TO]->(c)"
               " ON CREATE  SET c.username = $to, c.counter=1, c.accessTime = timestamp() "
               " ON MATCH SET c.counter = coalesce(c.counter, 0) + 1,  c.accessTime = timestamp() "
               "MERGE (b)-[s:SEND]->(c) "
               " ON CREATE  SET s.weight=3 , s.counter=1, s.accessTime = timestamp() "
               "MERGE (email2:Email {id:$reply_id}) "
               "ON CREATE  SET email2.id=$reply_id,email2.content=$thread_topic, email2.thread_index= $thread_index,"
               " email2.thread_topic= $thread_topic , email2.email_date=$email_date "
               "ON MATCH SET email2.thread_index= $thread_index, email2.thread_topic= $thread_topic "
               "MERGE (email)-[r:REPLY_OF]->(email2)"
               " ON CREATE  SET r.weight= 1 , r.counter=1, r.accessTime = timestamp() ,"
               " r.thread_index = $thread_index, r.thread_topic = $thread_topic"
               " ON MATCH SET r.counter = coalesce(r.counter, 0) + 1, r.weight=coalesce(r.weight, 0) + 1,"
               " r.accessTime = timestamp(), r.thread_index = $thread_index, r.thread_topic = $thread_topic ",
               email_id=email_id, content=content, _from=_from, to=to, email_date=email_date, reply_id=reply_id,
               thread_index=thread_index, thread_topic=thread_topic)

    @staticmethod
    def add_email_cc_reply(tx, email_id, content, _from, cc, email_date, reply_id, thread_index='', thread_topic=''):
        tx.run("MERGE (email:Email {id:$email_id})"
               " ON CREATE  SET email.id=$email_id,email.content= $content, email.thread_index= $thread_index,"
               " email.thread_topic= $thread_topic, email.email_date=$email_date "
               " ON MATCH SET  email.content = $content, email.thread_index= $thread_index,"
               " email.thread_topic= $thread_topic, email.email_date=$email_date "
               "MERGE (b:User {username:$_from}) "
               " ON CREATE  SET b.username = $_from, b.counter=1, b.accessTime = timestamp() "
               " ON MATCH SET b.counter = coalesce(b.counter, 0) + 1,  b.accessTime = timestamp() "
               "MERGE (b)-[:FROM]->(email) "
               "MERGE (c:User {username:$cc}) "
               "MERGE (email)-[:CC]->(c)"
               " ON CREATE  SET c.username = $cc, c.counter=1, c.accessTime = timestamp() "
               " ON MATCH SET c.counter = coalesce(c.counter, 0) + 1,  c.accessTime = timestamp() "
               "MERGE (b)-[s:SEND]->(c) "
               " ON CREATE  SET s.weight=1 , s.counter=1, s.accessTime = timestamp() "
               "MERGE (email2:Email {id:$reply_id}) "
               "ON CREATE  SET email2.id=$reply_id,email2.content=$thread_topic, email2.thread_index= $thread_index,"
               " email2.thread_topic= $thread_topic, email2.email_date=$email_date "
               "ON MATCH SET email2.thread_index= $thread_index, email2.thread_topic= $thread_topic "
               "MERGE (email)-[r:REPLY_OF]->(email2)"
               " ON CREATE  SET r.weight= 1 , r.counter=1, r.accessTime = timestamp() ,"
               " r.thread_index = $thread_index, r.thread_topic = $thread_topic"
               " ON MATCH SET r.counter = coalesce(r.counter, 0) + 1, r.weight=coalesce(r.weight, 0) + 1,"
               " r.accessTime = timestamp(), r.thread_index = $thread_index, r.thread_topic = $thread_topic ",
               email_id=email_id, content=content, _from=_from, cc=cc, email_date=email_date, reply_id=reply_id,
               thread_index=thread_index, thread_topic=thread_topic)

    @staticmethod
    def add_email_bcc_reply(tx, email_id, content, _from, co, email_date, reply_id, thread_index='', thread_topic=''):
        tx.run("MERGE (email:Email {id:$email_id})"
               " ON CREATE  SET email.id=$email_id,email.content= $content, email.thread_index= $thread_index,"
               " email.thread_topic= $thread_topic, email.email_date=$email_date "
               " ON MATCH SET  email.content = $content, email.thread_index= $thread_index,"
               " email.thread_topic= $thread_topic, email.email_date=$email_date "
               "MERGE (b:User {username:$_from}) "
               " ON CREATE  SET b.username = $_from, b.counter=1, b.accessTime = timestamp() "
               " ON MATCH SET b.counter = coalesce(b.counter, 0) + 1,  b.accessTime = timestamp() "
               "MERGE (b)-[:FROM]->(email) "
               "MERGE (c:User {username:$co}) "
               "MERGE (email)-[:BCC]->(c) "
               " ON CREATE  SET c.username = $co, c.counter=1, c.accessTime = timestamp() "
               " ON MATCH SET c.counter = coalesce(c.counter, 0) + 1,  c.accessTime = timestamp() "
               "MERGE (b)-[s:SEND]->(c) "
               " ON CREATE  SET s.weight=4 , s.counter=1, s.accessTime = timestamp() "
               "MERGE (email2:Email {id:$reply_id}) "
               "ON CREATE  SET email2.id=$reply_id,email2.content=$thread_topic, email2.thread_index= $thread_index,"
               " email2.thread_topic= $thread_topic, email2.email_date=$email_date "
               "ON MATCH SET email2.thread_index= $thread_index, email2.thread_topic= $thread_topic "
               "MERGE (email)-[r:REPLY_OF]->(email2)"
               " ON CREATE  SET r.weight= 1 , r.counter=1, r.accessTime = timestamp() ,"
               " r.thread_index = $thread_index, r.thread_topic = $thread_topic"
               " ON MATCH SET r.counter = coalesce(r.counter, 0) + 1, r.weight=coalesce(r.weight, 0) + 1,"
               " r.accessTime = timestamp(), r.thread_index = $thread_index, r.thread_topic = $thread_topic ",
               email_id=email_id, content=content, _from=_from, co=co, email_date=email_date, reply_id=reply_id,
               thread_index=thread_index, thread_topic=thread_topic)

    @staticmethod
    def add_relationship_between_emails(tx, email_id, content, email_id_target, email_date, thread_index='',
                                        thread_topic=''):
        tx.run("MATCH (email:Email {id:$email_id}),"
               "(email2:Email {id:$email_id_target}) "
               "MERGE (email)-[r:RELATION_WITH]->(email2)  "
               " ON CREATE  SET r.weight= 1 , r.counter=1, r.accessTime = timestamp() "
               " ON MATCH SET r.counter = coalesce(r.counter, 0) + 1, r.weight=coalesce(r.weight, 0) + 1,"
               " r.accessTime = timestamp()",
               email_id=email_id, content=content, email_id_target=email_id_target, email_date=email_date,
               thread_index=thread_index,
               thread_topic=thread_topic)

    def store(self, msg_list):
        """Return the tumber of message stored"""
        if self.driver is None:  # or self.open_db!=True
            self.logger.error('the connection is unable')
            return 0

        i = 0
        for msg in msg_list:
            try:
                i = i + 1
                email_to = None
                email_cc = None
                email_bcc = None
                email_subject = None
                sender = None
                in_reply_to = None
                references = None
                references_cleaned = None
                thread_topic = None
                thread_index = None
                email_date = None

                if 'from' in msg:
                    sender = self.clear_email(msg[ 'From' ].split(','))  # msg['from'].split(',')
                elif 'From' in msg:
                    sender = self.clear_email(msg['From'].split(','))  # msg['from'].split(',')
                else:
                    self.logger.debug('FROM DOEST EXIST')
                    break
                if 'subject' in msg:
                    email_subject = msg['subject']
                elif 'Subject' in msg:
                    email_subject = msg['Subject']
                else:
                    email_subject = "NA"
                if 'To' in msg:
                    if not (msg['To'] is None):
                        email_to = self.clear_email(msg['To'].split(','))
                    else:
                        if not (msg['Delivered-To'] is None):
                            email_to = self.clear_email(msg['Delivered-To'].split(','))
                        else:
                            self.logger.debug('TO DOESNT EXIST')
                elif 'Delivered-To' in msg:
                    if not (msg['Delivered-To'] is None):
                        email_to = self.clear_email(msg['Delivered-To'].split(','))
                    else:
                        self.logger.debug('Delivered-To DOESNT EXIST')
                else:
                    self.logger.debug('To DOESNT EXIST')

                if 'Message-Id' in msg:
                    if (msg['Message-Id'] is None):
                        self.logger.debug('Message-Id DOESNT EXIST')
                        return
                    else:
                        email_id = clean_id(msg[ 'Message-Id' ])
                elif 'Message-ID' in msg:
                    if (msg['Message-ID'] is None):
                        self.logger.debug('Message-ID DOESNT EXIST')
                        return
                    else:
                        email_id = clean_id(msg[ 'Message-ID' ])

                if 'CC' in msg:
                    email_cc = self.clear_email(msg[ 'Cc' ].split(','))
                elif 'Cc' in msg:
                    email_cc = self.clear_email(msg['Cc'].split(','))
                if 'Bcc' in msg:
                    email_bcc = self.clear_email(msg['Bcc'].split(','))
                if 'Thread-Topic' in msg:
                    if msg[ 'Thread-Topic' ] is None:
                        self.logger.debug('Thread-Topic DOESNT EXIST')
                    else:
                        thread_topic = msg[ 'Thread-Topic' ]
                if 'Thread-Index' in msg:
                    if msg[ 'Thread-Index' ] is None:
                        self.logger.debug('Thread-Index DOESNT EXIST')
                    else:
                        thread_index = clean_id(msg[ 'Thread-Index' ])
                if 'In-Reply-To' in msg:
                    if msg[ 'In-Reply-To' ] is None:
                        self.logger.debug('In-Reply-to DOESNT EXIST')
                    else:
                        in_reply_to = clean_id(msg[ 'In-Reply-To' ])
                if 'References' in msg:
                    if msg[ 'References' ] is None:
                        self.logger.debug('References DOESNT EXIST')
                    else:
                        references = msg[ 'References' ].replace('\n', '').replace('\t', '') \
                            .replace('\r', '').replace(' ', ',').split(',')
                        references_cleaned = list(map(clean_id, references))
                if 'Date' in msg:
                    if msg[ 'Date' ] is None:
                        self.logger.debug('Date DOESNT EXIST')
                    else:
                        email_date = neotime.DateTime.from_native(parse(msg[ 'Date' ]))


                with self.driver.session() as session:
                    session.write_transaction(self.define_emailUser_constraint)
                    for _from in sender:
                        if not (email_to is None):
                            for to in email_to:
                                if _from != None and to != None:
                                    if in_reply_to is not None:
                                        session.write_transaction(self.add_email_to_reply, email_id, email_subject,
                                                                  _from.lower(), to.lower(), email_date, in_reply_to,
                                                                  thread_index, thread_topic)
                                    else:
                                        session.write_transaction(self.add_email_to, email_id, email_subject,
                                                                  _from.lower(),
                                                                  to.lower(), email_date)
                                    self.logger.debug(
                                        ' Id: ' + email_id + ' From:' + _from.lower() + ' TO:' + to.lower())
                        if not (email_cc is None):
                            for cc in email_cc:
                                if _from != None and cc != None:
                                    if in_reply_to is not None:
                                        session.write_transaction(self.add_email_cc_reply, email_id, email_subject,
                                                                  _from.lower(), cc.lower(), email_date, in_reply_to,
                                                                  thread_index,
                                                                  thread_topic)
                                    else:
                                        session.write_transaction(self.add_email_cc, email_id, email_subject,
                                                                  _from.lower(),
                                                                  cc.lower(), email_date)
                                    self.logger.debug(
                                        ' Id: ' + email_id + ' From:' + _from.lower() + ' CC:' + cc.lower())
                        if not (email_bcc is None):
                            for bcc in email_bcc:
                                if _from != None and bcc != None:
                                    if in_reply_to is not None:
                                        session.write_transaction(self.add_email_bcc_reply, email_id, email_subject,
                                                                  _from.lower(), bcc.lower(), email_date, in_reply_to,
                                                                  thread_index,
                                                                  thread_topic)
                                    else:
                                        session.write_transaction(self.add_email_bcc, email_id, email_subject,
                                                                  _from.lower(), bcc.lower(), email_date)
                                    self.logger.debug(
                                        ' Id: ' + email_id + ' From:' + _from.lower() + ' Bcc:' + bcc.lower())
                        if not (references_cleaned is None):
                            for reference in references_cleaned:
                                if reference != email_id:
                                    session.write_transaction(self.add_relationship_between_emails, email_id,
                                                              email_subject,
                                                              reference, email_date, thread_index,
                                                              thread_topic)


            except Exception as read_exp:
                self.logger.exception("Error in store()")

        return i
