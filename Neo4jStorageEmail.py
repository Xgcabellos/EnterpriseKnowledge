# version v1.1
import logging
import re

from neo4j.v1 import GraphDatabase

from StorageEmail import storage_email

_author__ = 'Xavier Garcia Cabellos'
__date__ = '20180101'
__version__ = 0.01
__description__ = 'This scripts write emails in datasource Neo4J'


# driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "Gandalf"))

class neo4j_storage_email(storage_email):
    def __init__(self, driver):
        self.driver = driver
        log = logging.getLogger("neo4j.bolt")
        log.setLevel(logging.ERROR)
        # self.active_log('neo4j_storage_email.log', self.level)

    def storage_type(self):
        """"Return a string representing the type of connection this is."""
        return 'neo4j'

    def connect(self, url, login, pw):
        self.driver = GraphDatabase.driver(url, auth=(login, pw))
        # self.active_log(neo4j_storage_email + '.log', self.level)
        return self.driver

    def __del__(self):
        if self.open_db == True:
            self.driver = self.driver.close()

    @staticmethod
    def define_emailUser_constraint(tx):
        send_text = "CREATE CONSTRAINT ON (user:EmailUser) ASSERT user.username IS UNIQUE  "
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
               email_id=email_id, content=content, front=front, to=to)

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
               email_id=email_id, content=content, front=front, cc=cc)

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
               email_id=email_id, content=content, front=front, co=co)

    def store(self, msg_list):
        """Return the tumber of message stored"""
        i = 0
        for msg in msg_list:
            try:
                i = i + 1
                email_to = None
                email_cc = None
                email_bcc = None
                email_subject = None
                sender = None

                if 'from' in msg:
                    sender = self.clear_email(msg['from'].split(','))  # msg['from'].split(',')
                elif 'From' in msg:
                    sender = self.clear_email(msg['From'].split(','))  # msg['from'].split(',')
                else:
                    logging.debug('FROM DOEST EXIST')
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
                            logging.debug('TO DOESNT EXIST')
                elif 'Delivered-To' in msg:
                    if not (msg['Delivered-To'] is None):
                        email_to = self.clear_email(msg['Delivered-To'].split(','))
                    else:
                        logging.debug('Delivered-To DOESNT EXIST')
                else:
                    logging.debug('To DOESNT EXIST')

                if 'Message-Id' in msg:
                    if (msg['Message-Id'] is None):
                        logging.debug('Message-Id DOESNT EXIST')
                        return
                    else:
                        email_id = re.sub(r'[<>]', '', msg['Message-Id'])
                elif 'Message-ID' in msg:
                    if (msg['Message-ID'] is None):
                        logging.debug('Message-ID DOESNT EXIST')
                        return
                    else:
                        email_id = re.sub(r'[<>]', '', msg['Message-ID'])

                if 'CC' in msg:
                    email_cc = self.clear_email(msg['CC'].split(','))
                elif 'Cc' in msg:
                    email_cc = self.clear_email(msg['Cc'].split(','))
                if 'Bcc' in msg:
                    email_bcc = self.clear_email(msg['Bcc'].split(','))

                with self.driver.session() as session:
                    session.write_transaction(self.define_emailUser_constraint)
                    for front in sender:
                        if not (email_to is None):
                            for to in email_to:
                                if front != None and to != None:
                                    session.write_transaction(self.add_email_to, email_id, email_subject, front.lower(),
                                                              to.lower())
                                    logging.info(' Id: ' + email_id + ' From:' + front.lower() + ' TO:' + to.lower())
                        if not (email_cc is None):
                            for cc in email_cc:
                                if front != None and cc != None:
                                    session.write_transaction(self.add_email_cc, email_id, email_subject, front.lower(),
                                                              cc.lower())
                                    logging.info(' Id: ' + email_id + ' From:' + front.lower() + ' CC:' + cc.lower())
                        if not (email_bcc is None):
                            for bcc in email_bcc:
                                if front != None and bcc != None:
                                    session.write_transaction(self.add_email_bcc, email_id, email_subject,
                                                              front.lower(), bcc.lower())
                                    logging.info(' Id: ' + email_id + ' From:' + front.lower() + ' Bcc:' + bcc.lower())

            except Exception as read_exp:
                logging.exception("Error in store()")

        return i
