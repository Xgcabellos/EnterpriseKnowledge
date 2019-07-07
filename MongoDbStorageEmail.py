# version v1.1
import configparser
from logging import getLogger, debug, warning, DEBUG

from pymongo import MongoClient

from StorageEmail import storage_email
from TextProcess import log_level_conversor

_author__ = 'Xavier Garcia Cabellos'
__date__ = '20190420'
__version__ = 0.01
__description__ = 'This scripts write emails in datasource mongoDb'

config_name = '../input/config.ini'

config = configparser.ConfigParser()
config.read(config_name)

log_name = config[ 'LOGS' ][ 'LOG_FILE' ]
log_directory = config[ 'LOGS' ][ 'LOG_DIRECTORY' ]
log_level = log_level_conversor(config[ 'LOGS' ][ 'log_level_json' ])

module_logger = getLogger('MongoDbStorageEmail')


class mongodb_storage_email(storage_email):
    driver = None
    logger=None
    database='eknowedgedb'
    def __init__(self,driver,log_file,log_level,log_directory):
        if driver is not None:
            self.driver = driver
            monitoring.register(CommandLogger())
        self.logger = self.active_log(log_file, log_level, log_directory)

    def storage_type(self):
        """"Return a string representing the type of connection this is."""
        return 'mongodb'

    def connect(self, url, user, password, authSource=database):
        if self.logger is None:
            self.logger=self.active_log(mongodb_storage_email + '.log', self.level)
        self.driver = MongoClient(url, username=user,
                                  password=password, authSource=authSource,
                                  authMechanism='SCRAM-SHA-1', event_listeners=[ CommandLogger() ])
        return self.driver

    def __del__(self):
        if self.open_db == True:
            self.driver = self.driver.close()


    def store(self, msg_list,database='eknowedgedb'):
        """Return the number of message stored"""
        i=0
        if self.driver is None:
            self.logger.error('the connection is unable')
            return 0
        db = self.driver[database]
        emails = db.emails
        emails_stored = None
        try:
            emails_stored = emails.insert_many(msg_list)
        except Exception as mongo_insert_many_exp:
            self.logger.error('Error writing msgs in Mongo:{}'.format(mongo_insert_many_exp))
            for m in msg_list:
                try:
                    result = emails.insert_one(m)
                    i += 1
                    self.logger.debug(str(i) + " Email key is: {}".format(str(result)))
                except Exception as mongo_insert_one_exp:
                    self.logger.error('Error writing one msg in Mongo:{}'.format(mongo_insert_one_exp))
                    self.logger.error(str(m))

        if emails_stored is not None:
            for m in emails_stored.inserted_ids:
                i += 1
                self.logger.debug(str(i) + " Email key is: {}".format(str(m)))
        return i


from pymongo import monitoring

class CommandLogger(monitoring.CommandListener):

    def started(self, event):
        debug("Command {0.command_name} with request id "
                     "{0.request_id} started on server "
                     "{0.connection_id}".format(event))

    def succeeded(self, event):
        debug("Command {0.command_name} with request id "
                     "{0.request_id} on server {0.connection_id} "
                     "succeeded in {0.duration_micros} "
                     "microseconds".format(event))

    def failed(self, event):
        debug("Command {0.command_name} with request id "
                     "{0.request_id} on server {0.connection_id} "
                     "failed in {0.duration_micros} "
                     "microseconds".format(event))

class ServerLogger(monitoring.ServerListener):

    def opened(self, event):
        debug("Server {0.server_address} added to topology "
                     "{0.topology_id}".format(event))

    def description_changed(self, event):
        previous_server_type = event.previous_description.server_type
        new_server_type = event.new_description.server_type
        if new_server_type != previous_server_type:
            # server_type_name was added in PyMongo 3.4
            debug(
                "Server {0.server_address} changed type from "
                "{0.previous_description.server_type_name} to "
                "{0.new_description.server_type_name}".format(event))

    def closed(self, event):
        warning("Server {0.server_address} removed from topology "
                        "{0.topology_id}".format(event))


class HeartbeatLogger(monitoring.ServerHeartbeatListener):

    def started(self, event):
        debug("Heartbeat sent to server "
                     "{0.connection_id}".format(event))

    def succeeded(self, event):
        # The reply.document attribute was added in PyMongo 3.4.
        debug("Heartbeat to server {0.connection_id} "
                     "succeeded with reply "
                     "{0.reply.document}".format(event))

    def failed(self, event):
        warning("Heartbeat to server {0.connection_id} "
                        "failed with error {0.reply}".format(event))

class TopologyLogger(monitoring.TopologyListener):

    def opened(self, event):
        debug("Topology with id {0.topology_id} "
                     "opened".format(event))

    def description_changed(self, event):
        debug("Topology description updated for "
                     "topology id {0.topology_id}".format(event))
        previous_topology_type = event.previous_description.topology_type
        new_topology_type = event.new_description.topology_type
        if new_topology_type != previous_topology_type:
            # topology_type_name was added in PyMongo 3.4
            debug(
                "Topology {0.topology_id} changed type from "
                "{0.previous_description.topology_type_name} to "
                "{0.new_description.topology_type_name}".format(event))
        # The has_writable_server and has_readable_server methods
        # were added in PyMongo 3.4.
        if not event.new_description.has_writable_server():
            warning("No writable servers available.")
        if not event.new_description.has_readable_server():
            warning("No readable servers available.")

    def closed(self, event):
        debug("Topology with id {0.topology_id} "
                     "closed".format(event))




def main():
    log_level_json=DEBUG
    json_directory = './json/'
    users_file = "../input/users.csv"
    books_directory = '../input/books/'
    knowledge_file = "../input/knowledges.csv"
    module_logger = getLogger('MongoDbStorageEmail')


    json_store = mongodb_storage_email(None, log_name, log_level_json, log_directory)
    json_store.connect('localhost', 'admin', 'Gandalf6981', 'admin')
    import json
    from bson.json_util import loads

    with open(json_directory+'xgcabellos.gmail0_INBOX.json', 'r') as f:
        data = json.load(f)
    json_list=[]
    for doc in data:
        data_json = loads(json.dumps(doc))
        json_list.append(data_json)
    json_store.store(json_list)

    db = json_store.driver['eknowegdedb' ]
    emails = db.emails
    senders = [ i for i in emails.distinct("From") ]

    receivers = [ i for i in emails.distinct("To") ]

    cc_receivers = [ i for i in emails.distinct("Cc") ]

    bcc_receivers = [ i for i in emails.distinct("Bcc") ]

    print( "Num Senders: %i", len(senders))
    print( "Num Receivers: %i", len(receivers))
    print( "Num CC Receivers: %i", len(cc_receivers))
    print( "Num BCC Receivers: %i", len(bcc_receivers))

    # db = client['eknowedgedb']
    # emails = db.emails
    #
    # result = emails.insert_one(email)
    # emails_stored = emails.insert_many(emails)
    # print("First article key is: {}".format(result.inserted_id))
    # for article in articles.find({},{ "_id": 0, "author": 1, "about": 1}):
    #   print(article)
    #
    # doc = articles.find().sort("author", -1)
    # limited_result = articles.find().limit(1)
    #
    #
    # for x in doc:
    #   print(x)

    #
    # #ODM
    #
    # from mongoengine import *
    # connect('eknowedge', host='localhost', port=27017)
    # MongoClient(host=['localhost:27017'], document_class=dict, tz_aware=False, connect=True, read_preference=Primary())
    # class User(Document):
    #     email = StringField(required=True)
    #     first_name = StringField(max_length=30)
    #     last_name = StringField(max_length=30)
    # class Post(Document):
    #     title = StringField(max_length=120, required=True)
    #     author = ReferenceField(User)
    # user = User(email="connect@derrickmwiti.com", first_name="Derrick", last_name="Mwiti")
    # user.save()


if  __name__ == '__main__':
    main()