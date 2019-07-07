import argparse
import configparser
import logging
import multiprocessing as mp
import os
import sys

import EmailProcess
import PstProcess
from ConnectionProperties import email_IMAP_connexion_properties
from ConnectionProperties import google_connexion_properties
from EmailProcess import gmail
from EmployeeEmailWorker import read_users_info
from GraphAnalyticNeo4J import neo4j_graph_analytic
from JsonStorageEmail import json_storage_email
from MongoDbStorageEmail import mongodb_storage_email
from Neo4jStorageEmail import neo4j_storage_email
from TextProcess import log_level_conversor

__author__ = 'Xavier Garcia Cabellos'
__date__ = '20190601'
__version__ = 0.2
__description__ = 'This program get emails from different sources and store them in Neo4j and json'





# this variable should be manage by input parematers

_ORG_EMAIL = ""
_SMTP_SERVER = ''
log_path = ""
ORG_EMAIL = ''
config_name = '../input/config.ini'

config = configparser.ConfigParser()
config.read(config_name)


log_name = config['LOGS']['LOG_FILE']
log_directory = config['LOGS']['LOG_DIRECTORY']


log_level       = log_level_conversor(config['LOGS']['log_level'])
log_level_json  = log_level_conversor(config['LOGS']['log_level_json'])
log_level_graph = log_level_conversor(config['LOGS']['log_level_graph'])
log_level_email = log_level_conversor(config['LOGS']['log_level_email'])


users_file = config['INPUT']['USERS_FILE']
SSLusers_file = config[ 'INPUT' ][ 'USERSSSL_FILE' ]




EMAIL_SERVER = config['CONNECTION']['EMAIL_SERVER']
GRAPHDDBB_SERVER =  config['CONNECTION']['GRAPHDDBB_SERVER']
MAX_NUMBER_OR_CONNEXIONS =  int(config['PROCESS']['MAX_NUMBER_OR_CONNEXIONS'])
NUMBER_OF_MESSAGES_BY_CYCLE = int(config['PROCESS']['NUMBER_OF_MESSAGES_BY_CYCLE'])

JSON_STORE_TYPE = config['OUTPUT']['JSON_STORE_TYPE']



# -------------------------------------------------
#
# Utility to read email from Gmail Using Python
#
# ------------------------------------------------
def active_log(log_name='process.log', level=logging.INFO, log_directory='./logs/'):
    if log_directory:
        if not os.path.exists(log_directory):
            os.makedirs(log_directory)
        log_path = os.path.join(log_directory, log_name)
    else:
        log_path = log_name
    logging.basicConfig(filename=log_path, level=level,
                format='%(asctime)s | %(levelname)s | %(name)s | %(processName)s | %(message)s', filemode='a')

    # logging.info('Starting mail_process logger using v.' + str(__version__)+' System ' + sys.platform+' Version ' + sys.version)


from neo4j.v1 import GraphDatabase

def processFolders(reader,  name_folder, _FROM_EMAIL,output_queue):

    message_list = [ ]
    json_store=None
    graph_store=None
    driver2=None
    global driver
    count_of_messages_by_folder = 0
    directory_json = config[ 'OUTPUT' ][ 'DIRECTORY_JSON' ]
    # we choose between write in mongoDB or in file. for know it must to by done here, in the future, i will convert it a paramenter.
    if JSON_STORE_TYPE == 'file':
        json_store = json_storage_email(log_name, log_level_json, log_directory)
        json_store.directory = directory_json

    elif JSON_STORE_TYPE == 'mongoDB':
        json_store = mongodb_storage_email(None, log_name, log_level_json, log_directory)
        json_store.connect(config[ 'CONNECTION' ][ 'MONGODB_SERVER' ],
                           config[ 'CONNECTION' ][ 'MONGODB_LOGIN' ],
                           config[ 'CONNECTION' ][ 'MONGODB_PASSWORD' ],
                           config[ 'CONNECTION' ][ 'MONGODB_DATABASE' ])
    reader.json_store = json_store  # json_storage_email(log_name, log_level_json, log_directory)

    if driver is not None:
        try:
            driver2 = GraphDatabase.driver("bolt://" + GRAPHDDBB_SERVER + ":7687",
                                          auth=(config[ 'CONNECTION' ][ 'GRAPHDDBB_LOGIN' ],
                                                config[ 'CONNECTION' ][ 'GRAPHDDBB_PASSWORD' ]))





        except Exception as bErr:
            module_logger.error('Error connecting GraphDatabase: %s', bErr)

    graph_store = neo4j_storage_email(driver2, log_name, log_level_graph, log_directory)

    start_email_num = 0
    steps = NUMBER_OF_MESSAGES_BY_CYCLE  # each how many messages we are going to process @configuration
    # this part let me divide all the writes in blocks calls steps.
    while (start_email_num != -1):

        email_list = None
        try:
            # getting the messages in groups by folder.
            email_list = reader.checkForMessages(start_email_num, start_email_num + steps, name_folder)
            if email_list is not None:
                num_messages=len(email_list)
                module_logger.debug('Reading  '+str(num_messages) + ' to include in the '+
                                    str(len(message_list)) + ' of the already include in general list of '
                                    + name_folder )
                if num_messages > 0:
                    num_emails_verificador = start_email_num
                    start_email_num += num_messages
                    reader.email_filter(email_list)
                    message_list += email_list

                    try:
                        # write in graph_ddbb

                        result = graph_store.store(email_list)
                        if result>0:
                            module_logger.debug('Graph stored ' + str(result) + ' json messages ')
                    except Exception as graphError:
                        module_logger.error('Error writing in graph storage with user:' + _FROM_EMAIL + ': %s',
                                     graphError)

                    if num_emails_verificador + steps != start_email_num:  # because i have not more
                        start_email_num = -1  # exit of the while
                else:
                    start_email_num = -1  # exit of the while
            else:
                start_email_num = -1  # exit of the while

        except Exception as readingError:
            module_logger.exception('Error reading email with user:' + _FROM_EMAIL + ': %s', readingError)

    module_logger.debug('Processed ' + str(len(message_list)) + ' messages in folder ' + name_folder)
    output_queue.put(message_list)
    #return  message_list



active_log(log_name, log_level, log_directory)

#QueueListener(
#    _log_queue, logging.FileHandler("./logs/EKnowledge.log")).start()

# Push all logs into the queue:
#logging.getLogger().addHandler(QueueHandler(_log_queue))

module_logger = logging.getLogger('EmailProcesser')

mp.log_to_stderr()
driver = None
def main():
    output_queue = mp.Queue()
    global driver
    list_of_process = [ ]
    # Define an output queue



    logging.getLogger('chardet.charsetprober').setLevel(logging.INFO)
    logging.getLogger('EmployeeEmailworker').setLevel(logging.INFO)

    try:
        driver = GraphDatabase.driver("bolt://" + GRAPHDDBB_SERVER + ":7687",
                auth = (config['CONNECTION']['GRAPHDDBB_LOGIN'],config['CONNECTION']['GRAPHDDBB_PASSWORD']))

    except Exception as bErr:
        module_logger.error('Error connecting GraphDatabase: %s', bErr)
        driver=None

    directory_json = config['OUTPUT']['DIRECTORY_JSON']
    type_of_email = config['INPUT']['TYPE_OF_EMAIL']

    graph_store = neo4j_storage_email(driver, log_name, log_level_graph, log_directory)
    graph_store.active_log('EKnowledge.log', log_level_graph)

    module_logger.info('Starting EKnowledge v.' + str(__version__) + ' (Alpha)')
    module_logger.info('Coded by ' + str(__author__) + ' at date ' + str(__date__))
    module_logger.info(str(__description__))
    module_logger.info('Starting Script...')

    json_store = None
    # we choose between write in mongoDB or in file. for know it must to by done here, in the future, i will convert it a paramenter.
    if type_of_email == 'PST':
        pst = PstProcess.pst_file('../input/xavier.pst', './output', 'report.info', log_directory='output')
        pst.read_all()
        start_email_num = graph_store.store(pst.message_list)
        module_logger.info('graph stored ' + str(start_email_num) + ' json messages ')
        return

    elif JSON_STORE_TYPE == 'file':
        json_store = json_storage_email(log_name, log_level_json, log_directory)
        json_store.directory=directory_json

    elif JSON_STORE_TYPE =='mongoDB':
        json_store = mongodb_storage_email(None,log_name,log_level_json,log_directory)
        json_store.connect(config['CONNECTION']['MONGODB_SERVER'],config['CONNECTION']['MONGODB_LOGIN'],
                           config[ 'CONNECTION' ][ 'MONGODB_PASSWORD' ],config['CONNECTION']['MONGODB_DATABASE'])

    usersinfo = None
    if type_of_email == 'IMAP_SSL':
        usersinfo = read_users_info(SSLusers_file)  # we read all the email using a file .csv
    elif type_of_email == 'IMAP':
        usersinfo = read_users_info(users_file)  # we read all the email using a file .csv

    all_messages = [ ]
    for index, user in usersinfo.iterrows():    # Reading all users
        FROM_EMAIL = user[ 'sername' ]          # from sername. the name is a mistake without any meaning.
        FROM_PWD = user[ 'password' ]           # from sername. password similar to name.
        _SMTP_SERVER = user[ 'smtp']
        ACTIVE = user ['active']
        if ACTIVE.upper() != 'Y' :
            continue
        _ORG_EMAIL='@'+user['email'].split('@')[1]
        properties = None
        reader = None

        if type_of_email == 'IMAP_SSL':
            # Tested with gmail and only gmail. it let us to text real emails.

            properties = google_connexion_properties(_ORG_EMAIL, FROM_EMAIL, FROM_PWD)
            properties.SMTP_SERVER=_SMTP_SERVER

            reader = gmail(properties, True,gmail.active_log('EKnowledge.log', log_level_email))


        elif type_of_email == 'IMAP':
            # in this case, it is all case of emails accesed by IMAP.
            try:
                # we prepare the connexion properties
                properties = email_IMAP_connexion_properties(_ORG_EMAIL, FROM_EMAIL, FROM_PWD, False, EMAIL_SERVER, 143)

                # and generate a reader of the technology of the properties
                reader = gmail(properties, True, gmail.active_log('EKnowledge.log', log_level_email))
                # include the logger because i need use the same file for write the loggins


            except Exception as eMailConection:
                module_logger.error('Error connecting email with user:' + FROM_EMAIL + ': %s', eMailConection)
                continue
        # for each users, for each folder in reader, doesn't matter what is the type of connection
        reader.json_store = json_store
        # include in the object gmail a element for write json. in this case to file
        message_list = [ ]

        list_of_connexions={}
        for folder in reader.folder_list:
            forbidden=EmailProcess.forbidden(folder)
            #Get the las value of the folder. iti is the name ofi the folder.
            name_folder = str(folder).split('"/"')[ -1 ]
            # # be careful - with google is not exactly the same. verify.
            name_folder = name_folder.replace('\'', '').replace('"', '').strip()


            if not forbidden:
                repeat_connexion=True
                reader2 = None
                while repeat_connexion!=False:
                    reader2 = None
                    module_logger.debug('folder read:{}'.format(name_folder))
                    # for manage just only a group of connections.
                    if len(list_of_connexions)<MAX_NUMBER_OR_CONNEXIONS:
                        try:

                            reader2 = gmail(properties,False, reader.logger)

                        except Exception as eMailConection:
                            module_logger.error('Error connecting email with user:' + FROM_EMAIL + ' reading '+
                                                name_folder+': %s', eMailConection)
                            try:
                                reader2 = gmail(properties, False, reader.logger)

                            except Exception as eMailConection:
                                module_logger.error(' 2º Error connecting email with user:' + FROM_EMAIL + ' reading ' +
                                                    name_folder + ': %s', eMailConection)
                                continue


                        # message_list=processFolders(reader, name_folder, FROM_EMAIL)
                        p= mp.Process(target=processFolders, args=(reader2, name_folder, FROM_EMAIL, output_queue),)
                        list_of_process.append(p)
                        list_of_connexions[p.name] = reader2
                        p.start()
                        module_logger.debug(name_folder+ ' folder has started in process %s', p.name)
                        repeat_connexion=False
                    else:

                        for p1 in list_of_process:
                            repeat_connexion = True
                            if not p1.is_alive():
                                if output_queue.qsize() > 0:
                                    message_list += output_queue.get()

                                module_logger.info('{}.exitcode = {}'.format(p1.name, p1.exitcode) +
                                                    ' : Processed ' + str(len(message_list)) +
                                                    ' messages ')

                                del list_of_connexions[ p1.name ]
                                list_of_process.remove(p1)
                                repeat_connexion = False

                            else:
                                module_logger.debug('Joining ' + str(p1.name) + ' ' + str(output_queue.qsize()))
                                if output_queue.qsize() > 0:
                                    message_list += output_queue.get()
                                p1.join(1)
                                module_logger.debug('List of connections number = %s', len(list_of_connexions))
                                repeat_connexion = False

                        #module_logger.debug('ending for that verify the finish connexions')
                #module_logger.debug('ending while  that verify the  number of connexions')

            else:
                module_logger.debug('folder not read:{}'.format(name_folder) )

        # Get process results from the output queue
        finish=len(list_of_process)
        while finish!=0:
            for p in list_of_process:
                if not p.is_alive():
                    if output_queue.qsize() > 0:
                        try:
                            message_list+=output_queue.get(False)
                        except Exception as QueueException:
                            module_logger.warning(
                                'Queue empty. Doesn\'t matter if it say  queue is not empty: {}'.format(QueueException))
                    finish-=1
                    module_logger.info('{}.exitcode 2 = {}'.format(p.name, p.exitcode) +
                                        ' : Processed ' + str(len(message_list)) +
                                        ' messages ')
                    del list_of_connexions[ p.name ]
                    list_of_process.remove((p))

                else:
                    module_logger.debug('Joining 2 ' + p.name+ ' '+str(output_queue.qsize()))
                    if output_queue.qsize()>0:
                        message_list += output_queue.get()
                    p.join(5)



        # message_list += [ output_queue.get() for pro in list_of_process ]
        all_messages += message_list
        module_logger.info('Processed ' + str(len(all_messages)) + ' messages all folder ')
        list_of_connexions.clear()


    # statistics
    # summary(all_messages)

    try:
        driver = GraphDatabase.driver("bolt://" + GRAPHDDBB_SERVER + ":7687",
                                      auth=(config[ 'CONNECTION' ][ 'GRAPHDDBB_LOGIN' ],
                                            config[ 'CONNECTION' ][ 'GRAPHDDBB_PASSWORD' ]))
    except Exception as bErr:
        module_logger.error('Error connecting GraphDatabase: %s', bErr)

    graph = neo4j_graph_analytic(driver, log_name, log_level, log_directory)
    graph.analysis()





os.__call__ = processFolders

# setup args including default values and input error handling
def check_arg(args=None):
    parser = argparse.ArgumentParser(description='EmailProcessor - Enterprise knowledge about the relationship between '
                                                 'employees')

    parser.add_argument('-c', '--config_file', help='config file and directory', nargs='?',
                        default='./input/config.ini')
    # parser.add_argument('-e', '--emails_file', help= 'csv file of emails to process', nargs='?',
    #                     default='../input/config.ini')
    # parser.add_argument('-s', nargs='?', default='dsc')
    results = parser.parse_args(args)
    return (results.config_file)

# MAIN

if __name__ == '__main__':
    '''
        Enterprise knowledge about the relationship between employees'
        Get the email from a list of accounts, store the info in json in file or mongoDB 
        and the relationship in neo4j.
        Usage :  python3 EmailProcessor.py -c ../input/config.ini
        (note) All args are optional: '-c'
    '''

    config_name= check_arg(sys.argv[ 1: ])
    print('Inputs: config_file=%s ' % config_name)

    # start the program
    main()
