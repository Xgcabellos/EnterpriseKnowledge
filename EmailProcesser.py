# version v1.1
import logging

import ConnectionProperties
import EmailProcess
import PstProcess
from JsonStorageEmail import json_storage_email
from Neo4jStorageEmail import neo4j_storage_email

ORG_EMAIL   = "@gmail.com"
FROM_EMAIL  = "xgcabellos" + ORG_EMAIL
FROM_PWD = "XXXXXXXXXXXXXXXXX"

__author__ = 'Xavier Garcia Cabellos'
__date__ = '20180116'
__version__ = 0.02
__description__ = 'This program get emails front different sources and include in Neo4j and json'




# -------------------------------------------------
#
# Utility to read email from Gmail Using Python
#
# ------------------------------------------------

from neo4j.v1 import GraphDatabase

#10.162.41.156 - 192.168.1.105
driver = GraphDatabase.driver("bolt://192.168.1.105:7687", auth=("neo4j", "Gandalf"))
# driver = GraphDatabase.driver("bolt://10.162.41.156:7687", auth=("neo4j", "Gandalf"))

directory='./json/'
gmail = False
graph_store = neo4j_storage_email(driver)
graph_store.active_log('EKnowledge.log', logging.INFO)
logging.info('Starting EKnowledge v.' + str(__version__) + ' (Alpha)')
logging.info('Coded by ' + str(__author__) + ' at date ' + str(__date__))
logging.info(str(__description__))
logging.info('Starting Script...')

json_store = json_storage_email(None)
if gmail == True:
    properties = ConnectionProperties.google_connexion_properties(ORG_EMAIL, "xgcabellos", FROM_PWD)
    reader = EmailProcess.gmail(properties)
    for folder in reader.folder_list:
        name_folder = str(folder).split('"')[-2]
        if (name_folder.upper() != 'INBOX') and (name_folder.upper() != 'ENVIADO'):  ## be careful TEMPORAL
            continue
        start_email_num = 0
        steps = 3
        while (start_email_num!=-1):
            email_list=None
            file=properties.FROM_EMAIL.split('@')[0]+ '.gmail'+str(start_email_num) + '_'+ str(name_folder)+'.json'
            num_emails_verificador=start_email_num
            email_list=reader.read(start_email_num, start_email_num + steps, name_folder)
            print('Readed ' + str(len(email_list)) + ' email messages in '+name_folder)
            jsonified_messages= reader.jsonizer_emails(email_list)
            print('prepared '+str(len(jsonified_messages))+' json messages in '+name_folder)
            json_store.store(jsonified_messages,directory, file)
            start_email_num +=graph_store.store(email_list)
            print('graph stored ' +str(start_email_num) + ' json messages ')
            if num_emails_verificador+steps!=start_email_num:
                start_email_num=-1

else:
    pst = PstProcess.pst_file('../input/xavier.pst', './output', 'report.info', log_directory='output')
    pst.read_all()
    start_email_num = graph_store.store(pst.message_list)
    logging.info('graph stored ' + str(start_email_num) + ' json messages ')
