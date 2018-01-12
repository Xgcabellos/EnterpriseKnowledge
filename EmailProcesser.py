# version v1.1

import ConnectionProperties
import EmailProcess
from StorageEmail import file_storage_email
from StorageEmail import neo4j_storage_email

ORG_EMAIL   = "@gmail.com"
FROM_EMAIL  = "xgcabellos" + ORG_EMAIL
FROM_PWD = "xxxxxxxx"




# -------------------------------------------------
#
# Utility to read email from Gmail Using Python
#
# ------------------------------------------------

from neo4j.v1 import GraphDatabase

#10.162.41.156 - 192.168.1.105
driver = GraphDatabase.driver("bolt://192.168.1.105:7687", auth=("neo4j", "Gandalf"))
#becareful different depend operating system. it must to be done agnostic.
directory='./json/'
test=False
if test==True:
    properties= ConnectionProperties.google_connexion_properties(ORG_EMAIL,"xgcabellos",FROM_PWD)
    reader=EmailProcess.gmail(properties)
    graph_store=neo4j_storage_email(driver)
    json_store=file_storage_email(None)
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
    pst= EmailProcess.pst_file('../input/xavier.pst', './output', 'report.info', log_directory='output')
    pst.read_all()
