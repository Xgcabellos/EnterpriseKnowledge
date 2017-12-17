# version v1.1

import EmailProcess
import ConnectionProperties

from StorageEmail import neo4j_storage_email

ORG_EMAIL   = "@gmail.com"
FROM_EMAIL  = "xgcabellos" + ORG_EMAIL
FROM_PWD    = "xxxxxx"


# -------------------------------------------------
#
# Utility to read email from Gmail Using Python
#
# ------------------------------------------------

from neo4j.v1 import GraphDatabase, unicode

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "Gandalf"))



num_emails=1080
steps=10

properties= ConnectionProperties.google_connexion_properties(ORG_EMAIL,"xgcabellos",FROM_PWD)
reader=EmailProcess.gmail(properties)
graph_store=neo4j_storage_email(driver)
while (num_emails!=-1):
    email_list=None
    num_emails_verificador=num_emails
    email_list=reader.read(num_emails, num_emails + steps)
    #email_list = read_email_from_gmail(num_emails, num_emails + steps)
    reader.jsonizer_emails(email_list,'D:\\Desarrollo\\PycharmProjects\\RE1\\json\\',num_emails)
    #num_emails+=process_email(email_list)
    num_emails +=graph_store.store(email_list)
    if num_emails_verificador+steps!=num_emails:
        num_emails=-1

