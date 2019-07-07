# version v1.0

from abc import ABCMeta, abstractmethod


class connexion_properties(object):
    """abstact class for email server connecxion .

       Attributes:
           org_email: company domain
           from_email: connexcion email
           from_password: Password
           smtp_server: Logic direction of server
           smtp_port:connection's port. For Google https  993
           crypted: use SSL or not
    """

    __metaclass__ = ABCMeta

    ORG_EMAIL = "@organization.com"
    FROM_EMAIL = "email" + ORG_EMAIL
    FROM_PWD = "password"
    SMTP_SERVER = "imap.gmail.com"
    SMTP_PORT = 993
    CRYPTED = True
    DUMMY=False



    def __init__(self):
        """Return a new connecxion_properties object """

        pass

    def __init__(self, org_email, from_email, from_password, smtp_server=None, smtp_port=0, crypted=True):
        """Return a new connecxion_properties object """
        self.ORG_EMAIL = org_email
        self.FROM_EMAIL = from_email + self.ORG_EMAIL
        self.FROM_PWD = from_password
        if smtp_server is not None:
            self.SMTP_SERVER = smtp_server
        if not smtp_port == 0:
            self.SMTP_PORT = smtp_port  # 143 / 993
        self.CRYPTED = crypted

    @abstractmethod
    def connection_type(self):
        """"Return a string representing the type of connection this is."""
        return 'Abstract Class'

    def __del__(self):
        self.ORG_EMAIL = "@organization.com"
        self.FROM_EMAIL = "email" + self.ORG_EMAIL
        self.FROM_PWD = "password"
        self.SMTP_SERVER = "imap.gmail.com"
        self.SMTP_PORT = 993


class google_connexion_properties(connexion_properties):
    """abstact class for email server connecxion .

       Attributes:
           org_email: company domain
           from_email: connexcion email
           from_password: Password
           smtp_server: Logic direction of server
           smtp_port:connection's port. For Google https  993
    """

    def __init__(self, org_email, from_email, from_password, smtp_server="imap.gmail.com", smtp_port=993, crypted=True): # ,smtp_server,smtp_port):
        """Return a new connecxion_properties object
          """
        self.ORG_EMAIL = org_email
        self.FROM_EMAIL = from_email + self.ORG_EMAIL
        self.FROM_PWD = from_password
        if smtp_server is not None:
            self.SMTP_SERVER = smtp_server
        if not smtp_port == 0:
            self.SMTP_PORT = smtp_port  # 143 / 993
        self.CRYPTED = crypted


    def connection_type(self):
        """"Return a string representing the type of connection this is."""
        return 'google gmail'


class email_IMAP_connexion_properties(connexion_properties):
    """abstact class for email server connecxion .

       Attributes:
           org_email: company domain
           from_email: connexcion email
           from_password: Password
           smtp_server: Logic direction of server
           smtp_port:connection's port. For Google https  993
    """

    def __init__(self, org_email, from_email, from_password, crypted, smtp_server,
                 smtp_port) -> object:  # ,smtp_server,smtp_port):
        """Return a new connecxion_properties object
          """
        self.ORG_EMAIL = org_email
        self.FROM_EMAIL = from_email + self.ORG_EMAIL
        self.FROM_PWD = from_password
        self.SMTP_SERVER = smtp_server
        self.SMTP_PORT = smtp_port  # 143 / 993
        self.CRYPTED = crypted

    def connection_type(self):
        """"Return a string representing the type of connection this is."""
        return 'IMAP email '


class email_POP3_connexion_properties(connexion_properties):
    """abstact class for email server connecxion .

           Attributes:
               smtp_server: server smtp
               org_email: company domain
               from_email: connexcion email
               from_password: Password
               smtp_server: Logic direction of server
               smtp_port:connection's port. For Google https  993
        """

    def __init__(self, org_email, from_email, from_password, smtp_server=None, smtp_port=0, crypted=True) -> object:  # ,smtp_server,smtp_port):
        """Return a new connecxion_properties object
              """
        self.ORG_EMAIL = org_email
        self.FROM_EMAIL = from_email + self.ORG_EMAIL
        self.FROM_PWD = from_password
        if smtp_server is not None:
            self.SMTP_SERVER = smtp_server
        if not smtp_port == 0:
            self.SMTP_PORT = smtp_port  # 143 / 993
        self.CRYPTED = crypted

    def connection_type(self):
        """"Return a string representing the type of connection this is."""
        return 'POP3 email'

class email_DUMMY_connexion_properties(connexion_properties):
    """abstact class for email server connecxion .

           Attributes:
               smtp_server: server smtp
               org_email: company domain
               from_email: connexcion email
               from_password: Password
               smtp_server: Logic direction of server
               smtp_port:connection's port. For Google https  993
        """

    def __init__(self) -> object:  # ,smtp_server,smtp_port):
        """Return a new connecxion_properties object
              """
        self.DUMMY = True
        pass

    def connection_type(self):
        """"Return a string representing the type of connection this is."""
        return 'DUMMY email'
