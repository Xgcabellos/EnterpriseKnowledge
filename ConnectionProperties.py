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
    """

    __metaclass__ = ABCMeta

    ORG_EMAIL = "@organization.com"
    FROM_EMAIL = "email" + ORG_EMAIL
    FROM_PWD = "password"
    SMTP_SERVER = "imap.xxxx.com"
    SMTP_PORT = 993


    def __init__(self):
        """Return a new connecxion_properties object """
        pass

    def __init__(self, org_email, from_email,from_password,smtp_server, smtp_port):
        """Return a new connecxion_properties object """
        self.ORG_EMAIL=org_email
        self.FROM_EMAIL=from_email+ self.ORG_EMAIL
        self.FROM_PWD=from_password

    @abstractmethod
    def connection_type(self):
        """"Return a string representing the type of connection this is."""
        raise RuntimeError('abstact class')

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

    def __init__(self, org_email, from_email,from_password) -> object:#,smtp_server,smtp_port):
        """Return a new connecxion_properties object
          """
        self.ORG_EMAIL = org_email
        self.FROM_EMAIL = from_email+ self.ORG_EMAIL
        self.FROM_PWD = from_password
        self.SMTP_SERVER = "imap.gmail.com"
        self.SMTP_PORT = 993

    def connection_type(self):
        """"Return a string representing the type of connection this is."""
        return 'google gmail'