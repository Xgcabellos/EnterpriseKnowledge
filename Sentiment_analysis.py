import configparser
import json
import logging
import warnings

from mongodb_storage_analysis import mongodb_storage
from sentiment_process import log_level_conversor, active_log, message_matrix, \
    message_row, compound_sentiments, emotion_sentiments, reading_json_files

warnings.filterwarnings("ignore", category=DeprecationWarning)

_author__ = 'Xavier Garcia Cabellos'
__date__ = '20190801'
__version__ = 0.01
__description__ = 'This scripts read text and give the sentiment of different mails'

# noinspection PyCompatibility
config_name = './input/config.ini'
json_directory = './json/'

config = configparser.ConfigParser()
config.read(config_name)

module_logger = logging.getLogger('sentiment_analysis')


class sentimentAnalysis:
    title = ""  # for subject
    textId = ""  # For message_id
    writer = ""  # for from email use to have the name and it can be used by signature.

    def __init__(self, log_file=config['LOGS']['LOG_FILE'],
                 log_level=log_level_conversor(config['LOGS']['log_level_sentiment']),
                 log_directory=config['LOGS']['LOG_DIRECTORY']):
        active_log(log_file, log_level, log_directory)
        self.logger = logging.getLogger("sentiment_analysis.SentimentProcess")

    def __del__(self):
        self.logger.debug('Deleting SentimentAnalysis object')

    def process_type(self):
        """"Return a string representing the type of processor this is."""
        return 'sentiment Analysys'

    def reading_json_files(self, path, number__files=10000):
        return reading_json_files(path, number__files)

    def message_list_from_json_file(self, jfile):
        with open(jfile, 'r') as f:
            data = json.load(f)
            return data

    def message_matrix(self, data):
        return message_matrix(data)

    def message_row(self, json_message):
        return message_row(json_message)

    def compound_sentiments(self, hp):
        return compound_sentiments(hp)

    def emotion_sentiments(self, hp):
        return emotion_sentiments(hp)

    def sentiment_analysis_fromfile(self, path=json_directory, number_of_json_files=2):
        # Getting the files
        json_files = self.reading_json_files(path, number_of_json_files)

        # Getting all messages in a matrix
        for jfile in json_files:
            data = self.message_list_from_json_file(jfile)
            hp2 = self.message_matrix(data)
            # hp2 = self.compound_sentiments(hp2)
            hp_df = self.emotion_sentiments(hp2)
        return hp_df

    def sentiment_analysis_fromDDBB(self, json_sentiment_store, collection_name, limit):
        """

        :rtype: object
        """
        db = json_sentiment_store.driver[collection_name]
        emails = db.emails
        data = []
        # testing
        emails_p = [i for i in emails.distinct("_id")]
        emails_m = [i for i in emails.distinct("Message-Id")]
        senders = [i for i in emails.distinct("From")]
        receivers = [i for i in emails.distinct("To")]
        cc_receivers = [i for i in emails.distinct("Cc")]
        bcc_receivers = [i for i in emails.distinct("Bcc")]
        print("Num id: %i", len(emails_p))
        print("Num messageId: %i", len(emails_m))
        print("Num Senders: %i", len(senders))
        print("Num Receivers: %i", len(receivers))
        print("Num CC Receivers: %i", len(cc_receivers))
        print("Num BCC Receivers: %i", len(bcc_receivers))

        if not limit < 0:
            data = [j for j in emails.find().limit(limit)]
        else:
            data = [j for j in emails.find()]
        # Getting all messages in a matrix
        self.logger.info('starting message_matrix ')
        hp2 = self.message_matrix(data)
        # self.logger.info('starting compount sentiments ')
        # hp2 = self.compound_sentiments(hp2)
        self.logger.info('starting emotion sentiments ')
        hp_df = self.emotion_sentiments(hp2)
        self.logger.info('end sentiments ')
        return hp_df


def sentiment_analysis(json_store, company, limit):
    """

    :param limit:
    :param company:
    :type json_store: object
    :rtype: Dataframe
    """
    df = sentimentAnalysis().sentiment_analysis_fromDDBB(json_store, company, limit)
    if json_store is not None:
        if json_store.storage_type() == 'mongodb storage':
            db = json_store.driver[company]
            emotions = db.emotions
            # records = json.loads(df.T.to_json()).values()
            # write in mongoDb
            emotions.insert_many(df.to_dict('records'))
        elif json_store.storage_type() == 'file':
            json_store.store((df.T.to_json()).values(), json_directory, company)
    return df


if __name__ == '__main__':
    # this main is just only for testing. in this file we define the class text_processor that is used in anywhere
    # but for testing text processor methods, it is possible to use it.

    json_store = mongodb_storage()
    json_store.connect('localhost', 'admin', 'Gandalf6981', 'admin')
    company = 'tuitravel-ad'
    sentiment_analysis(json_store, company, 100)
