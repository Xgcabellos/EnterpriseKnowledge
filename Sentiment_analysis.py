import configparser
import json
import logging
import warnings

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

    def sentiment_analysis_fromfile(self, source, target):
        path = json_directory
        number_of_json_files = 2

        # Getting the files
        json_files = self.reading_json_files(path, number_of_json_files)

        # Getting all messages in a matrix
        for jfile in json_files:
            data = self.message_list_from_json_file(jfile)
            # for doc in data:
            #     hp = self.message_row(doc)
            #     # hp = message_matrix(jfile)
            #     hp = self.compound_sentiments(hp)

            hp2 = self.message_matrix(data)
            hp2 = self.compound_sentiments(hp2)
            hp_df = self.emotion_sentiments(hp2)
        return 0


if __name__ == '__main__':
    # this main is just only for testing. in this file we define the class text_processor that is used in anywhere
    # but for testing text processor methods, it is possible to use it.

    sentimentAnalysis().sentiment_analysis(source, target)
