import configparser
import json
import logging
from logging import Logger

from EmailProcessor import active_log
from TextProcess import log_level_conversor, text_process

_author__ = 'Xavier Garcia Cabellos'
__date__ = '2019601'
__version__ = 0.01
__description__ = 'This scripts read messages and process for relationship'

config_name = '../input/config.ini'

config = configparser.ConfigParser()
config.read(config_name)

module_logger = logging.getLogger('TextProcess')
log_name = config[ 'LOGS' ][ 'LOG_FILE' ]
log_directory = config[ 'LOGS' ][ 'LOG_DIRECTORY' ]
log_level_text = log_level_conversor(config[ 'LOGS' ][ 'log_level_text' ])


class message_process:
    # tags for clena html
    text = None
    thread_index = ''

    def __init__(self, log_file=log_name, log_level=log_level_text, log_directory=log_directory):
        active_log(log_file, log_level, log_directory)
        self.logger: Logger = logging.getLogger("MessageProcess.message_process")
        self.text = text_process()

    def __del__(self):
        self.text = None

    def process_type(self):
        """"Return a string representing the type of processor this is."""
        return 'message'

    def clean_json_message(self, json_message):
        self.text.clean_json_message(json_message)
        self.document_processed.clear()
        if 'Thread-Index' in json_message:
            self.thread_index = json_message[ 'Thread-Index' ]
        return json_message


def main():
    json_directory = './json/'

    with open(json_directory + 'xgarcia.tuitravel-ad.net.200.INBOX.json', 'r') as f:
        data = json.load(f)
    json_list = [ ]

    for doc in data:
        proc = text_process(False)  # not visialization of replies
        print("#################################################")
        proc.clean_json_message(doc)

        print('-------------------------------------------------------------------------------------------')
        number_of_seen = 0
        for p in proc.fragments:
            # print('-------------------------------------------------------------------------------------------')
            if not p.hidden:
                print(p.content)
                # print(doc[ 'parts' ][0][ 'contentType' ])
                number_of_seen += 1
            # else:
            #   print('hidden------------------------------------------------------------------------')
        print('-------------------------------------------------------------------------------------------')
        if number_of_seen == 0:
            print(doc[ 'parts' ][ 0 ][ 'content' ])
            print('-------------------------------------------------------------------------------------------')
        proc = None


if __name__ == '__main__':
    main()
