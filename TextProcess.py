import configparser
import json
import logging
import os
import re
from logging import Logger

import contractions as contractions
import inflect as inflect
import nltk
import unicodedata
from bs4 import BeautifulSoup
from bs4 import Comment
from email_reply_parser import EmailReplyParser
from htmllaundry import sanitize
from nltk.corpus import stopwords

_author__ = 'Xavier Garcia Cabellos'
__date__ = '2019601'
__version__ = 0.01
__description__ = 'This scripts read text and html and break it in different elements'

#######
# Notas: # html2text.html2text(html) para pasar de html a text


# noinspection PyCompatibility
config_name = '../input/config.ini'


def log_level_conversor(level):
    if level == 'INFO':
        return logging.INFO
    elif level == 'DEBUG':
        return logging.DEBUG
    elif level == 'ERROR':
        return logging.ERROR
    elif level == 'WARNING':
        return logging.WARNING
    elif level == 'WARN':
        return logging.WARN
    elif level == 'CRITICAL':
        return logging.CRITICAL
    elif level == 'FATAL':
        return logging.FATAL


config = configparser.ConfigParser()
config.read(config_name)

module_logger = logging.getLogger('TextProcess')
log_name = config[ 'LOGS' ][ 'LOG_FILE' ]
log_directory = config[ 'LOGS' ][ 'LOG_DIRECTORY' ]
log_level_text = log_level_conversor(config[ 'LOGS' ][ 'log_level_text' ])


def active_log(log_name=log_name, level=log_level_text, log_directory=log_directory):
    if log_directory:
        if not os.path.exists(log_directory):
            os.makedirs(log_directory)
        log_path = os.path.join(log_directory, log_name)
    else:
        log_path = log_name

    logging.basicConfig(filename=log_path, level=level,
                        format='%(asctime)s | %(levelname)s | %(name)s | %(message)s', filemode='a')

    logger: Logger = logging.getLogger("TextProcess.text_process")
    # self.logger.debug('Starting text_process logger using v.' + str(__version__) + ' System ' + sys.platform)
    return logger


class text_process:
    # tags for clena html
    VALID_TAGS = [  'body', 'div', 'em', 'p', 'ul', 'li', 'br' ]
    # clean for clean paragrphs
    TEXT_TAGS = [ 'div', 'p', 'ul', 'ol', 'dl' ]
    # for clean the footer. it depend of the lengague and should be manage by locate.
    SIG_REGEX = re.compile(r'(--|__|-\w)|(^Sent from my (\w+\s*){1,3})|(^Enviado desde mi (\w+\s*){1,3})')
    QUOTE_HDR_REGEX = re.compile('On.*wrote:$')
    # clean emails old fasion with > for reply.
    QUOTED_REGEX = re.compile(r'(^>+)', re.DOTALL | re.MULTILINE)
    # for locate the start of reply.
    HEADER_REGEX = re.compile(r'(From|Sent|To|Subject): .+')
    _MULTI_QUOTE_HDR_REGEX = r'(?!On.*On\s.+?wrote:)(On\s(.+?)wrote:)'
    MULTI_QUOTE_HDR_REGEX = re.compile(_MULTI_QUOTE_HDR_REGEX, re.DOTALL | re.MULTILINE)
    MULTI_QUOTE_HDR_REGEX_MULTILINE = re.compile(_MULTI_QUOTE_HDR_REGEX, re.DOTALL)
    document_processed = [ ]
    text = ''
    fragments = [ ]
    # frament is type Paragraph. Get paragraps analysed.
    fragment = None
    # this variable help me to hidden all paragrphs after header, sig or quoted
    found_visible = True
    # this value let mark as hidden or not all after some type of paragrphs. as Quoted, Sig, Header....
    reply_visble = False

    def __init__(self, reply_visible=False, log_file=log_name, log_level=log_level_text, log_directory=log_directory):
        active_log(log_file, log_level, log_directory)
        self.logger = logging.getLogger("TextProcess.text_process")
        self.found_visible = True
        self.reply_visble = reply_visible

    def __del__(self):
        self.document_processed.clear()
        self.fragments.clear()
        self.found_visible = False
        self.reply_visble = False

    def process_type(self):
        """"Return a string representing the type of processor this is."""
        return 'text'

    def clean_json_message(self, json_message):

        self.document_processed.clear()
        for message in json_message[ 'parts' ]:
            if message[ 'contentType' ] == 'text/html':
                #message[ 'content' ] = sanitize(self.clean_html(message[ 'content' ]))
                clean_text0=self.prepare_paragraphs(message[ 'content' ])
                clean_text1=self.clean_html(clean_text0)
                clean_text2=sanitize(clean_text1)
                message[ 'content' ] = clean_text2
                soup = BeautifulSoup(message[ 'content' ], 'lxml')
                paraphs = soup.find_all(self.TEXT_TAGS)



                for p in paraphs:
                    text = p.getText().rstrip()
                    if text != '':
                        self.scan_paraph(text)
                        self._finish_fragment()
                        self.document_processed.append(text)
            elif message[ 'contentType' ] == 'text/plain':
                paraphs = self.split_paragraphs(message[ 'content' ])
                paraphs_list = [ ]
                for p in paraphs:
                    text = p
                    if text != '':
                        self.scan_paraph(text)
                        self._finish_fragment()
                        self.document_processed.append(text)

        return json_message

    def scan_paraph(self, line):
        """ Reviews each line in email message and determines fragment type

            line - a row of text from an email message
        """
        is_quote_header = self.QUOTE_HDR_REGEX.search(line) is not None
        is_quoted = self.QUOTED_REGEX.search(line) is not None
        is_header = is_quote_header or self.HEADER_REGEX.search(line) is not None
        hidden = False

        if is_quote_header or is_header or is_quoted or not self.found_visible:
            if not self.reply_visble:
                if self.fragment:
                    self.fragment.hidden = True
                self.found_visible = False

        if self.fragment is None:
            self.fragment = Paragraph(is_quoted, '', headers=is_header, hidden=hidden)
            # self.found_visible=True

        if self.SIG_REGEX.match(line.strip()):
            self.fragment.signature = True
            if not self.reply_visble:
                self.found_visible = False
                self.fragment.hidden = True

            # print('sign Paraph: {},{},{}:{},{}'.format(is_quote_header, is_quoted, is_header, self.fragment.hidden,
            #                                       self.found_visible))
            self._finish_fragment()

        if self.fragment \
                and ((self.fragment.headers == is_header and self.fragment.quoted == is_quoted) or
                     (self.fragment.quoted and (is_quote_header or len(line.strip()) == 0))):

            self.fragment.lines.append(line)
            if is_quote_header or is_header or is_quoted or not self.found_visible:
                if not self.reply_visble:
                    hidden = True
                    self.found_visible = False
                    self.fragment.hidden = True
            #     print('Paraph not: {},{},{}:{},{}'.format(is_quote_header, is_quoted, is_header, self.fragment.hidden,
            #                                            self.found_visible))
            # else:
            #      print('Paraph: {},{},{}:{},{}'.format(is_quote_header, is_quoted, is_header, self.fragment.hidden,
            #                                                self.found_visible))

        else:

            if is_quote_header or is_header or is_quoted or not self.found_visible:
                hidden = True
            if self.fragment and not self.reply_visble:
                self.fragment.hidden = True
            self._finish_fragment()
            self.fragment = Paragraph(is_quoted, line, headers=is_header, hidden=hidden)
            # print('New paraph: {},{},{}:{},{}'.format(is_quote_header,is_quoted,is_header,
            # self.fragment.hidden,self.found_visible))

    def quote_header(self, line):
        """ Determines whether line is part of a quoted area

            line - a row of the email message

            Returns True or False
        """
        return self.QUOTE_HDR_REGEX.match(line[ ::-1 ]) is not None

    def _finish_fragment(self):
        """ Creates fragment
        """

        if self.fragment:
            self.fragment.finish()
            if self.fragment.headers:
                # Regardless of what's been seen to this point, if we encounter a headers fragment,
                # all the previous fragments should be marked hidden and found_visible set to False.
                if not self.reply_visble:
                    self.found_visible = False
                    # for f in self.fragments:
                    self.fragment.hidden = True
            if not self.found_visible and not self.reply_visble:
                if self.fragment.quoted \
                        or self.fragment.headers \
                        or self.fragment.signature \
                        or (len(self.fragment.content.strip()) == 0):
                    self.fragment.hidden = True
                # else:
                #     self.found_visible = True
            self.fragments.append(self.fragment)
        self.fragment = None

    @staticmethod
    def parse_email(email_message):
        return EmailReplyParser.read(email_message)

    @staticmethod
    def parse_reply(email_message):
        return EmailReplyParser.parse_reply(email_message)

    # def processor(self, text):
    #
    @staticmethod
    def clean_email(text):
        #return text.replace('---','\n---').replace('__','\n__').replace('From: ','\nFrom:')
        #return text.replace('---','\n\t\t\t---').replace('__','\n\t\t\t__')
        return text.replace('/(^\w.+:\n)?(^>.*(\n|$))+/mi', '')

    @staticmethod
    def split_paragraphs(text):
        # paragraphs=re.split( '\n\s{3,}',text)
        paragraphs = re.split('\s{4,}', text)
        return paragraphs

    @staticmethod
    def prepare_paragraphs(text):
        ######################3
        # Be careful, it make nothing. just only useful for include cleaning functions.
        #return (text)
        text_out=text.replace('-----Original Appointment-----' , '</p><p>-----Original Appointment-----').\
            replace('From:' , '<\p><p>From:')

        return text_out

    @staticmethod
    def sanitize_html(soup):

        for tag in soup.findAll(True):
            if tag.name not in text_process.VALID_TAGS:
                tag.hidden = True

        return soup

    @staticmethod
    def strip_html(text):
        soup = BeautifulSoup(text, 'lxml')  # "html.parser")
        for tag in soup.findAll(True):
            if tag.name not in text_process.VALID_TAGS:
                tag.hidden = True

        text_out = soup.get_text()
        return text_out

    @staticmethod
    def clean_html(text):
        soup = BeautifulSoup(text, 'lxml')  # "html.parser")
        for tag in soup.findAll(True):
            if tag.name not in text_process.VALID_TAGS:
                tag.hidden = True
            if isinstance(tag, Comment):
                tag.extract()

        text_out = " ".join(soup.prettify().split())
        #text_out = soup.prettify()
        return text_out

    @staticmethod
    def remove_between_square_brackets(text):
        return re.sub('\[[^]]*\]', '', text)

    @staticmethod
    def denoise_text(text):
        text = text_process.strip_html(text)
        text = text_process.remove_between_square_brackets(text)
        return text

    @staticmethod
    def replace_contractions(text):
        """Replace contractions in string of text"""
        return contractions.fix(text)

    @staticmethod
    def remove_non_ascii(words):
        """Remove non-ASCII characters from list of tokenized words"""
        new_words = [ ]
        for word in words:
            new_word = unicodedata.normalize('NFKD', word).encode('ascii', 'ignore').decode('utf-8', 'ignore')
            new_words.append(new_word)
        return new_words

    @staticmethod
    def to_lowercase(words):
        """Convert all characters to lowercase from list of tokenized words"""
        new_words = [ ]
        for word in words:
            new_word = word.lower()
            new_words.append(new_word)
        return new_words

    @staticmethod
    def remove_punctuation(words):
        """Remove punctuation from list of tokenized words"""
        new_words = [ ]
        for word in words:
            new_word = re.sub(r'[^\w\s]', '', word)
            if new_word != '':
                new_words.append(new_word)
        return new_words

    @staticmethod
    def replace_numbers(words):
        """Replace all interger occurrences in list of tokenized words with textual representation"""
        p = inflect.engine()
        new_words = [ ]
        for word in words:
            if word.isdigit():
                new_word = p.number_to_words(word)
                new_words.append(new_word)
            else:
                new_words.append(word)
        return new_words

    @staticmethod
    def remove_stopwords(words):
        """Remove stop words from list of tokenized words"""
        new_words = [ ]
        for word in words:
            if word not in stopwords.words('english'):
                new_words.append(word)
        return new_words

    @staticmethod
    def stem_words(words):
        """Stem words in list of tokenized words"""
        stemmer = nltk.LancasterStemmer()
        stems = [ ]
        for word in words:
            stem = stemmer.stem(word)
            stems.append(stem)
        return stems

    @staticmethod
    def lemmatize_verbs(words):
        """Lemmatize verbs in list of tokenized words"""
        lemmatizer = nltk.WordNetLemmatizer()
        lemmas = [ ]
        for word in words:
            lemma = lemmatizer.lemmatize(word, pos='v')
            lemmas.append(lemma)
        return lemmas

    @staticmethod
    def normalize(words):
        words = text_process.remove_non_ascii(words)
        words = text_process.to_lowercase(words)
        words = text_process.remove_punctuation(words)
        words = text_process.replace_numbers(words)
        words = text_process.remove_stopwords(words)
        return words


class Paragraph(object):
    """ A Fragment is a part of
        an Email Message, labeling each part.
    """
    hidden = True

    def __init__(self, quoted, first_line, headers=False, hidden=False):
        self.signature = False
        self.headers = headers
        if headers or quoted:
            self.hidden = True
        else:
            self.hidden = hidden
        self.quoted = quoted
        self._content = None
        self.lines = [ first_line ]

    def finish(self):
        """ Creates block of content with lines
            belonging to fragment.
        """
        # self.lines.reverse()
        self._content = '\n'.join(self.lines)
        self.lines = None

    @property
    def content(self):
        return self._content.strip()


def main():
    sample = """<h1>Title Goes Here</h1>
        <b>Bolded Text</b>
        <i>Italicized Text</i>
        <img src="this should all be gone"/>
        <a href="this will be gone, too">But this will still be here!</a>
        I run. He ran. She is running. Will they stop running?
        I talked. She was talking. They talked to them about running. Who ran to the talking runner?
        [Some text we don't want to keep is in here]
        ¡Sebastián, Nicolás, Alejandro and Jéronimo are going to the store tomorrow morning!
        something... is! wrong() with.,; this :: sentence.
        I can't do this anymore. I didn't know them. Why couldn't you have dinner at the restaurant?
        My favorite movie franchises, in order: Indiana Jones; Marvel Cinematic Universe; Star Wars; Back to the Future; Harry Potter.
        Don't do it.... Just don't. Billy! I know what you're doing. This is a great little house you've got here.
        [This is some other unwanted text]
        John: "Well, well, well."
        James: "There, there. There, there."
        &nbsp;&nbsp;
        There are a lot of reasons not to do this. There are 101 reasons not to do it. 1000000 reasons, actually.
        I have to go get 2 tutus from 2 different stores, too.
        22    45   1067   445
        {{Here is some stuff inside of double curly braces.}}
        {Here is more stuff in single curly braces.}
        [DELETE]
        </body>
        </html>"""

    #  sample = text_process.denoise_text(sample)
    #  print(sample)
    #  sample = text_process.replace_contractions(sample)
    #  print(sample)
    #  #nltk.download('punkt')
    #  words = nltk.word_tokenize(sample)
    #  print(words)
    # # nltk.download('stopwords')
    #  words = text_process.normalize(words)
    #  print(words)

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

    #     for message in doc['parts']:
    #         if message['contentType']=='text/html':
    #             message['content']=text_process.clean_html(message['content'])
    #     data_json = loads(json.dumps(doc))
    #     try:
    #         html=data_json['parts'][0]['content']
    #         print("#################################################")
    #         # print(html)
    #         # print('-------------------------------------------------')
    #         # print(EbookProcess.extract_content_with_arc90(html))
    #         # print('--------------------------------------------------')
    #         # print(text_process.strip_html(html))
    #         # print('-------------------------------------------------')
    #         html_ok=sanitize(html)
    #         #print(html_ok)
    #         #print('-------------------------------------------------')
    #         soup = BeautifulSoup(html_ok, 'lxml')
    #         page = soup.find_all(text_process.VALID_TAGS)
    #         for p in page:
    #             print(p.getText())
    #             print('-------------------------------------------------')
    #     except Exception as err:
    #         print(err)


if __name__ == '__main__':
    #thiis main is just only for trsting. in this file we define the class text_processor that is used in amywhere
    #but for testing text processor methosd, it is possible to use it.
    main()
