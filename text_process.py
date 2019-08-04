import configparser
import json
import logging
import re
import unicodedata
import warnings
from collections import defaultdict
from logging import Logger

import contractions
import inflect as inflect
import nltk
from bs4 import BeautifulSoup
from bs4 import Comment
# from email_reply_parser import EmailReplyParser
from htmllaundry import sanitize
from nltk.corpus import stopwords
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

warnings.filterwarnings("ignore", category=DeprecationWarning)

# nltk.download()

# Plotting tools
import matplotlib.pyplot as plt

plt.style.use('fivethirtyeight')
# %matplotlib  inline
import os
# Set Pandas to display all rows of dataframes

# nltk
from nltk import tokenize, wordpunct_tokenize
import pandas as pd
from nltk import word_tokenize

pd.set_option('display.max_rows', 500)

_author__ = 'Xavier Garcia Cabellos'
__date__ = '2019601'
__version__ = 0.01
__description__ = 'This scripts read text and html and break it in different elements'

#######
# Notas: # html2text.html2text(html) para pasar de html a text


# noinspection PyCompatibility
config_name = '../input/config.ini'
json_directory = './json/'


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
log_name = config['LOGS']['LOG_FILE']
log_directory = config['LOGS']['LOG_DIRECTORY']
log_level_text = log_level_conversor(config['LOGS']['log_level_text'])


def active_log(log_name=log_name, level=log_level_text, log_directory=log_directory):
    if log_directory:
        if not os.path.exists(log_directory):
            os.makedirs(log_directory)
        log_path = os.path.join(log_directory, log_name)
    else:
        log_path = log_name

    logging.basicConfig(filename=log_path, level=level,
                        format='%(asctime)s | %(levelname)s | %(name)s | %(message)s', filemode='a')

    logger: Logger = logging.getLogger("TextProcess.TextProcess")
    # self.logger.debug('Starting TextProcess logger using v.' + str(__version__) + ' System ' + sys.platform)
    return logger


class TextProcess:
    # tags for clena html
    VALID_TAGS = ['body', 'div', 'em', 'p', 'ul', 'li', 'br']
    # clean for clean paragrphs
    TEXT_TAGS = ['div', 'p', 'ul', 'ol', 'dl']
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
    document_processed = []
    text = ''
    fragments = []
    # frament is type Paragraph. Get paragraps analysed.
    fragment = None
    # this variable help me to hidden all paragrphs after header, sig or quoted
    found_visible = True
    # this value let mark as hidden or not all after some type of paragrphs. as Quoted, Sig, Header....
    reply_visble = False
    json_doc = None
    title = ""  # for subject
    textId = ""  # For message_id
    writer = ""  # for from email use to have the name and it can be used by signature.

    def __init__(self, reply_visible=False, log_file=log_name, log_level=log_level_text, log_directory=log_directory):
        active_log(log_file, log_level, log_directory)
        self.logger = logging.getLogger("TextProcess.TextProcess")
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
        self.json_doc = json_message
        if "Subject" in json_message:
            self.title = json_message["Subject"]
        if "Message-Id" in json_message:
            self.textId = json_message["Message-Id"]
        elif "Message-ID" in json_message:
            self.textId = json_message["Message-ID"]
        else:
            raise Exception(
                " Fatal Error. the message-ID or Message-Id of {}  doesn't exist. It doesn't SHOULD happened".format(
                    self.title))
        self.writer = json_message["From"]

        self.document_processed = []
        self.fragments = []
        for message in json_message['parts']:
            if message['contentType'] == 'text/html':
                # message[ 'content' ] = sanitize(self.clean_html(message[ 'content' ]))
                clean_text0 = self.prepare_paragraphs(message['content'])
                clean_text1 = self.clean_html(clean_text0)
                clean_text2 = sanitize(clean_text1)
                message['content'] = clean_text2
                soup = BeautifulSoup(message['content'], 'lxml')
                paraphs = soup.find_all(self.TEXT_TAGS)

                for p in paraphs:
                    text = p.getText().rstrip()
                    if text != '':
                        self.scan_paraph(text)
                        self._finish_fragment()
                        self.document_processed.append(text)
            elif message['contentType'] == 'text/plain':
                paraphs = self.split_paragraphs(message['content'])
                paraphs_list = []
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
        return self.QUOTE_HDR_REGEX.match(line[::-1]) is not None

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

    # @staticmethod
    # def parse_email(email_message):
    #     return EmailReplyParser.read(email_message)
    #
    # @staticmethod
    # def parse_reply(email_message):
    #     return EmailReplyParser.parse_reply(email_message)

    # def processor(self, text):
    #
    @staticmethod
    def clean_email(text):
        # return text.replace('---','\n---').replace('__','\n__').replace('From: ','\nFrom:')
        # return text.replace('---','\n\t\t\t---').replace('__','\n\t\t\t__')
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
        # return (text)
        text_out = text.replace('-----Original Appointment-----', '</p><p>-----Original Appointment-----'). \
            replace('From:', '<\p><p>From:')

        return text_out

    @staticmethod
    def sanitize_html(soup):

        for tag in soup.findAll(True):
            if tag.name not in TextProcess.VALID_TAGS:
                tag.hidden = True

        return soup

    @staticmethod
    def strip_html(text):
        soup = BeautifulSoup(text, 'lxml')  # "html.parser")
        for tag in soup.findAll(True):
            if tag.name not in TextProcess.VALID_TAGS:
                tag.hidden = True

        text_out = soup.get_text()
        return text_out

    @staticmethod
    def clean_html(text):
        soup = BeautifulSoup(text, 'lxml')  # "html.parser")
        for tag in soup.findAll(True):
            if tag.name not in TextProcess.VALID_TAGS:
                tag.hidden = True
            if isinstance(tag, Comment):
                tag.extract()

        text_out = " ".join(soup.prettify().split())
        # text_out = soup.prettify()
        return text_out

    @staticmethod
    def remove_between_square_brackets(text):
        return re.sub('\[[^]]*\]', '', text)

    @staticmethod
    def denoise_text(text):
        text = TextProcess.strip_html(text)
        text = TextProcess.remove_between_square_brackets(text)
        return text

    @staticmethod
    def replace_contractions(text):
        """Replace contractions in string of text"""
        return contractions.fix(text)

    @staticmethod
    def remove_non_ascii(words):
        """Remove non-ASCII characters from list of tokenized words"""
        new_words = []
        for word in words:
            new_word = unicodedata.normalize('NFKD', word).encode('ascii', 'ignore').decode('utf-8', 'ignore')
            new_words.append(new_word)
        return new_words

    @staticmethod
    def to_lowercase(words):
        """Convert all characters to lowercase from list of tokenized words"""
        new_words = []
        for word in words:
            new_word = word.lower()
            new_words.append(new_word)
        return new_words

    @staticmethod
    def remove_punctuation(words):
        """Remove punctuation from list of tokenized words"""
        new_words = []
        for word in words:
            new_word = re.sub(r'[^\w\s]', '', word)
            if new_word != '':
                new_words.append(new_word)
        return new_words

    @staticmethod
    def replace_numbers(words):
        """Replace all interger occurrences in list of tokenized words with textual representation"""
        p = inflect.engine()
        new_words = []
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
        new_words = []
        for word in words:
            if word not in stopwords.words('english'):
                new_words.append(word)
        return new_words

    @staticmethod
    def stem_words(words):
        """Stem words in list of tokenized words"""
        stemmer = nltk.LancasterStemmer()
        stems = []
        for word in words:
            stem = stemmer.stem(word)
            stems.append(stem)
        return stems

    @staticmethod
    def lemmatize_verbs(words):
        """Lemmatize verbs in list of tokenized words"""
        lemmatizer = nltk.WordNetLemmatizer()
        lemmas = []
        for word in words:
            lemma = lemmatizer.lemmatize(word, pos='v')
            lemmas.append(lemma)
        return lemmas

    @staticmethod
    def normalize(words):
        words = TextProcess.remove_non_ascii(words)
        words = TextProcess.to_lowercase(words)
        words = TextProcess.remove_punctuation(words)
        words = TextProcess.replace_numbers(words)
        words = TextProcess.remove_stopwords(words)
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
        self.lines = [first_line]

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


def compute_ratios(text):
    tokens = wordpunct_tokenize(text)
    words = [word.lower() for word in tokens]

    langratios = {}

    for language in stopwords.fileids():
        stopwords_set = set(stopwords.words(language))
        words_set = set(words)
        common_elements = words_set.intersection(stopwords_set)
        langratios[language] = len(common_elements)

    return langratios


def detect_language(text):
    # it seen more precise than the other. but just only in long test.  better than small ones
    ratios = compute_ratios(text)

    mostLang = max(ratios, key=ratios.get)
    return mostLang


import spacy
from spacy_langdetect import LanguageDetector


def detect_language_spacy(text):
    # aprox 60 times slower

    nlp = spacy.load("en")
    nlp.add_pipe(LanguageDetector(), name="language_detector", last=True)
    doc = nlp(text)

    # document level language detection. Think of it like average language of document!
    # print(doc._.language['language'])
    # sentence level language detection
    # for i, sent in enumerate(doc.sents):
    #    print(sent, sent._.language)
    return doc._.language


filepath = ('data/'
            'NRC-Sentiment-Emotion-Lexicons/'
            'NRC-Emotion-Lexicon-v0.92/'
            'NRC-Emotion-Lexicon-Wordlevel-v0.92.txt')
emolex_df = pd.read_csv(filepath,
                        names=["word", "emotion", "association"],
                        sep='\t')
emolex_words = emolex_df.pivot(index='word',
                               columns='emotion',
                               values='association').reset_index()
emotions = emolex_words.columns.drop('word')


def text_emotion(df, column):
    """
    Takes a DataFrame and a specified column of text and adds 10 columns to the
    DataFrame for each of the 10 emotions in the NRC Emotion Lexicon, with each
    column containing the value of the text in that emotions
    INPUT: DataFrame, string
    OUTPUT: the original DataFrame with ten new columns
    """

    new_df = df.copy()

    emo_df = pd.DataFrame(0, index=df.index, columns=emotions)

    # stemmer = nltk.SnowballStemmer("english")

    file = ''
    message_Id = ''
    language = ''
    language2 = ''
    for i, row in new_df.iterrows():

        # if row['file'] != file:
        #     # print(row['file'])
        #     file = row['file']
        if row['message_Id'] != message_Id:
            # print('   ', row['message_Id'])
            message_Id = row['message_Id']
        if row['language'] != language:
            # print('   ', row['message_Id'])
            language = row['language']
        if row['language2'] != language2:
            # print('   ', row['message_Id'])
            language2 = row['language2']
            if language2 == 'ca':
                language2 = 'spanish'
            elif language2 == 'es':
                language2 = 'spanish'
            else:
                language2 = 'english'
        document = word_tokenize(new_df.loc[i][column])
        stemmer = nltk.SnowballStemmer(language)
        for word in document:
            word = stemmer.stem(word.lower())
            emo_score = emolex_words[emolex_words.word == word]
            if not emo_score.empty:
                for emotion in list(emotions):
                    emo_df.at[i, emotion] += emo_score[emotion]

    new_df = pd.concat([new_df, emo_df], axis=1)

    return new_df


def main():
    DEFAULT_LANGUAGE = 'english'
    path = json_directory

    # Reading all files in json_directory (path)
    json_file = []
    # r=root, d=directories, f = files
    number_of_files = 2
    for r, d, f in os.walk(path):
        for json_f in f:
            if '.json' in json_f:
                if number_of_files > 0:
                    json_file.append(os.path.join(r, json_f))
                number_of_files -= 1

    hp = defaultdict(dict)
    for jfile in json_file:
        with open(jfile, 'r') as f:
            data = json.load(f)
        doc_list = []

        for doc in data:
            proc = TextProcess(False)  # not visualization of replies
            proc.clean_json_message(doc)
            doc_list.append(proc)
        module_logger.info(jfile + ' processing {} messages....'.format(str(len(doc_list))))

        # write matrix with a list of paragraphs

        for messageInfo in doc_list:
            number_of_seen = 0
            list_fragments = []
            languages = {}

            for p in messageInfo.fragments:
                if not p.hidden:
                    number_of_seen += 1
                    list_fragments.append(p.content)
                    language = detect_language(p.content)
                    if language in languages:
                        languages[language] += 1
                    else:
                        languages[language] = 1

            hp[jfile][messageInfo.textId] = (messageInfo.title, list_fragments, languages)

    hp = dict(hp)

    # The compound score is computed by summing the valence scores of each word in the lexicon, adjusted according
    # to the rules, and then normalized to be between -1 (most extreme negative) and +1 (most extreme positive).
    # This is the most useful metric if you want a single unidimensional measure of sentiment for a given sentence.
    # Calling it a 'normalized, weighted composite score' is accurate.
    # i t is also useful for researchers who would like to set standardized thresholds for classifying sentences
    # as either positive, neutral, or negative.Typical threshold values (used in the literature cited on this page)
    # are:

    #
    # positive sentiment: compound score >= 0.05
    # neutral sentiment: (compound score > -0.05) and (compound score < 0.05)
    # negative sentiment: compound score <= -0.05

    # The pos, neu, and neg scores are ratios for proportions of text that fall in each category (so these should all
    # add up to be 1... or close to it with float operation). These are the most useful metrics if you want
    # multidimensional measures of sentiment for a given sentence.
    # https://github.com/cjhutto/vaderSentiment?source=post_page---------------------------

    p = None
    # data = {'file': [], 'message_Id': [], 'text': [], 'language': [], 'language2': [], 'compound': []}
    data = {'message_Id': [], 'text': [], 'language': [], 'language2': [], 'compound': []}
    hp_df = None
    analyzer = SentimentIntensityAnalyzer()
    for json_f in hp:
        index = 0
        for messageId in hp[json_f]:
            sentence_list = []
            for p in hp[json_f][messageId][1]:
                text = p.replace('\n', '').replace('\r', ' ')

                sentence_list += tokenize.sent_tokenize(text)

            sentiments = {'compound': 0.0, 'neg': 0.0, 'neu': 0.0, 'pos': 0.0}

            for sentence in sentence_list:
                vs = analyzer.polarity_scores(sentence)
                sentiments['compound'] += vs['compound']
                sentiments['neg'] += vs['neg']
                sentiments['neu'] += vs['neu']
                sentiments['pos'] += vs['pos']

            if len(sentence_list):
                sentiments['compound'] = sentiments['compound'] / len(sentence_list)
                sentiments['neg'] = sentiments['neg'] / len(sentence_list)
                sentiments['neu'] = sentiments['neu'] / len(sentence_list)
                sentiments['pos'] = sentiments['pos'] / len(sentence_list)

            # lang ='NONE' #
            lang = detect_language(' '.join(sentence_list))
            # if 'arabic' in lang:
            #     lang= DEFAULT_LANGUAGE

            lang2 = {'language': 0}
            # lang2 = detect_language_spacy(' '.join(sentence_list))

            hp[json_f][messageId] = (hp[json_f][messageId][0], hp[json_f][messageId][1],
                                     hp[json_f][messageId][2], sentiments, lang, lang2)
            index += 1
            # module_logger.info('{:45} compound:{:.3f},neg:{:.3f},neu:{:.3f}, pos:{:.3f}'
            #                    .format(hp[json_file_name][text_id][0], hp[json_file_name][text_id][3]['compound'],
            #                            hp[json_file_name][text_id][3]['neg'],
            #                            hp[json_file_name][text_id][3]['neu'],
            #                            hp[json_file_name][text_id][3]['pos']))
            # module_logger.info('{:45} compound:{:.3f}, languages: {}, main language: {}, language 2 {}'
            # module_logger.info('{:45} compound:{:.3f}, main language: {}, language2: {}'
            #                    .format(hp[json_f][messageId][0], hp[json_f][messageId][3]['compound'],
            #                            # str(hp[json_file_name][message_Id][2]),
            #                            str(hp[json_f][messageId][4]),
            #                            str(hp[json_f][messageId][5]['language'])))

            # Other way of sentiment analysis.  NRC emotion lexicon

            title = hp[json_f][messageId][0]
            #         print('   ', chapter, title)
            text = ' '.join(hp[json_f][messageId][1]).replace('\n', '').replace('\r', ' ')
            language = hp[json_f][messageId][4]
            language2 = hp[json_f][messageId][5]
            # data['file'].append(json_f)
            data['message_Id'].append(title)
            data['text'].append(text)
            data['language'].append(language)
            data['language2'].append(language2['language'])
            data['compound'].append(hp[json_f][messageId][3]['compound'])

        hp_df = pd.DataFrame(data=data)
        hp_df = text_emotion(hp_df, 'text')
        # index = hp_df.shape[0]
        module_logger.info(hp_df.to_string())  # hp_df.iloc[index2, : ])

        data = {'message_Id': [], 'text': [], 'language': [], 'language2': [], 'compound': []}
        # compound_sentiments = [hp[json_file_name][text_id][3]['compound'] for json_file_name in hp
        #                     for text_id in hp[json_file_name]]

    json_file_name_indices = {}
    idx = 0
    for json_f in hp:
        start = idx
        for message_Id in hp[json_f]:
            idx += 1
        json_file_name_indices[json_f] = (start, idx)

    sentiment_scores = [
        [hp[json_file_name][textId][3][sentiment] for json_file_name in hp for textId in hp[json_file_name]]
        for sentiment in ['compound', 'neg', 'neu', 'pos']]

    compound_sentiment = sentiment_scores[0]

    for json_f in json_file_name_indices:
        compound = compound_sentiment[
                   json_file_name_indices[json_f][0]: json_file_name_indices[json_f][1]]
        module_logger.info('{:45} {:.2f}%'.format(json_f, 100 * sum(compound) / len(compound)))
    module_logger.info('{:45} {:.2f}%'.format('Across the entire series',
                                              100 * sum(compound_sentiment) / len(compound_sentiment)))

    # Other way of sentiment analysis.  NRC emotion lexicon

    # hp_df = None
    # for json_file_name in hp:
    #     print(json_file_name)
    #     hp_df = None
    #     data = {'file': [], 'message_Id': [], 'text': [], 'language': [], 'language2': [], 'compound': []}
    #     for message_Id in hp[json_file_name]:
    #         title = hp[json_file_name][message_Id][0]
    #         #         print('   ', chapter, title)
    #         text = ' '.join(hp[json_file_name][message_Id][1]).replace('\n', '')
    #         language = hp[json_file_name][message_Id][4]
    #         language2 = hp[json_file_name][message_Id][5]
    #         data['file'].append(json_file_name)
    #         data['message_Id'].append(title)
    #         data['text'].append(text)
    #         data['language'].append(language)
    #         data['language2'].append(language2['language'])
    #         data['compound'].append(hp[json_file_name][message_Id][3]['compound'])
    #
    #     hp_df = pd.DataFrame(data=data)
    #     hp_df = text_emotion(hp_df, 'text')
    #     module_logger.info(hp_df.to_string())


if __name__ == '__main__':
    # this main is just only for testing. in this file we define the class text_processor that is used in anywhere
    # but for testing text processor methods, it is possible to use it.
    main()
