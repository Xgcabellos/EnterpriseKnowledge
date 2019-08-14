import configparser
import json
import logging
import warnings
from collections import defaultdict
from logging import Logger

import nltk
# from email_reply_parser import EmailReplyParser
from nltk.corpus import stopwords
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

import text_process as text_process

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

# spaCy
import spacy
from spacy_langdetect import LanguageDetector

pd.set_option('display.max_rows', 500)

_author__ = 'Xavier Garcia Cabellos'
__date__ = '20190801'
__version__ = 0.01
__description__ = 'This scripts read text and give the sentiment of different mails'

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

module_logger = logging.getLogger('sentiment_process')
log_name = config['LOGS']['LOG_FILE']
log_directory = config['LOGS']['LOG_DIRECTORY']
log_level_text = log_level_conversor(config['LOGS']['log_level_text'])
DEFAULT_LANGUAGE = 'english'


def active_log(log_name=log_name, level=log_level_text, log_directory=log_directory):
    if log_directory:
        if not os.path.exists(log_directory):
            os.makedirs(log_directory)
        log_path = os.path.join(log_directory, log_name)
    else:
        log_path = log_name

    logging.basicConfig(filename=log_path, level=level,
                        format='%(asctime)s | %(levelname)s | %(name)s | %(message)s', filemode='a')

    logger: Logger = logging.getLogger("sentiment_process.SentimentProcess")
    # self.logger.debug('Starting TextProcess logger using v.' + str(__version__) + ' System ' + sys.platform)
    return logger


# load info for NRC - sentiment-Lexicon
filepath = ('data/'
            'NRC-Sentiment-Emotion-Lexicons/'
            'NRC-Emotion-Lexicon-v0.92/'
            'NRC-Emotion-Lexicon-Wordlevel-v0.92.txt')
emolex_df = pd.read_csv(filepath, names=["word", "emotion", "association"], sep='\t')
emolex_words = emolex_df.pivot(index='word', columns='emotion', values='association').reset_index()
emotions = emolex_words.columns.drop('word')

stemmer_english = nltk.SnowballStemmer('english')
stemmer_spanish = nltk.SnowballStemmer('spanish')
stemmer_spanish = nltk.SnowballStemmer('spanish')
stemmer_french = nltk.SnowballStemmer('french')
stemmer_german = nltk.SnowballStemmer('german')
# Load info for spaCy find language
nlp = spacy.load("en")
nlp.add_pipe(LanguageDetector(), name="language_detector", last=True)


class sentimentProcess:
    title = ""  # for subject
    textId = ""  # For message_id
    writer = ""  # for from email use to have the name and it can be used by signature.

    def __init__(self, reply_visible=False, log_file=log_name, log_level=log_level_text, log_directory=log_directory):
        active_log(log_file, log_level, log_directory)
        self.logger = logging.getLogger("sentiment_process.SentimentProcess")

    def __del__(self):
        self.logger.debug('Deleting SentimentProcess object')

    def process_type(self):
        """"Return a string representing the type of processor this is."""
        return 'sentiment'


def compute_ratios(text):
    # Method for calculate the language of the text.

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
    # This method give the most probable language between different options
    # it seen more precise than the other. but just only in long test.  better than small ones
    ratios = compute_ratios(text)

    mostLang = max(ratios, key=ratios.get)
    if ratios[mostLang] > 0:
        return mostLang
    else:
        return DEFAULT_LANGUAGE


def detect_language_spacy(text):
    # aprox 60 times slower
    doc = nlp(text)

    # document level language detection. Think of it like average language of document!
    # print(doc._.language['language'])
    # sentence level language detection
    # for i, sent in enumerate(doc.sents):
    #    print(sent, sent._.language)
    return doc._.language


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

    stemmer = None  # nltk.SnowballStemmer("english")

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
        if language == 'spanish':
            stemmer = stemmer_spanish
        elif language == 'english':
            stemmer = stemmer_english
        elif language == 'french':
            stemmer = stemmer_french
        elif language == 'german':
            stemmer = stemmer_german
        else:
            stemmer = nltk.SnowballStemmer(language)
        document = word_tokenize(new_df.loc[i][column])

        for word in document:
            word = stemmer.stem(word.lower())
            emo_score = emolex_words[emolex_words.word == word]
            if not emo_score.empty:
                for emotion in list(emotions):
                    emo_df.at[i, emotion] += emo_score[emotion]

    new_df = pd.concat([new_df, emo_df], axis=1)

    return new_df


def reading_json_files(path, number__files=10000):
    # Reading all files in json_directory (path)
    # Number__files is for limitation of  the number files
    json_file = []
    # r=root, d=directories, f = files
    number_of_files = number__files
    for r, d, f in os.walk(path):
        for json_f in f:
            if '.json' in json_f:
                if number_of_files > 0:
                    json_file.append(os.path.join(r, json_f))
                number_of_files -= 1
    return json_file


def message_matrix(jfile):
    hp = defaultdict(dict)

    with open(jfile, 'r') as f:
        data = json.load(f)
    doc_list = []

    for doc in data:
        proc = text_process.TextProcess(True)  # not visualization of replies
        proc.clean_json_message(doc)
        doc_list.append(proc)
    module_logger.info(jfile + ' processing {} messages....'.format(str(len(doc_list))))

    # write matrix with a list of paragraphs

    for messageInfo in doc_list:
        number_of_seen = 0
        list_fragments = []
        languages = []

        for p in messageInfo.fragments:
            if not p.hidden:
                number_of_seen += 1
                list_fragments.append(p.content)
                language = detect_language(p.content)
                languages.append(language)

        hp[messageInfo.textId] = (messageInfo.title, list_fragments, languages)

    hp = dict(hp)
    return hp


def message_row(json_message):
    # prepare structure
    hp = defaultdict(dict)
    # Process message
    messageInfo = text_process.TextProcess(False)  # not visualization of replies
    messageInfo.clean_json_message(json_message)
    module_logger.debug('Processing  message {}'.format(messageInfo.textId))

    # write row with a list of paragraphs
    number_of_seen = 0
    list_fragments = []
    languages = []

    for p in messageInfo.fragments:
        if not p.hidden:
            number_of_seen += 1
            list_fragments.append(p.content)
            language = detect_language(p.content)
            languages.append(language)

    hp[messageInfo.textId] = (messageInfo.title, list_fragments, languages)

    hp = dict(hp)
    return hp


def compound_sentiments(hp):
    '''
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
    '''

    p = None
    analyzer = SentimentIntensityAnalyzer()
    for messageId in hp:
        sentence_list = []
        index = 0
        for p in hp[messageId][1]:
            text = p.replace('\n', '').replace('\r', ' ')
            sentence_list += tokenize.sent_tokenize(text)  # western language work right and  similar
            index += 1
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

        hp[messageId] = (hp[messageId][0], hp[messageId][1],
                         hp[messageId][2], sentiments, lang, lang2)

        # module_logger.info('{:45} compound:{:.3f},neg:{:.3f},neu:{:.3f}, pos:{:.3f}'
        #                    .format(hp[text_id][0], hp[text_id][3]['compound'],
        #                            hp[text_id][3]['neg'],
        #                            hp[text_id][3]['neu'],
        #                            hp[text_id][3]['pos']))
        # module_logger.info('{:45} compound:{:.3f}, languages: {}, main language: {}, language 2 {}'
        module_logger.info('{:45} compound:{:.3f}, main language: {}, language2: {}'
                           .format(hp[messageId][0], hp[messageId][3]['compound'],
                                   # str(hp[message_Id][2]),
                                   str(hp[messageId][4]),
                                   str(hp[messageId][5]['language'])))

    return hp


def emotion_sentiments(hp):
    '''
    # Other way of sentiment analysis.  NRC emotion lexicon

    '''
    p = None
    # data = {'file': [], 'message_Id': [], 'text': [], 'language': [], 'language2': [], 'compound': []}
    data = {'message_Id': [], 'subject': [], 'text': [], 'language': [], 'language2': [], 'compound': []}
    hp_df = None

    for messageId in hp:
        subject = hp[messageId][0]
        #         print('   ', chapter, title)
        text = ' '.join(hp[messageId][1]).replace('\n', '').replace('\r', ' ')
        language = hp[messageId][4]
        language2 = hp[messageId][5]
        # data['file'].append(json_f)
        data['message_Id'].append(messageId)
        data['subject'].append(subject)
        data['text'].append(text)
        data['language'].append(language)
        data['language2'].append(language2['language'])
        data['compound'].append(hp[messageId][3]['compound'])

    hp_df = pd.DataFrame(data=data)
    hp_df = text_emotion(hp_df, 'text')
    index = hp_df.shape[0]
    index2 = 0
    while index2 < index:
        module_logger.info(
            'subject:{} , language:{}, compound:{:.3f}, anger:{},  anticipation:{},  disgust:{},  fear:{},  joy:{}, '
            ' negative:{},  positive:{},  sadness:{},  surprise:{},  trust:{}'
                .format(hp_df.loc[index2].at['subject'], hp_df.loc[index2].at['language'],
                        hp_df.loc[index2].at['compound'],
                        hp_df.loc[index2].at['anger'], hp_df.loc[index2].at['anticipation'],
                        hp_df.loc[index2].at['disgust'],
                        hp_df.loc[index2].at['fear'], hp_df.loc[index2].at['joy'], hp_df.loc[index2].at['negative'],
                        hp_df.loc[index2].at['positive'], hp_df.loc[index2].at['sadness'],
                        hp_df.loc[index2].at['surprise'],
                        hp_df.loc[index2].at['trust']))
        index2 += 1
    # hp_df.iloc[index2, : ])
    data = {'message_Id': [], 'subject': [], 'text': [], 'language': [], 'language2': [], 'compound': []}
    return hp_df


def main():
    path = json_directory
    number_of_json_files = 2

    # Getting the files
    json_files = reading_json_files(path, number_of_json_files)

    # Getting all messages in a matrix
    for jfile in json_files:
        with open(jfile, 'r') as f:
            data = json.load(f)

        for doc in data:
            hp = message_row(doc)
            # hp = message_matrix(jfile)
            hp = compound_sentiments(hp)

        hp2 = message_matrix(jfile)
        hp2 = compound_sentiments(hp2)
        hp_df = emotion_sentiments(hp2)


if __name__ == '__main__':
    # this main is just only for testing. in this file we define the class text_processor that is used in anywhere
    # but for testing text processor methods, it is possible to use it.
    main()
