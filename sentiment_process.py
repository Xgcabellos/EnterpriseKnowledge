import configparser
import json
import logging
import os
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


# nltk
from nltk import tokenize, wordpunct_tokenize
import pandas as pd

# spaCy

pd.set_option('display.max_rows', 500)

_author__ = 'Xavier Garcia Cabellos'
__date__ = '20190801'
__version__ = 0.01
__description__ = 'This scripts read text and give the sentiment of different mails'

# noinspection PyCompatibility
config_name = './input/config.ini'
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
filepathml = ('data/'
              'NRC-Sentiment-Emotion-Lexicons/'
              'NRC-Emotion-Lexicon-v0.92/'
              'NRC-Emotion-Lexicon-v0.92-In105Languages-Nov2017Translations.xlsx')

# emolex_df = pd.read_csv(filepath, names=["word", "emotion", "association"], sep='\t')
# emolex_words = emolex_df.pivot(index='word', columns='emotion', values='association').reset_index()
# emotions = emolex_words.columns.drop('word')
# Load info for spaCy find language
# nlp = spacy.load("en")
# nlp.add_pipe(LanguageDetector(), name="language_detector", last=True)

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
    # module_logger.info('detect_language...')
    if ratios[mostLang] > 0:
        if mostLang not in ['azerbaijani', 'romanian', 'hungarian']:
            return mostLang
    return DEFAULT_LANGUAGE


#
# def detect_language_spacy(text):
#     # aprox 60 times slower
#     doc = nlp(text)
#
#     # document level language detection. Think of it like average language of document!
#     # print(doc._.language['language'])
#     # sentence level language detection
#     # for i, sent in enumerate(doc.sents):
#     #    print(sent, sent._.language)
#     return doc._.language


def text_emotion(df, column):
    """
    Takes a DataFrame and a specified column of text and adds 10 columns to the
    DataFrame for each of the 10 emotions in the NRC Emotion Lexicon, with each
    column containing the value of the text in that emotions
    INPUT: DataFrame, string
    OUTPUT: the original DataFrame with ten new columns
    """
    LANGUAGES_ACCEPTED = ['english', 'spanish', 'french', 'german']
    new_df = df.copy()

    emolex_dfml = pd.read_excel(filepathml)
    print(emolex_dfml.head())
    fields_english = emolex_dfml.loc[:, 'English (en)']
    fields_spanish = emolex_dfml.loc[:, 'Spanish (es)']
    fields_french = emolex_dfml.loc[:, 'French (fr)']
    fields_german = emolex_dfml.loc[:, 'German (de)']

    emotions_ml = emolex_dfml.columns.to_list()
    emotions_ml = emotions_ml[105:]
    emotions_only = emolex_dfml.loc[:, emotions_ml]

    emolex_english = pd.concat([fields_english, emotions_only], axis=1)
    emolex_english.set_index('English (en)')
    emolex_spanish = pd.concat([fields_spanish, emotions_only], axis=1)
    emolex_spanish.set_index('Spanish (es)')
    emolex_french = pd.concat([fields_french, emotions_only], axis=1)
    emolex_french.set_index('French (fr)')
    emolex_german = pd.concat([fields_german, emotions_only], axis=1)
    emolex_german.set_index('German (de)')

    stemmer_english = nltk.SnowballStemmer('english')
    stemmer_spanish = nltk.SnowballStemmer('spanish')
    stemmer_french = nltk.SnowballStemmer('french')
    stemmer_german = nltk.SnowballStemmer('german')



    emo_df = pd.DataFrame(0, index=df.index, columns=emotions_ml)

    stemmer = None  # nltk.SnowballStemmer("english")

    file = ''
    message_Id = ''
    language = ''
    language2 = ''
    column_lang_name = 'English (en)'
    emolex = None
    lemmatizer = None
    for i, row in new_df.iterrows():

        if row['message_Id'] != message_Id:
            # print('   ', row['message_Id'])
            message_Id = row['message_Id']
        if row['language'] != language:
            # print('   ', row['message_Id'])
            language = row['language']
            if language not in LANGUAGES_ACCEPTED:
                language = detect_language(new_df.loc[i][column])
                if language not in LANGUAGES_ACCEPTED:
                    language = DEFAULT_LANGUAGE
        # if row['language2'] != language2:
        #     # print('   ', row['message_Id'])
        #     language2 = row['language2']
        #     if language2 == 'ca':
        #         language2 = 'spanish'
        #     elif language2 == 'es':
        #         language2 = 'spanish'
        #     else:
        #         language2 = 'english'
        if language == 'spanish':
            stemmer = stemmer_spanish

            column_lang_name = 'Spanish (es)'
            emolex = emolex_spanish
        elif language == 'english':
            stemmer = stemmer_english
            column_lang_name = 'English (en)'
            emolex = emolex_english

        elif language == 'french':
            stemmer = stemmer_french
            column_lang_name = 'French (fr)'
            emolex = emolex_french

        elif language == 'german':
            stemmer = stemmer_german
            column_lang_name = 'German (de)'
            emolex = emolex_german

        else:
            try:
                stemmer = nltk.SnowballStemmer(language)

                emolex = emolex_dfml
            except Exception as e:
                module_logger.error(str(e))
                stemmer = stemmer_english
                column_lang_name = 'English (en)'
                emolex = emolex_english

        try:
            module_logger.info('tokenize {}'.format(str(new_df.loc[i][column])))
            text = text_process.TextProcess.lemmatizer(new_df.loc[i][column], language)
            document = text_process.TextProcess.normalize_text(text)
            # if language == 'english':
            #     document = text_process.TextProcess.lemmatize_verbs(document)

            # for word in document:
            #     word = stemmer.stem(word.lower())
            #     emo_score = emolex_words[emolex_words.word == word]
            #     if not emo_score.empty:
            #         for emotion in list(emotions):
            #             emo_df.at[i, emotion] += emo_score[emotion]
            lengh = len(document)
            for word in document:
                # word = stemmer.stem(word.lower())

                emo_score = emolex[emolex.loc[:, column_lang_name] == word]
                # emo_score = emolex[emolex[column_lang_name].str.match(word, na=False)]

                if not emo_score.empty:

                    for emotion in list(emotions_ml):
                        # print('word:{}, emo_score:{}'.format(word,str(emo_score[emotion])))

                        l = len(emo_score[emotion].index)
                        # be careful. sometimes return more than one. i have chosen the first.
                        if l == 1:
                            emo_df.at[i, emotion] += emo_score[emotion]
                        else:
                            emo_df.at[i, emotion] += emo_score.iloc[0][emotion]

            module_logger.info(
                'words:{}, row{}, language:{},  anger:{},  anticipation:{},  disgust:{},  fear:{},  joy:{}, '
                ' negative:{},  positive:{},  sadness:{},  surprise:{},  trust:{}'
                    .format(lengh, str(row['subject']), language, emo_df.loc[i].at['Anger'],
                            emo_df.loc[i].at['Anticipation'],
                            emo_df.loc[i].at['Disgust'],
                            emo_df.loc[i].at['Fear'], emo_df.loc[i].at['Joy'], emo_df.loc[i].at['Negative'],
                            emo_df.loc[i].at['Positive'], emo_df.loc[i].at['Sadness'],
                            emo_df.loc[i].at['Surprise'],
                            emo_df.loc[i].at['Trust']))
            module_logger.info(
                '----------------------------------------------------------------------------------------------')
        except Exception as normalizeException:
            module_logger.error(normalizeException)

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


def message_matrix(data):
    hp = defaultdict(dict)
    doc_list = []

    for doc in data:
        proc = text_process.TextProcess(True)  # not visualization of replies
        try:
            proc.paraphs_message(doc)
            doc_list.append(proc)
        except Exception as TextProcessException:
            module_logger.error(TextProcessException)
    module_logger.info('processing {} messages....'.format(str(len(doc_list))))

    # write matrix with a list of paragraphs

    for messageInfo in doc_list:
        number_of_seen = 0
        list_fragments = []
        languages = []

        for p in messageInfo.fragments:
            if not p.hidden:
                number_of_seen += 1
                language = detect_language(p.content)
                list_fragments.append(p.content)
                # language = detect_language(p.content)
                # languages.append(language)

        hp[messageInfo.textId] = (messageInfo.title, list_fragments)

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
                         sentiments, lang, lang2)

        # module_logger.info('{:45} compound:{:.3f},neg:{:.3f},neu:{:.3f}, pos:{:.3f}'
        #                    .format(hp[text_id][0], hp[text_id][3]['compound'],
        #                            hp[text_id][3]['neg'],
        #                            hp[text_id][3]['neu'],
        #                            hp[text_id][3]['pos']))
        # module_logger.info('{:45} compound:{:.3f}, languages: {}, main language: {}, language 2 {}'
        module_logger.debug('{:45} compound:{:.3f}, main language: {}, language2: {}'
                            .format(hp[messageId][0], hp[messageId][2]['compound'],
                                    str(hp[messageId][3]),
                                    str(hp[messageId][4]['language'])))

    return hp


def emotion_sentiments(hp):
    '''
    # Other way of sentiment analysis.  NRC emotion lexicon

    '''
    p = None
    # data = {'file': [], 'message_Id': [], 'text': [], 'language': [], 'language2': [], 'compound': []}
    data = {'message_Id': [], 'subject': [], 'text': [], 'language': []}
    hp_df = None

    for messageId in hp:
        subject = hp[messageId][0]
        #         print('   ', chapter, title)
        text = ' '.join(hp[messageId][1]).replace('\n', '').replace('\r', ' ')
        language = detect_language(text)
        data['message_Id'].append(messageId)
        data['subject'].append(subject)
        data['text'].append(text)
        data['language'].append(language)

    hp_df = pd.DataFrame(data=data)

    hp_df = text_emotion(hp_df, 'text')

    index = hp_df.shape[0]
    index2 = 0
    while index2 < index:
        module_logger.info(
            'subject:{} , language:{}, anger:{},  anticipation:{},  disgust:{},  fear:{},  joy:{}, '
            ' negative:{},  positive:{},  sadness:{},  surprise:{},  trust:{}'
                .format(hp_df.loc[index2].at['subject'], hp_df.loc[index2].at['language'],
                        hp_df.loc[index2].at['Anger'], hp_df.loc[index2].at['Anticipation'],
                        hp_df.loc[index2].at['Disgust'],
                        hp_df.loc[index2].at['Fear'], hp_df.loc[index2].at['Joy'], hp_df.loc[index2].at['Negative'],
                        hp_df.loc[index2].at['Positive'], hp_df.loc[index2].at['Sadness'],
                        hp_df.loc[index2].at['Surprise'],
                        hp_df.loc[index2].at['Trust']))
        index2 += 1
    # hp_df.iloc[index2, : ])
    data = {'message_Id': [], 'subject': [], 'text': [], 'language': []}
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
        # hp2 = compound_sentiments(hp2)
        hp_df = emotion_sentiments(hp2)


if __name__ == '__main__':
    # this main is just only for testing. in this file we define the class text_processor that is used in anywhere
    # but for testing text processor methods, it is possible to use it.
    main()
