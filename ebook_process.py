import logging
import os
import re
import sys
from typing import List

import ebooklib
from bs4 import BeautifulSoup, NavigableString
from bs4 import Comment
from bs4 import Tag
from ebooklib import epub
from ebooklib.epub import EpubBook

_author__ = 'Xavier Garcia Cabellos'
__date__ = '20180101'
__version__ = 0.01
__description__ = 'This scripts read EbookProcess and break it in different elements'

#######
# Notas: # html2text.html2text(html) para pasar de html a text
books_file = '../input/books/Free_as_in_freedom.epub'  # for testing
module_logger = logging.getLogger('ebookprocess')
log_level = logging.INFO


# noinspection PyCompatibility
class EbookProcess:
    items = []
    # noinspection PyCompatibility
    book: EpubBook = None
    title = ""
    index = ""
    level = log_level

    def __init__(self, books_file=books_file, log_directory="./logs/", exception=Exception):
        """
                   The main function opens a PST and calls functions to parse and report data from the PST
                   :type exception: object
                   :param books_file: A string representing the path to the epub  file to analyze
                   :param log_directory: A string representing the path to the log file
                   :return: None
                   """
        self.active_log('Sending_Emails.log', self.level, log_directory)
        self.logger.debug('System ' + sys.platform + '  Version ' + sys.version)
        self._content = None
        self._paragraphs = []
        self.header = []
        count = 0
        try:
            self.book = epub.read_epub(books_file)
            self.title = self.book.title  # self.book.get_metadata('DC', 'title')
            self.index = self.book.get_item_with_href('index.xhtml')
            self.logger.info('reading epub....' + str(self.title) + "  from " + books_file)
            for item in self.book.get_items():
                if item.get_type() == ebooklib.ITEM_DOCUMENT:
                    # self.content.append(item.get_content().decode('utf-8', 'ignore'))
                    # self.items.append(item.get_name())
                    # self.logger.debug("item.get_name()")
                    for paragraph in extract_content_with_arc90(item.get_content().decode('utf-8', 'ignore')):
                        self._paragraphs.append(paragraph)
                    # self.logger.debug(preprocess_paragraph(str(paragraph)))
                    count += 1
                    if count == 4:  # ojo!!!!!!!!!!!!!!!! quitar el break es solo para ejecutar esto rapido
                        return  # ojo!!!!!!!!!!!!!!!! quitar el break es solo para ejecutar esto rapido


        except exception as BookException:
            self.logger.exception("Error: problems reading the book")

    # self.logger.debug('Finish of Read ' + str(count) + ' messages')

    def __del__(self):
        """

        """
        self.book = None
        self.title = None
        self.index = None
        self.logger = None
        self._paragraphs = None
        self._content = None

    @property
    def content(self):
        """

        :return:
        """
        return self._content

    @content.setter
    def content(self, content):
        """

        :param content:
        """
        self._content = content

    @property
    def paragraphs(self):
        """

        :return:
        """
        return self._paragraphs

    @paragraphs.setter
    def paragraphs(self, paragraphs):
        """

        :param paragraphs:
        """
        self._paragraphs = paragraphs

    @property
    def header(self):
        """

        :return:
        """
        return self._header

    @header.setter
    def header(self, header):
        """

        :param header:
        """
        self._header = header

    @staticmethod
    def create_html(title, header, paragraphs):
        """

        :param title:
        :param header:
        :param paragraphs:
        :return:
        """
        count = 0
        soup = BeautifulSoup()
        mem_attr = ['Description', 'PhysicalID', 'Slot', 'Size', 'Width']
        html = Tag(builder=soup.builder, name='html')
        # paragraph = Tag(builder=soup.builder, name='p')
        head = Tag(builder=soup.builder, name='head')
        titl = Tag(builder=soup.builder, name='title')
        body = Tag(builder=soup.builder, name='body')
        if (header is not None and len(header) != 0):
            head.insert(0, NavigableString(header))
        if (title is not None and len(title) != 0):
            titl.insert(0, NavigableString(str(title[0])))
        soup.append(html)
        html.append(head)
        head.append(titl)
        html.append(body)
        if (paragraphs is not None and len(paragraphs) != 0):
            for pg in paragraphs:
                paragraph = Tag(builder=soup.builder, name='p')
                paragraph.insert(count, NavigableString(str(pg.text)))
                count += 1
                body.append(paragraph)

        # self.logger.debug(soup)
        return str(soup.prettify())

    def create_multiple_htmls(self, number, header=header, paragraphs=paragraphs):
        """
        Method for make several html doc with the paragraphs of the book
        :param number: Number of paragraphs in each doc
        :param header: header of the EbookProcess
        :param paragraphs: Paragraphs of the book
        :return: list of html document
        """
        htmls: List[object] = []
        count = 1
        each = number
        subparagraph = []
        for paragraph in self.paragraphs:
            subparagraph.append(paragraph)
            if (count % each) == 0:
                htmls.append(self.create_html(self.title, self.header, subparagraph))
                subparagraph.clear()
            count = count + 1
        return htmls

    def active_log(self, log_name='ebook_process.log', level=logging.INFO, log_directory='./logs/'):

        if log_directory:
            if not os.path.exists(log_directory):
                os.makedirs(log_directory)
            log_path = os.path.join(log_directory, log_name)
        else:
            log_path = log_name

        logging.basicConfig(filename=log_path, level=level,
                            format='%(asctime)s | %(levelname)s | %(name)s | %(message)s', filemode='a')

        self.logger = logging.getLogger("ebookprocess.EbookProcess")
        self.logger.debug('Starting ebook_process logger using v.' + str(
            __version__) + ' System ' + sys.platform + ' Version ' + sys.version)


NEGATIVE = re.compile(".*comment.*|.*meta.*|.*footer.*|.*foot.*|.*cloud.*|.*head.*")
POSITIVE = re.compile(".*post.*|.*hentry.*|.*entry.*|.*content.*|.*text.*|.*body.*")
BR = re.compile("<br */? *>[ rn]*<br */? *>")


def preprocess_paragraph(text):
    """

    :param text:
    :return:
    """
    text.replace('[^\s]', '')
    return text


def extract_content_with_arc90(html):
    """

    :param html:
    :return:
    """
    soup = BeautifulSoup(re.sub(BR, "</p><p>", html), 'lxml')
    soup = simplify_html_before(soup)

    topParent = None
    parents = []
    for paragraph in soup.findAll("p"):

        parent = paragraph.parent
        if parent is not None:
            if (parent not in parents):
                parents.append(parent)
                parent.score = 0

                if (parent.has_key("class")):
                    if (NEGATIVE.match(str(parent["class"]))):
                        parent.score -= 50
                    elif (POSITIVE.match(str(parent["class"]))):
                        parent.score += 25

                if (parent.has_key("id")):
                    if (NEGATIVE.match(str(parent["id"]))):
                        parent.score -= 50
                    elif (POSITIVE.match(str(parent["id"]))):
                        parent.score += 25

            if (len(paragraph.renderContents()) > 10):
                if parent.score is not None:
                    parent.score += 1

        # you can add more rules here!

    if topParent is not None:
        topParent = max(parents, key=lambda x: x.score)
        soup = simplify_html_after(topParent)
    return soup.findAll("p")


def simplify_html_after(soup):
    """

    :param soup:
    :return:
    """
    for element in soup.findAll(True):
        element.attrs = {}
        if (len(element.renderContents().strip()) == 0):
            element.extract()
    return soup


def simplify_html_before(soup):
    """

    :param soup:
    :return:
    """
    comments = soup.findAll(text=lambda text: isinstance(text, Comment))
    [comment.extract() for comment in comments]

    # you can add more rules here!

    map(lambda x: x.replaceWith(x.text.strip()), soup.findAll("li"))  # tag to text
    map(lambda x: x.replaceWith(x.text.strip()), soup.findAll("em"))  # tag to text
    map(lambda x: x.replaceWith(x.text.strip()), soup.findAll("tt"))  # tag to text
    map(lambda x: x.replaceWith(x.text.strip()), soup.findAll("b"))  # tag to text

    replace_by_paragraph(soup, 'blockquote')
    replace_by_paragraph(soup, 'quote')

    map(lambda x: x.extract(), soup.findAll("code"))  # delete all
    map(lambda x: x.extract(), soup.findAll("style"))  # delete all
    map(lambda x: x.extract(), soup.findAll("script"))  # delete all
    map(lambda x: x.extract(), soup.findAll("link"))  # delete all

    delete_if_no_text(soup, "td")
    delete_if_no_text(soup, "tr")
    delete_if_no_text(soup, "div")

    delete_by_min_size(soup, "td", 10, 2)
    delete_by_min_size(soup, "tr", 10, 2)
    delete_by_min_size(soup, "div", 10, 2)
    delete_by_min_size(soup, "table", 10, 2)
    delete_by_min_size(soup, "p", 50, 2)

    return soup


def delete_if_no_text(soup, tag):
    """

    :param soup:
    :param tag:
    :return:
    """
    for p in soup.findAll(tag):
        if (len(p.renderContents().strip()) == 0):
            p.extract()
    return soup


def delete_by_min_size(soup, tag, length, children):
    """

    :param soup:
    :param tag:
    :param length:
    :param children:
    :return:
    """
    for p in soup.findAll(tag):
        if (len(p.text) < length and len(p) <= children):
            p.extract()
    return soup


def replace_by_paragraph(soup, tag):
    """

    :param soup:
    :param tag:
    :return:
    """
    for t in soup.findAll(tag):
        t.name = "p"
        t.attrs = {}
    return soup


if __name__ == "__main__":
    book_readen = EbookProcess(books_file, "./output/")
    html_messages = book_readen.create_multiple_htmls(30)
    for html in html_messages:
        module_logger.debug(html)
