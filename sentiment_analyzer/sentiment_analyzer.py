#!/usr/bin/env python
# -*- coding: utf-8 -*-

import csv
import htmlentitydefs
import pickle
import re

from collections import OrderedDict

from nltk import bigrams, NaiveBayesClassifier
from nltk.corpus import stopwords

HTTP_STRING = r"""
    (?:(?:https?:\/\/)?
        (?:[\da-z\.-]+)\.
        (?:[a-z\.]{2,6})
        (?:[\/\w\.-]*)*\/?)
"""

EMOTICON_STRING = r"""
    (?:
      [<>]?
      [:;=8]                     # eyes
      [\-o\*\']?                 # optional nose
      [\)\]\(\[dDpP/\:\}\{@\|\\]+ # mouth
      |
      [\)\]\(\[dDpP/\:\}\{@\|\\]+ # mouth
      [\-o\*\']?                 # optional nose
      [:;=8]                     # eyes
      [<>]?
    )"""

REGEX_STRINGS = (
    # Phone numbers:
    r"""
    (?:
      (?:            # (international)
        \+?[01]
        [\-\s.]*
      )?
      (?:            # (area code)
        [\(]?
        \d{3}
        [\-\s.\)]*
      )?
      \d{3}          # exchange
      [\-\s.]*
      \d{4}          # base
    )""",
    # Emoticons:
    EMOTICON_STRING,
    HTTP_STRING,
    # HTML tags:
    r"""<[^>]+>""",
    # Twitter username:
    r"""(?:@[\w_]+)""",
    # Twitter hashtags:
    r"""(?:\#+[\w_]+[\w\'_\-]*[\w_]+)""",
    # Remaining word types:
    r"""
    (?:[a-z][a-z'\-_]+[a-z])       # Words with apostrophes or dashes.
    |
    (?:[+\-]?\d+[,/.:-]\d+[+\-]?)  # Numbers, including fractions, decimals.
    |
    (?:[\w_]+)                     # Words without apostrophes or dashes.
    |
    (?:\.(?:\s*\.){1,})            # Ellipsis dots.
    |
    (?:\S)                         # Everything else that isn't whitespace.
    """
)

WORD_RE = re.compile(
    r"""(%s)""" % "|".join(REGEX_STRINGS), re.VERBOSE | re.I | re.UNICODE)

EMOTICON_RE = re.compile(REGEX_STRINGS[1], re.VERBOSE | re.I | re.UNICODE)

HTTP_RE = re.compile(REGEX_STRINGS[2], re.VERBOSE | re.I | re.UNICODE)

HTML_ENTITY_DIGIT_RE = re.compile(r"&#\d+;")
HTML_ENTITY_ALPHA_RE = re.compile(r"&\w+;")
AMP = "&amp;"


class FeatureExtractor:

    def __init__(self, preserve_case=False):
        self.preserve_case = preserve_case
        self.stopwords = stopwords.words('english')

    def tokenize(self, s):
        try:
            s = unicode(s)
        except UnicodeDecodeError:
            s = str(s).encode('string_escape')
            s = unicode(s)
        # Fix HTML character entitites:
        s = self.__html2unicode(s)
        # Tokenize:
        words = WORD_RE.findall(s)
        # Possible alter the case, but avoid changing emoticons like :D
        # into :d:
        if not self.preserve_case:
            words = map(
                (lambda x: self.normalize_emoticons(x) if EMOTICON_RE.match(x)
                 else self.mask_urls(x.lower())), words)
        stop_words = stopwords.words('english')
        return [word for word in words
                if word not in stop_words and word[0] != '#']

    def __html2unicode(self, s):
        ents = set(HTML_ENTITY_DIGIT_RE.findall(s))
        if len(ents) > 0:
            for ent in ents:
                entnum = ent[2:-1]
                try:
                    entnum = int(entnum)
                    s = s.replace(ent, unichr(entnum))
                except:
                    pass
        # Now the alpha versions:
        ents = set(HTML_ENTITY_ALPHA_RE.findall(s))
        ents = filter((lambda x: x != AMP), ents)
        for ent in ents:
            entname = ent[1:-1]
            try:
                s = s.replace(
                    ent, unichr(htmlentitydefs.name2codepoint[entname]))
            except:
                pass
            s = s.replace(AMP, " and ")
        return s

    def normalize_emoticons(self, emoticon):
        return "".join(OrderedDict.fromkeys(emoticon))

    def mask_urls(self, string):
        return re.sub(HTTP_RE, '_url_', string)

    def get_feats(self, words):
        return dict([(word, True) for word in words])

    def read_train_corpus(self, csv_corpus_file, tweet_col, sentiment_col):
        train = []
        with open(csv_corpus_file, 'rb') as csv_file:
            reader = csv.reader(csv_file, delimiter=',', quotechar='"')
            for row in reader:
                train.append([row[tweet_col], row[sentiment_col]])
        return train

    def prepare_training_data(self, training_data):
        pass


class Classifier():

    def __init__(self, train_data_filename=None, string_col=4, sentiment_col=1,
                 test_data_filename=None, model_file=None, data_source=None):
        self.train_data_filename = train_data_filename
        if test_data_filename:
            self.test_data_filename = test_data_filename
        self.fe = FeatureExtractor()
        if model_file:
            self.nb = pickle.load(open(model_file, 'r'))
        else:
            self.nb = model_file
        self.sentiment_col = sentiment_col
        self.string_col = string_col
        self.data_source = data_source

    def classify(self, string):
        words = self.fe.tokenize(string)
        raw_feats = words + list(bigrams(words))
        feats = self.fe.get_feats(raw_feats)
        if self.nb:
            return self.nb.classify(feats)

    def train(self):
        train_data = self.fe.read_train_corpus(
            self.train_data_filename, self.string_col, self.sentiment_col)
        training_dataset = []
        for sample in train_data:
            if sample[1] != 'irrelevant':
                words = self.fe.tokenize(sample[0])
                raw_feats = words + list(bigrams(words))
                feats = self.fe.get_feats(raw_feats)
                training_dataset.append((feats, sample[1]))
        nb = NaiveBayesClassifier.train(training_dataset)
        with open("res/models/" + self.data_source, 'wb') as pfile:
            pickle.dump(nb, pfile, protocol=pickle.HIGHEST_PROTOCOL)
        nb.show_most_informative_features(100)
        self.nb = nb
