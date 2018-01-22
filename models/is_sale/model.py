import os
import json
import string
import scipy.sparse as sp
from typing import Tuple, List
from schema import Schema, Use

from nltk.util import ngrams
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords

import pymorphy2
import xgboost as xgb
from sklearn.feature_extraction.text import CountVectorizer

import settings as st
import utils.utils as utils

__all__ = (
    'IsSaleModel',
)

DEFAULT_PATH_CONF = os.path.join('models', 'is_sale', 'etc', 'config.yml')


class CountVectorizerWrapper(CountVectorizer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def validate_vocabulary(self):
        super()._validate_vocabulary()


class IsSaleModel(object):
    __table_punkt = str.maketrans({key: None for key in string.punctuation})
    __table_nums = str.maketrans({key: None for key in string.digits})
    __lemma_class = pymorphy2.MorphAnalyzer()

    __my_stop_words_unigrams = set(['nan', 'рено', 'renault', 'литр', 'чех', 'axlbsrbkbh', 'xlhsrdjn', 'два',
                                    'год', 'также', 'ама', 'город', 'цветсинийсинийбелый', 'который', 'мой', 'день',
                                    'подсказать', 'рубль', 'связь', 'хотеть', 'avfablauc', 'axlhsrdja', 'axlbsrbynbh',
                                    'avfjzbd', 'axlbsrchah', 'axllzbra', 'avflzbra', 'axlbsrbyabh',
                                    'axlhsrdjn', 'axlbsrbyabh', 'axlhsrdjn', 'avflzbra', 'тест', 'email',
                                    'добрый', 'получить', 'уважение', 'пожалуйста', 'avfbzar', 'здравствовать'] +
                                   stopwords.words('english') + stopwords.words('russian'))

    __my_stop_words_bigrams = {'просьба_прислать', 'способ_дилер', 'необходимый_консультация', 'база_каков',
                               'avfjzbd_предложение', 'axlbsrchah_предложение', 'axllzbra_предложение',
                               'avflzbra_предложение', 'axlbsrbyabh_предложение', 'axlhsrdjn_предложение', 'тест_тест',
                               'axllsrbhbh_предложение', 'мес_axllsrbhbh', 'мес_avflzbra', ''}

    __in_data_schema = Schema({"data": [{'types': str,
                                         'comments': str,
                                         'client_id': str,
                                         'return_customer': Use(int),
                                         'leads_count': Use(int)}]})

    def __init__(self, config_path=None):
        if not config_path:
            conf_path = os.path.join(st.PROJECT_DIR, DEFAULT_PATH_CONF)
        else:
            conf_path = config_path

        self.__config = utils.load_cfg(conf_path)

        self.__return_customer_col = self.config['model_data']['model_columns'][0]
        self.__leads_count_col = self.config['model_data']['model_columns'][1]
        self.__comments_col = self.config['model_data']['model_columns'][2]
        self.__types_col = self.config['model_data']['model_columns'][3]
        self.__output_col = self.config['model_data']['output_columns'][0]

        self.__high_threshold = self.config['model']['thresholds']['high']
        self.__medium_threshold = self.config['model']['thresholds']['medium']

        with open(os.path.join(st.PROJECT_DIR, self.__config['model_data']['thesauri'])) as fin:
            self.__mappings = json.load(fin)

        with open(os.path.join(st.PROJECT_DIR, self.__config['model_data']['comments_vocab']), 'r') as fcomm:
            comments_vocab = {k: int(v) for k, v in json.load(fcomm).items()}
        self.__comments_class = CountVectorizerWrapper(analyzer=self.tokenize_comments,
                                                       min_df=25, stop_words=None, vocabulary=comments_vocab)
        self.__comments_class.validate_vocabulary()

        with open(os.path.join(st.PROJECT_DIR, self.__config['model_data']['types_vocab']), 'r') as ftypes:
            types_vocab = {k: int(v) for k, v in json.load(ftypes).items()}
        self.__types_class = CountVectorizerWrapper(analyzer=self.tokenize_types, vocabulary=types_vocab)
        self.__types_class.validate_vocabulary()

        self.__features = self.comments_class.get_feature_names() + self.types_class.get_feature_names() \
                          + [self.__return_customer_col, self.__leads_count_col]

        self.__bst_params = self.__config['model']['best_params']
        self.__bst_booster = xgb.Booster(params=self.bst_params,
                                         model_file=os.path.join(st.PROJECT_DIR, self.__config['model']['file']))

    def tokenize_comments(self, text: str) -> Tuple:
        # remove all punktuation and digits
        tokenz = tuple(word_tokenize(text.lower().translate(self.__table_punkt).translate(self.__table_nums)))
        # map words
        tokenz = tuple(map(lambda x: self.__mappings.get(x, x), tokenz))
        # remove len(str) <= 2
        tokenz = tuple(el for el in tokenz if len(el) > 2)
        # Russian lemmatization
        tokenz = tuple(map(lambda x: self.__lemma_class.parse(x)[0].normal_form, tokenz))
        # Remove stop words unigrams
        tokenz = tuple(el for el in tokenz if el not in self.__my_stop_words_unigrams)
        # Create ngrams
        out = tuple('_'.join(ng) for ng in ngrams(tokenz, 2))

        return tuple(el for el in out if el not in self.__my_stop_words_bigrams)

    @staticmethod
    def tokenize_types(text):
        return tuple(t for t in text.lower().split('. ') if t)

    @property
    def config(self):
        return self.__config

    @property
    def mappings(self):
        return self.__mappings

    @property
    def bst_params(self):
        return self.__bst_params

    @property
    def bst_booster(self):
        return self.__bst_booster

    @property
    def comments_class(self):
        return self.__comments_class

    @property
    def types_class(self):
        return self.__types_class

    @property
    def features(self):
        return self.__features

    @property
    def high_threshold(self):
        return self.__high_threshold

    @property
    def medium_threshold(self):
        return self.__medium_threshold

    def process_data(self, client_data: str or dict) -> Tuple[xgb.DMatrix, List]:
        """
        :param client_data:
        json with structure
            {
              "data":[
                {
                  "column1":". VN",
                  "column2":15847,
                  ...
                },
                {
                  "column1":". VN",
                  "column2":15847,
                  ...
                },
                ...
              ]
            }
        :return:
            Tuple[xgb.DMatrix of prepared data, List of output column values]
        """
        if isinstance(client_data, str):
            data_flow = json.loads(client_data)
        elif isinstance(client_data, dict):
            data_flow = client_data
        else:
            raise TypeError('Input data must be string or dict')
        data_flow = self.__in_data_schema.validate(data_flow)['data']

        return_customer_list = []
        leads_count_list = []
        comments_tpl = tuple()
        types_tpl = tuple()
        output_list = []

        for row in data_flow:
            return_customer_list.append(int(row[self.__return_customer_col]))
            leads_count_list.append(int(row[self.__leads_count_col]))
            comments_tpl += (row[self.__comments_col],)
            types_tpl += (row[self.__types_col],)
            output_list.append(row[self.__output_col])

        x_comments = self.comments_class.transform(comments_tpl)
        x_types = self.types_class.transform(types_tpl)

        # csr_comments_types = sp.hstack((x_comments, x_types), format='csr')
        csr_return_customer = sp.csr_matrix(return_customer_list).transpose()
        csr_leads_count = sp.csr_matrix(leads_count_list).transpose()

        processed_data = sp.hstack((x_comments, x_types, csr_return_customer, csr_leads_count), format='csr')

        return xgb.DMatrix(processed_data), output_list

    def online(self, input_data: str or dict) -> List:
        """
        :param input_data:
        json with structure
            {
              "data":[
                {
                  "column1":". VN",
                  "column2":15847,
                  ...
                },
                {
                  "column1":". VN",
                  "column2":15847,
                  ...
                },
                ...
              ]
            }
        :return:
            List of dicts {'client_id': client_id, 'sale_probability': pred, 'sale_class': sale_class}
        """
        prepared_data, out_col = self.process_data(input_data)

        predict = self.bst_booster.predict(prepared_data)

        res = []
        for client_id, pred in zip(out_col, predict):
            if pred >= self.high_threshold:
                sale_class = 'high'
            elif self.medium_threshold <= pred < self.high_threshold:
                sale_class = 'medium'
            else:
                sale_class = 'low'

            res.append({'client_id': client_id, 'sale_probability': pred, 'sale_class': sale_class})

        return res
