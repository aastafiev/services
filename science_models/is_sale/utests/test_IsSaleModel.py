import unittest

import os
from schema import SchemaError

import settings as st
from science_models.is_sale.model import IsSaleModel
from utils.utils import ignore_warnings


class TestIsSaleModel(unittest.TestCase):
    def setUp(self):
        self.test_model = IsSaleModel(os.path.join(st.PROJECT_DIR, 'science_models', 'is_sale', 'etc', 'config.yml'))
        with open(os.path.join(st.PROJECT_DIR, 'utests', 'data', 'data_for_IsSaleModel.txt'), 'r',
                  encoding='utf-8') as fin:
            self.test_input_good = fin.readline()
        with open(os.path.join(st.PROJECT_DIR, 'utests', 'data', 'data_for_IsSaleModel_bad.txt'), 'r',
                  encoding='utf-8') as fin:
            self.test_input_bad = fin.readline()
        self.expected_values = [{"client_id": "79639", "sale_probability": 0.0427027, "sale_class": "low"},
                                {"client_id": "24378", "sale_probability": 0.0426351, "sale_class": "low"},
                                {"client_id": "584346", "sale_probability": 0.0582207, "sale_class": "medium"},
                                {"client_id": "78577", "sale_probability": 0.116067, "sale_class": "high"},
                                {"client_id": "13070", "sale_probability": 0.0537706, "sale_class": "medium"}]

    @ignore_warnings
    def test_online_model_valid(self):
        res = self.test_model.online(self.test_input_good)
        self.assertIsInstance(res, list, 'The model gives wrong output type. Expected <list>.')
        for res_line, control_line in zip(res, self.expected_values):
            self.assertIsInstance(res_line, dict, 'The model gives wrong output type. Expected <list> of <dicts>.')
            first = '{client_id} {sale_probability:.6f} {sale_class}'.format(**res_line)
            second = '{client_id} {sale_probability:.6f} {sale_class}'.format(**control_line)
            self.assertEqual(first, second, 'Model (xgboost) prediction corrupted.')

    @ignore_warnings
    def test_online_check_input_data_schema(self):
        with self.assertRaises(SchemaError):
            self.test_model.online(self.test_input_bad)
