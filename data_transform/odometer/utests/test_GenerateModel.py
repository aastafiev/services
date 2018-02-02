import unittest

import os
import json
from dateutil.parser import parse

import settings as st
from data_transform.odometer.generate import generate_gen
from data_transform.odometer.common.common_func import ClientLastRow, to_java_date_str
from utils.utils import ignore_warnings


class TestGenerateModel(unittest.TestCase):
    def setUp(self):
        test_source_path = os.path.join(st.PROJECT_DIR, 'utests', 'data', 'GenerateModel_source.json')
        test_target_path = os.path.join(st.PROJECT_DIR, 'utests', 'data', 'GenerateModel_target.json')
        with open(test_source_path, 'r') as jin1, open(test_target_path, 'r') as jin2:
            source = json.load(jin1)
            self.expected_values = sorted(json.load(jin2), key=lambda x: parse(x['date_service']))

        self.test_source = ClientLastRow(
            client_name=source['client_name'],
            vin=source['vin'],
            model=source['model'],
            date_service=parse(source['date_service']),
            odometer=source['odometer'],
            day_mean_km=source['day_mean_km']
        )

    def check_values(self, source, target):
        self.assertIsInstance(source, dict,
                              'The data model returns wrong output type. Expected <list> of <dicts>.')
        self.assertEqual(source['client_name'], target['client_name'],
                         'Returned data corrupted in client_name value.')
        self.assertEqual(source['vin'], target['vin'], 'Returned data corrupted in vin value.')
        self.assertEqual(source['model'], target['model'], 'Returned data corrupted in model value.')
        self.assertEqual(source['date_service'], to_java_date_str(parse(target['date_service'])),
                         'Returned data corrupted in date_service value.')
        self.assertTrue(target['odometer'] - 2 <= source['odometer'] <= target['odometer'] + 2,
                        'Returned data corrupted in odometer value.')
        self.assertEqual(source['exp_work_type'], target['exp_work_type'],
                         'Returned data corrupted in exp_work_type value.')

    @ignore_warnings
    def test_generate(self):
        for res_row, control_row in zip(generate_gen(self.test_source, parse('2017-11-01T00:00:00+0300')),
                                        self.expected_values):
            self.check_values(res_row, control_row)
