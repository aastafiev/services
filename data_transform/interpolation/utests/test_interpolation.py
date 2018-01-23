import unittest

import os
import json
from collections import OrderedDict
from datetime import datetime

import settings as st
from data_transform.interpolation.interpolation import interpolate_gen
from utils.utils import ignore_warnings


class TestIsSaleModel(unittest.TestCase):
    def setUp(self):
        test_rows_path = os.path.join(st.PROJECT_DIR, 'data_transform', 'interpolation', 'data', 'test_rows.json')
        test_out_path = os.path.join(st.PROJECT_DIR, 'data_transform', 'interpolation', 'data', 'test_out.json')
        with open(test_rows_path, 'r') as jin1,  open(test_out_path, 'r') as jin2:
            test_rows = json.load(jin1)['data']
            self.expected_values = json.load(jin2)

        self.client_data = OrderedDict()
        for row in sorted(test_rows, key=lambda x: datetime.strptime(x['date_service'], '%Y-%m-%dT%H:%M:%S')):
            key = datetime.strptime(row['date_service'], '%Y-%m-%dT%H:%M:%S').date().isoformat()
            self.client_data[key] = {'client_name': row['client_name'],
                                     'vin': row['vin'],
                                     'model': row['model'],
                                     'odometer': row['odometer'] if row['odometer'] else 0,
                                     'presence': 1}

    @ignore_warnings
    def test_interpolation(self):
        # res = [i for i in interpolate_gen(self.client_data)]
        res = interpolate_gen(self.client_data)
        for res_row, control_row in zip(res, self.expected_values):
            self.assertIsInstance(res_row, dict, 'The data model returns wrong output type. Expected <list> of <dicts>.')
            self.assertEqual(json.dumps(res_row), json.dumps(control_row), 'Returned data corrupted.')
