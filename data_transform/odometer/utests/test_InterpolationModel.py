import unittest

import os
import json
from collections import OrderedDict
from datetime import datetime
from dateutil.parser import parse
from dateutil.tz import tzlocal
import itertools

import settings as st
from data_transform.odometer.interpolation import interpolate_gen
from utils.utils import ignore_warnings


class TestInterpolationModel(unittest.TestCase):
    def setUp(self):
        test_rows_path = os.path.join(st.PROJECT_DIR, 'utests', 'data', 'InterpolationModel_source.json')
        test_out_path = os.path.join(st.PROJECT_DIR, 'utests', 'data', 'InterpolationModel_target.json')
        with open(test_rows_path, 'r') as jin1, open(test_out_path, 'r') as jin2:
            test_rows = json.load(jin1)
            self.expected_values = sorted(json.load(jin2),
                                          key=lambda x: datetime.strptime(x['date_service'], '%Y-%m-%dT%H:%M:%S'))

        self.client_data = OrderedDict()
        for row in test_rows:
            key = parse(row['date_service']).date().isoformat()
            self.client_data[key] = {'client_name': row['client_name'],
                                     'vin': row['vin'],
                                     'model': row['model'],
                                     'odometer': row['odometer'] if row['odometer'] else 0,
                                     'presence': 1}

    def check_values(self, source, target):
        local_tz = tzlocal()
        self.assertIsInstance(source, dict,
                              'The data model returns wrong output type. Expected <list> of <dicts>.')
        self.assertEqual(source['client_name'], target['client_name'],
                         'Returned data corrupted in client_name value.')
        self.assertEqual(source['vin'], target['vin'], 'Returned data corrupted in vin value.')
        self.assertEqual(source['model'], target['model'], 'Returned data corrupted in model value.')
        self.assertEqual(parse(source['date_service']),
                         parse(target['date_service']).replace(tzinfo=local_tz),
                         'Returned data corrupted in date_service value.')
        self.assertEqual(source['presence'], target['presence'], 'Returned data corrupted in presence value.')
        self.assertEqual(source['exp_work_type'], target['exp_work_type'],
                         'Returned data corrupted in exp_work_type value.')
        self.assertTrue(target['odometer'] - 2 <= source['odometer'] <= target['odometer'] + 2,
                        'Returned data corrupted in odometer value.')
        if source['km']:
            self.assertTrue(target['km'] - 2 <= source['km'] <= target['km'] + 2,
                            'Returned data corrupted in km value')
        else:
            self.assertEqual(target['km'], source['km'], 'Returned data corrupted in km value')

    @ignore_warnings
    def test_interpolation(self):
        def check_by_date(v):
            return datetime.strptime(v['date_service'], '%Y-%m-%dT%H:%M:%S') < max_interp_data

        for res_row, control_row in zip(interpolate_gen(self.client_data,
                                                        months_mean_lag=-3,
                                                        months_data_lag=-24),
                                        self.expected_values):
            self.check_values(res_row, control_row)

        max_interp_data = datetime.strptime('2017-05-25T00:00:00', '%Y-%m-%dT%H:%M:%S')
        with_max_interp_date = interpolate_gen(self.client_data,
                                               months_mean_lag=-3,
                                               max_interp_date=max_interp_data)
        new_expected_values = itertools.filterfalse(check_by_date, self.expected_values)

        for res_row, control_row in zip(with_max_interp_date, new_expected_values):
            self.check_values(res_row, control_row)
