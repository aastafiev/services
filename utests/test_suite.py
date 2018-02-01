#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest

from science_models.is_sale.utests.test_IsSaleModel import TestIsSaleModel
from science_services.is_sale.utests.test_IsSaleModel_rest_api import TestIsSaleModelRestAPI
from data_transform.odometer.utests.test_InterpolationModel import TestInterpolationModel
from data_services.odometer.utest.test_InterpolationModel_rest_api import TestInterpolationModelRestAPI


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(TestIsSaleModel('test_online_model_valid'))
    suite.addTest(TestIsSaleModel('test_online_check_input_data_schema'))
    suite.addTest(TestIsSaleModelRestAPI('test_service_httpok_json_valid'))
    suite.addTest(TestIsSaleModelRestAPI('test_service_empty_request'))
    suite.addTest(TestIsSaleModelRestAPI('test_service_corrupted_request'))
    suite.addTest(TestInterpolationModel('test_interpolation'))
    suite.addTest(TestInterpolationModelRestAPI('test_service_httpok_json_valid'))
    suite.addTest(TestInterpolationModelRestAPI('test_service_empty_request'))
    suite.addTest(TestInterpolationModelRestAPI('test_service_corrupted_request'))
    return suite


if __name__ == '__main__':
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(test_suite())
