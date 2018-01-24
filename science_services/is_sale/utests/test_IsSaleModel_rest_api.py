import os
from aiohttp.test_utils import AioHTTPTestCase, unittest_run_loop
from aiohttp.web_exceptions import HTTPBadRequest, HTTPOk
from schema import Schema, Use

import settings as st
from science_services.is_sale.server import get_app as get_my_app
from utils.utils import ignore_warnings


class TestIsSaleModelRestAPI(AioHTTPTestCase):
    async def setUpAsync(self):
        with open(os.path.join(st.PROJECT_DIR, 'utests', 'data', 'data_for_IsSaleModel.txt'), 'r',
                  encoding='utf-8') as fin:
            self.test_input_good = fin.readline()

        self.__in_data_schema = Schema([{'client_id': str,
                                         'sale_class': str,
                                         'sale_probability': Use(float)}])

    async def get_application(self):
        return get_my_app()

    @ignore_warnings
    @unittest_run_loop
    async def test_service_httpok_json_valid(self):
        response = await self.client.post("/is_sale", json=self.test_input_good)
        self.assertEqual(response.status, HTTPOk.status_code)
        self.__in_data_schema.validate(await response.json())

    @unittest_run_loop
    async def test_service_empty_request(self):
        response = await self.client.post("/is_sale")
        self.assertEqual(response.status, HTTPBadRequest.status_code)
        self.assertEqual(await response.json(), {'msg': 'Empty json request'})

    @unittest_run_loop
    async def test_service_corrupted_request(self):
        response = await self.client.post("/is_sale", json='kdfjghdf')
        self.assertEqual(response.status, HTTPBadRequest.status_code)
        out = await response.json()
        self.assertRegex(out['msg'], '^Unable to serialize the object')
