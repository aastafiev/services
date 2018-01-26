import os
import json
from aiohttp.test_utils import AioHTTPTestCase, unittest_run_loop
from aiohttp.web_exceptions import HTTPBadRequest, HTTPOk
from schema import Schema, Or

import settings as st
from data_services.interpolation.server import get_app as get_my_app


class TestInterpolationModelRestAPI(AioHTTPTestCase):
    async def setUpAsync(self):
        with open(os.path.join(st.PROJECT_DIR, 'utests', 'data', 'valid_InterpolationModel_request.json'), 'r') as fin:
            self.valid_request = json.load(fin)

        self.__response_schema = Schema([{"client_name": str,
                                          "vin": str,
                                          "model": str,
                                          "odometer": int,
                                          "presence": int,
                                          "date_service": str,
                                          "km": Or(None, int),
                                          "exp_work_type": Or(None, str)}])

    async def get_application(self):
        return get_my_app()

    @unittest_run_loop
    async def test_service_httpok_json_valid(self):
        async with self.client.post("/utest", json=self.valid_request) as r:
            self.assertEqual(r.status, HTTPOk.status_code, await r.text())
            self.__response_schema.validate(await r.json())

    @unittest_run_loop
    async def test_service_empty_request(self):
        response = await self.client.post("/interpolation")
        self.assertEqual(response.status, HTTPBadRequest.status_code)
        self.assertEqual(await response.json(), {'msg': 'Empty json request'})

    @unittest_run_loop
    async def test_service_corrupted_request(self):
        async with self.client.post("/interpolation", json='kdfjghdf') as r:
            self.assertEqual(r.status, HTTPBadRequest.status_code)
            out = await r.json()
            self.assertEqual(out['msg'],
                             'No prq key found in json request!',
                             'The service manage HTTPBadRequest in wrong way')

        async with self.client.post("/interpolation", json={'prq': {}}) as r:
            self.assertEqual(r.status, HTTPBadRequest.status_code)
            out = await r.json()
            self.assertEqual(out['msg'],
                             'No client_name key found in json request!',
                             'The service manage HTTPBadRequest in wrong way')

        async with self.client.post("/interpolation", json={'prq': {'client_name': None}}) as r:
            self.assertEqual(r.status, HTTPBadRequest.status_code)
            out = await r.json()
            self.assertEqual(out['msg'],
                             'No vin key found in json request!',
                             'The service manage HTTPBadRequest in wrong way')
