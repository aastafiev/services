# -*- coding: utf-8 -*-

from collections import OrderedDict
import sqlalchemy as sa
from aiohttp import web

from .db import tbl_odometer, tbl_odometer_interpolated, tbl_utest_interpolation
from data_transform.interpolation.interpolation import interpolate_gen


async def handle_interpolate(request):
    client_request = await request.json()
    if 'prq' not in client_request:
        raise web.HTTPBadRequest(reason='No prq key found in json request!')
    elif 'client_name' not in client_request['prq']:
        raise web.HTTPBadRequest(reason='No client_name key found in json request!')
    elif 'vin' not in client_request['prq']:
        raise web.HTTPBadRequest(reason='No vin key found in json request!')

    request.app.logger.debug('Got client: {client_name}, vin: {vin}'.format(**client_request['prq']))
    client_name = client_request['prq']['client_name']
    vin = client_request['prq']['vin']

    query_interp_info = sa.select([sa.func.max(tbl_odometer_interpolated.c.date_service).label('max_interp_date')]) \
        .where(sa.and_(tbl_odometer_interpolated.c.client_name == client_name, tbl_odometer_interpolated.c.vin == vin))

    query_client = sa.select([tbl_odometer.c.client_name,
                              tbl_odometer.c.vin,
                              tbl_odometer.c.model,
                              tbl_odometer.c.date_service,
                              tbl_odometer.c.odometer])\
        .where(sa.and_(tbl_odometer.c.client_name == client_name, tbl_odometer.c.vin == vin))\
        .order_by(tbl_odometer.c.date_service.asc())

    pg = request.app['db']
    client_data = OrderedDict()
    async with pg.acquire() as conn_get:
        max_interp_date = (await conn_get.scalar(query_interp_info))
        async for row in conn_get.execute(query_client):
            client_data[row.date_service.date().isoformat()] = {'client_name': row.client_name,
                                                                'vin': row.vin,
                                                                'model': row.model,
                                                                'odometer': row.odometer if row.odometer else 0,
                                                                'presence': 1}

    return list(interpolate_gen(client_data, max_interp_date, request.app['months_lag']))


async def handle_utest_interpolate(request):
    client_request = await request.json()
    if 'prq' not in client_request:
        raise web.HTTPBadRequest(reason='No prq key found in json request!')
    elif 'client_name' not in client_request['prq']:
        raise web.HTTPBadRequest(reason='No client_name key found in json request!')
    elif 'vin' not in client_request['prq']:
        raise web.HTTPBadRequest(reason='No vin key found in json request!')

    client_name = client_request['prq']['client_name']
    vin = client_request['prq']['vin']

    max_interp_date = None

    query_client = sa.select([tbl_utest_interpolation.c.client_name,
                              tbl_utest_interpolation.c.vin,
                              tbl_utest_interpolation.c.model,
                              tbl_utest_interpolation.c.date_service,
                              tbl_utest_interpolation.c.odometer])\
        .where(sa.and_(tbl_utest_interpolation.c.client_name == client_name, tbl_utest_interpolation.c.vin == vin))\
        .order_by(tbl_utest_interpolation.c.date_service.asc())

    client_data = OrderedDict()
    async with request.app['db'].acquire() as conn_get:
        async for row in conn_get.execute(query_client):
            client_data[row.date_service.date().isoformat()] = {'client_name': row.client_name,
                                                                'vin': row.vin,
                                                                'model': row.model,
                                                                'odometer': row.odometer if row.odometer else 0,
                                                                'presence': 1}

    return list(interpolate_gen(client_data, max_interp_date, request.app['months_lag']))
