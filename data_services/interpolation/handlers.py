# -*- coding: utf-8 -*-

# import json
from collections import OrderedDict
import sqlalchemy as sa
from aiohttp import web
# from aiohttp.web_exceptions import HTTPBadRequest

# from common.middlewares import error_middleware

import data_services.interpolation.db as db
from data_transform.interpolation.interpolation import interpolate_gen

__all__ = (
    'handle_interpolate',
    'handle_utest_interpolate',
)


async def handle_interpolate(request):
    # request.app.logger.debug('Start interpolation')

    client_request = await request.json()
    if 'prq' not in client_request:
        raise web.HTTPBadRequest(reason='No prq key found in json request!')
    elif 'client_name' not in client_request['prq']:
        raise web.HTTPBadRequest(reason='No client_name key found in json request!')
    elif 'vin' not in client_request['prq']:
        raise web.HTTPBadRequest(reason='No vin key found in json request!')

    client_name = client_request['prq']['client_name']
    vin = client_request['prq']['vin']

    query_interp_info = sa.select([sa.func.max(db.odometer_interpolated.c.date_service).label('max_interp_date')]) \
        .where(sa.and_(db.odometer.c.client == client_name, db.odometer.c.vin == vin))

    query_client = sa.select([db.odometer.c.client.label('client_name'),
                              db.odometer.c.vin,
                              db.odometer.c.model,
                              db.odometer.c.date_service,
                              db.odometer.c.odometer])\
        .where(sa.and_(db.odometer.c.client == client_name, db.odometer.c.vin == vin))\
        .order_by(db.odometer.c.date_service.asc())

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

    return list(interpolate_gen(client_data, max_interp_date, request.app['year_lag']))


async def handle_utest_interpolate(request):
    # request.app.logger.debug('Start interpolation')

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

    query_client = sa.select([db.utest_interpolation.c.client_name,
                              db.utest_interpolation.c.vin,
                              db.utest_interpolation.c.model,
                              db.utest_interpolation.c.date_service,
                              db.utest_interpolation.c.odometer])\
        .where(sa.and_(db.utest_interpolation.c.client_name == client_name, db.utest_interpolation.c.vin == vin))\
        .order_by(db.utest_interpolation.c.date_service.asc())

    client_data = OrderedDict()
    async with request.app['db'].acquire() as conn_get:
        async for row in conn_get.execute(query_client):
            client_data[row.date_service.date().isoformat()] = {'client_name': row.client_name,
                                                                'vin': row.vin,
                                                                'model': row.model,
                                                                'odometer': row.odometer if row.odometer else 0,
                                                                'presence': 1}

    return list(interpolate_gen(client_data, max_interp_date, request.app['year_lag']))
