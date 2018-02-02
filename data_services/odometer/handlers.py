# -*- coding: utf-8 -*-

from collections import OrderedDict
from aiohttp import web
from schema import SchemaError
from dateutil.parser import parse

from data_transform.odometer.interpolation import interpolate_gen
from data_transform.odometer.generate import generate_gen
from data_transform.odometer.common.common_func import ClientLastRow


async def handle_interpolate(request):
    client_request = await request.json()

    try:
        if request.app['validate_request'] and request.app['request_schema_interp']:
            request.app['request_schema_interp'].validate(client_request)
    except SchemaError as se:
        raise web.HTTPBadRequest(reason='Wrong request! {}'.format(se))

    client_data, row = OrderedDict(), None
    for row in client_request:
        client_data[parse(row['date_service']).date().isoformat()] = {'client_name': row['client_name'],
                                                                      'vin': row['vin'],
                                                                      'model': row['model'],
                                                                      'odometer': row['odometer'] if row['odometer']
                                                                      else 0,
                                                                      'presence': 1}

    request.app.logger.debug('Got client: {client_name}, vin: {vin}'.format(**row))
    max_interp_date = parse(row['max_interp_date']) if row['max_interp_date'] else None

    ret = list(interpolate_gen(client_data,
                               months_mean_lag=request.app['months_mean_lag'],
                               max_interp_date=max_interp_date,
                               months_data_lag=request.app['months_data_lag']))

    return ret if ret else 'no relevant data or no new visits by client (max_interp_date == max_date_window)'


async def handle_generate(request):
    client_request = await request.json()

    try:
        if request.app['validate_request'] and request.app['request_schema_generate']:
            request.app['request_schema_generate'].validate(client_request)
    except SchemaError as se:
        raise web.HTTPBadRequest(reason='Wrong request! {}'.format(se))

    client_data = ClientLastRow(
        client_name=client_request['client_name'],
        vin=client_request['vin'],
        model=client_request['model'],
        date_service=parse(client_request['date_service']),
        odometer=client_request['odometer'],
        day_mean_km=client_request['day_mean_km']
    )

    ret = [res for res in generate_gen(client_data,
                                       parse(client_request['date_from']) if client_request['date_from'] else None)
           if res['exp_work_type']]
    return ret if ret else 'no data with next exp_work_type'
