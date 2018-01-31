# -*- coding: utf-8 -*-

from collections import OrderedDict
from aiohttp import web
from schema import SchemaError
from dateutil.parser import parse

from data_transform.interpolation.interpolation import interpolate_gen


async def handle_interpolate(request):
    client_request = await request.json()

    try:
        if request.app['validate_request'] and request.app['request_schema']:
            request.app['request_schema'].validate(client_request)
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

    return list(interpolate_gen(client_data,
                                months_mean_lag=request.app['months_mean_lag'],
                                max_interp_date=max_interp_date,
                                months_data_lag=request.app['months_data_lag']))
