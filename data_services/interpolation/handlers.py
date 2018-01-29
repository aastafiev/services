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

    request.app.logger.debug('Got client: {client_name}, vin: {vin}'.format(**client_request[0]))
    max_interp_date = client_request[0]['max_interp_date']

    client_data = OrderedDict()
    for row in client_request:
        client_data[parse(row['date_service']).date().isoformat()] = {'client_name': row['client_name'],
                                                                      'vin': row['vin'],
                                                                      'model': row['model'],
                                                                      'odometer': row['odometer'] if row['odometer']
                                                                      else 0,
                                                                      'presence': 1}

    return list(interpolate_gen(client_data, max_interp_date, request.app['months_lag']))
