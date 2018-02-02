#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio
import logging
import os

from aiohttp import web
from schema import Schema, Or

import settings as st
from common.middlewares import error_middleware
from utils.utils import load_cfg

from data_services.odometer.handlers import handle_interpolate, handle_generate


SERVICE_CONFIG = load_cfg(os.path.join(st.PROJECT_DIR, 'data_services', 'odometer', 'etc', 'config.yml'))
DEFAULT_LOG_FORMAT = '[%(levelname)1.1s %(asctime)s %(name)s %(module)s:%(lineno)d] %(message)s'


async def on_startup(app):
    app['months_data_lag'] = SERVICE_CONFIG['other']['months_data_lag']
    app['months_mean_lag'] = SERVICE_CONFIG['other']['months_mean_lag']


async def on_cleanup(app):
    pass


def add_routes(app):
    app.router.add_post('/interpolation', handle_interpolate)
    app.router.add_post('/generate', handle_generate)
    return app


def get_app(val_request: bool = False):
    app = web.Application(middlewares=[error_middleware], debug=True)

    app['validate_request'] = val_request
    if val_request:
        app['request_schema_interp'] = Schema([{"client_name": str,
                                                "vin": str,
                                                "model": str,
                                                "date_service": str,
                                                "max_interp_date": Or(None, str),
                                                "odometer": Or(None, int)}])
        app['request_schema_generate'] = Schema({"client_name": str,
                                                 "vin": str,
                                                 "model": str,
                                                 "date_service": str,
                                                 "odometer": int,
                                                 "day_mean_km": int,
                                                 "date_from": Or(None, str)})

    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)

    return add_routes(app)


if __name__ == '__main__':
    logging.basicConfig(level=logging.getLevelName(SERVICE_CONFIG['other']['log_level'].upper()),
                        format=DEFAULT_LOG_FORMAT)

    loop = asyncio.get_event_loop()
    try:
        web.run_app(get_app(),
                    host=SERVICE_CONFIG['service']['host'],
                    port=SERVICE_CONFIG['service']['port'],
                    loop=loop)
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
