#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio
import logging
import os

from aiohttp import web
import aiopg.sa

import settings as st
from common.middlewares import error_middleware
from utils.utils import load_cfg

from data_services.interpolation.handlers import handle_interpolate, handle_utest_interpolate


SERVICE_CONFIG = load_cfg(os.path.join(st.PROJECT_DIR, 'data_services', 'interpolation', 'etc', 'config.yml'))
DEFAULT_LOG_FORMAT = '[%(levelname)1.1s %(asctime)s %(name)s %(module)s:%(lineno)d] %(message)s'


async def init_db(app):
    app['db'] = await aiopg.sa.create_engine(**app['db_conf'], loop=app.loop)


async def on_cleanup(app):
    app['db'].close()
    await app['db'].wait_closed()


def add_routes(app):
    app.router.add_post('/interpolation', handle_interpolate)
    app.router.add_post('/utest', handle_utest_interpolate)
    return app


def get_app():
    app = web.Application(middlewares=[error_middleware], debug=True)
    app['db_conf'] = SERVICE_CONFIG['db']
    app['months_lag'] = SERVICE_CONFIG['other']['months_lag']

    app.on_startup.append(init_db)
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
