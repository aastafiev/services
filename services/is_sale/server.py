#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio
import logging
import os

from aiohttp import web

import settings as st
from services.is_sale.handlers import is_sale_predict
from services.common.middlewares import error_middleware
from models.is_sale.model import IsSaleModel
from utils.utils import load_cfg

__all__ = (
    'is_sale_predict',
)

SERVICE_CONFIG = load_cfg(os.path.join(st.PROJECT_DIR, 'services', 'is_sale', 'etc', 'config.yml'))
DEFAULT_LOG_FORMAT = '[%(levelname)1.1s %(asctime)s %(name)s %(module)s:%(lineno)d] %(message)s'


async def init_model_is_sale(app):
    app['model'] = IsSaleModel(app['config_path'])


def add_routes(app):
    app.router.add_post('/is_sale', is_sale_predict)
    return app


def get_app():
    # app = web.Application(middlewares=[error_middleware, ], loop=loop, logger=logger)
    # app = web.Application(loop=loop, client_max_size=1024 * 1024 * 50, logger=logger) client_max_size=1024 * 1024 * 50
    app = web.Application(middlewares=[error_middleware], debug=True)
    app['config_path'] = os.path.join(st.PROJECT_DIR, SERVICE_CONFIG['service']['model_config'])
    app.on_startup.append(init_model_is_sale)
    # app.on_cleanup.append(on_cleanup)
    return add_routes(app)


if __name__ == '__main__':
    logging.basicConfig(level=logging.getLevelName(SERVICE_CONFIG['other']['log_level'].upper()),
                        format=DEFAULT_LOG_FORMAT)

    loop = asyncio.get_event_loop()
    web.run_app(get_app(),
                host=SERVICE_CONFIG['service']['host'],
                port=SERVICE_CONFIG['service']['port'],
                loop=loop)
