# -*- coding: utf-8 -*-
import traceback
import json

from aiohttp import web
from aiohttp.web_exceptions import HTTPException, HTTPBadRequest, HTTPOk, HTTPInternalServerError

__all__ = (
    'error_middleware',
)


def json_response(data, status):
    body = json.dumps(data, default=str)
    return web.Response(body=body, content_type='application/json', status=status.status_code)


def json_error(reason, status):
    return json_response({'msg': reason}, status=status)


def json_success(data):
    return json_response(data, status=HTTPOk)


@web.middleware
async def error_middleware(request, handler):
    try:
        if request.method == 'POST' and (not request.body_exists or not request.can_read_body):
            return json_error('Empty json request', status=HTTPBadRequest)

        response = await handler(request)
        return json_success(response) if not isinstance(response, web.Response) else response
    except HTTPException as exc:
        return json_error(exc.reason, exc)
    except json.JSONDecodeError as exc:
        return json_error('Unable to serialize the object. Error: {}'.format(str(exc)), HTTPBadRequest)
    except:
        return json_error(traceback.format_exc(), HTTPInternalServerError)
