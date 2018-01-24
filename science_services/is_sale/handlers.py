# -*- coding: utf-8 -*-


async def is_sale_predict(request):
    request.app.logger.debug('Start prediction')

    model = request.app['model']
    cl_data = await request.json()
    predicted = model.online(cl_data)

    request.app.logger.debug('End predictions')
    return predicted

