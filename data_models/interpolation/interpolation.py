#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio
import aiopg.sa
import sqlalchemy as sa
from datetime import timedelta, datetime
from statistics import mode

from collections import OrderedDict, defaultdict
from scipy.interpolate import interp1d
import numpy as np

import data_services.interpolation.db as db

YEAR_LAG = -3


def filter_x_y(x, y):
    # Check values
    y_local = np.array(y)
    x_local = np.array(x)
    for i in range(0, y_local.size - 1):
        for j in range(i + 1, y_local.size):
            if y_local[i] > y_local[j]:
                y_local[j] = 0
    zero_idx = np.where(y_local == 0)
    if zero_idx[0].size:
        y_local = np.delete(y_local, zero_idx)
        x_local = np.delete(x_local, zero_idx)

    return x_local, y_local


def interpolate(x, y, xnew):
    f = interp1d(x, y, kind='linear', fill_value='extrapolate')
    ynew = f(xnew)
    ynew[ynew < 0] = 0
    return ynew


def calc_exp_work_type(value):
    work_types = {'M-15': (12000, 18000),
                  'M-30': (28000, 32000),
                  'M-40': (39000, 41000),
                  'M-45': (43500, 48500),
                  'M-50': (49000, 51000),
                  'M-60': (58000, 62000),
                  'M-70': (69000, 71500),
                  'M-75': (73000, 77500),
                  'M-80': (79000, 81000),
                  'M-90': (88500, 92000),
                  'M-100': (99000, 101500),
                  'M-105': (103500, 107000),
                  'M-110': (109000, 111000),
                  'M-120': (119000, 121500),
                  'M-130': (129000, 131500),
                  'M-135': (134000, 138000),
                  'M-140': (139000, 142000),
                  'M-150': (148000, 152500)}

    for key, (segment_start, segment_end) in work_types.items():
        if segment_start <= value <= segment_end:
            return key

    return None


async def interpolate_data_gen(get_conn, client_name, vin):
    def date_range(start_date, end_date):
        for n in range(int((end_date - start_date).days) + 1):
            yield start_date + timedelta(days=n)

    def first(s):
        """Return the first element from an ordered collection
           or an arbitrary element from an unordered collection.
           Raise StopIteration if the collection is empty.
        """
        return next(iter(s))

    def last(s):
        """Return the last element from an ordered collection
           or an arbitrary element from an unordered collection.
           Raise StopIteration if the collection is empty.
        """
        key = next(reversed(s))
        return key, s[key]

    query_interp_info = sa.select([sa.func.max(db.odometer_interpolated.c.date_service).label('max_interp_date')])\
        .where(sa.and_(db.odometer.c.client == client_name, db.odometer.c.vin == vin))

    max_interp_date = (await get_conn.scalar(query_interp_info))

    query_client = sa.select([db.odometer.c.client.label('client_name'),
                              db.odometer.c.vin,
                              db.odometer.c.model,
                              db.odometer.c.date_service,
                              db.odometer.c.odometer]).where(sa.and_(db.odometer.c.client == client_name,
                                                                     db.odometer.c.vin == vin))\
                                                      .order_by(db.odometer.c.date_service.asc())

    client_data = OrderedDict()
    model = []
    async for row in get_conn.execute(query_client):
        model.append(row.model)
        client_data[row.date_service.date().isoformat()] = {'client_name': row.client_name,
                                                            'vin': row.vin,
                                                            'model': row.model,
                                                            'odometer': row.odometer if row.odometer else 0,
                                                            'presence': 1}

    model_mode = mode(model)
    min_date_cl = datetime.strptime(first(client_data), '%Y-%m-%d')
    max_date = datetime.strptime(last(client_data)[0], '%Y-%m-%d')
    min_date = datetime(year=max_date.year + YEAR_LAG, month=1, day=1) if not max_interp_date else max_interp_date
    min_date = min_date if min_date >= min_date_cl else min_date_cl

    x = tuple()
    x_new = tuple()
    y = tuple()
    date_mapper = defaultdict(str)

    for _x, day in enumerate(date_range(min_date, max_date), 1):
        x_new += (_x, )
        key = day.date().isoformat()
        date_mapper[_x] = key
        if key in client_data:
            x += (_x, )
            y += (client_data[key]['odometer'], )

    filtered_x_y = filter_x_y(x, y)
    if filtered_x_y[0].size > 1 and filtered_x_y[1].size > 1:
        y_new_arr = interpolate(filtered_x_y[0], filtered_x_y[1], x_new)
        km_arr = np.append([-1], np.diff(y_new_arr))

        for x_key, map_date in date_mapper.items():
            new_odometer = int(round(y_new_arr[x_key - 1], 0))
            km = int(round(km_arr[x_key - 1], 0)) if km_arr[x_key - 1] != -1 else None
            exp_work_type = calc_exp_work_type(new_odometer)
            if map_date in client_data:
                row = client_data[map_date]
                row['date_service'] = map_date
                row['odometer'] = new_odometer
                row['km'] = km
                row['exp_work_type'] = exp_work_type
                yield row
            else:
                yield {'client_name': client_name, 'vin': vin, 'modal': model_mode, 'presence': 0,
                       'date_service': map_date, 'odometer': new_odometer, 'km': km, 'exp_work_type': exp_work_type}


async def get_postgres_engine(loop, db):
    return await aiopg.sa.create_engine(**db, loop=loop)


async def main(loop, db_config):
    async with await get_postgres_engine(loop, db_config) as pg_get:
        async with pg_get.acquire() as conn_get:
            res = [i async for i in interpolate_data_gen(conn_get,
                                                         client_name='Ababij Vitalij Dmitrievich',
                                                         vin='VF1VY0C0VUC520845')]

    return res


if __name__ == '__main__':
    db_conf = {'database': 'test',
               'host': 'localhost',
               'user': 'postgres',
               'password': '123',
               'port': '5432'}

    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main(loop, db_conf))
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
