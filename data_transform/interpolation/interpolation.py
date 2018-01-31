#!/usr/bin/env python
# -*- coding: utf-8 -*-

from datetime import timedelta, datetime
from dateutil.parser import parse
from dateutil.tz import tzlocal
from dateutil.relativedelta import relativedelta
from statistics import mode, StatisticsError
from typing import Tuple, Iterable

from collections import OrderedDict, defaultdict
from scipy.interpolate import interp1d
import numpy as np


def filter_x_y(x: tuple, y: tuple) -> Tuple[np.array, np.array]:
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


def interpolate_raw(x: np.array, y: np.array, new_x: tuple) -> np.array:
    f = interp1d(x, y, kind='linear', fill_value='extrapolate')
    y_new = f(new_x)
    y_new[y_new < 0] = 0
    return y_new


def calc_exp_work_type(value: int):
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


def interpolate_gen(client_data: OrderedDict, months_mean_lag: int,
                    max_interp_date: datetime = None, months_data_lag: int = -3) -> Iterable[dict]:
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

    assert client_data, 'client_data empty'
    assert months_mean_lag < 0, 'months_mean_lag should be less than 0'

    local_tz = tzlocal()
    if max_interp_date and not max_interp_date.tzinfo:
        max_interp_date = max_interp_date.replace(tzinfo=local_tz)
    max_date_window = parse(last(client_data)[0]).replace(tzinfo=local_tz)
    date_for_mean = max_date_window + relativedelta(months=months_mean_lag)
    min_date_window = max_date_window + relativedelta(months=months_data_lag)\
        if not max_interp_date else max_interp_date

    model, model_mode = (value['model'] for _, value in client_data.items()), None
    try:
        model_mode = mode(model)
    except StatisticsError as serr:
        model_mode = None if str(serr) == 'no mode for empty data' else first(model)

    key_cl = first(client_data)
    client_name = client_data[key_cl]['client_name']
    vin = client_data[key_cl]['vin']
    min_date_cl = parse(key_cl).replace(tzinfo=local_tz)
    min_date_window = min_date_window if min_date_window >= min_date_cl else min_date_cl
    date_for_mean = date_for_mean if date_for_mean >= min_date_cl else min_date_cl

    x = tuple()
    x_new = tuple()
    y = tuple()
    date_mapper = defaultdict(str)
    date_mean_index = -1

    for _x, day in enumerate(date_range(min_date_cl, max_date_window), 1):
        x_new += (_x, )
        key = day.date().isoformat()
        if date_mean_index == -1 and day == date_for_mean:
            date_mean_index = _x - 1
        if min_date_window <= day <= max_date_window:
            date_mapper[_x] = key
        if key in client_data:
            x += (_x, )
            y += (client_data[key]['odometer'], )

    filtered_x_y = filter_x_y(x, y)
    if filtered_x_y[0].size > 1 and filtered_x_y[1].size > 1:
        y_new_arr = interpolate_raw(filtered_x_y[0], filtered_x_y[1], x_new)
        km_arr = np.append([-1], np.diff(y_new_arr))
        date_mean_index = date_mean_index if date_mean_index != -1 else 0
        day_mean_km = int(round(float(np.mean(km_arr[date_mean_index:], dtype=np.float64)), 0))

        length = len(date_mapper)
        for i, (x_key, map_date) in enumerate(date_mapper.items(), 1):
            new_odometer = int(round(y_new_arr[x_key - 1], 0))
            km = int(round(km_arr[x_key - 1], 0)) if km_arr[x_key - 1] != -1 else None
            exp_work_type = calc_exp_work_type(new_odometer)
            if map_date in client_data:
                row = client_data[map_date]
                row['model'] = model_mode
                row['date_service'] = parse(map_date).replace(tzinfo=local_tz).astimezone().isoformat()
                row['odometer'] = new_odometer
                row['km'] = km
                row['exp_work_type'] = exp_work_type
                row['day_mean_km'] = None if i != length else day_mean_km
                yield row
            else:
                yield {'client_name': client_name, 'vin': vin, 'model': model_mode, 'presence': 0,
                       'date_service': parse(map_date).replace(tzinfo=local_tz).astimezone().isoformat(),
                       'odometer': new_odometer, 'km': km, 'day_mean_km': None, 'exp_work_type': exp_work_type}
