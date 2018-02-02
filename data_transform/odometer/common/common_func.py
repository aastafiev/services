# -*- coding: utf-8 -*-
from datetime import timedelta, datetime
from scipy.interpolate import interp1d
import numpy as np
from typing import Tuple, NamedTuple


def to_java_date_str(date: datetime) -> str:
    return "{}.{:03.0f}".format(date.strftime('%Y/%m/%d %H:%M:%S'), date.microsecond / 1000)


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


def date_range(start_date, end_date):
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + timedelta(days=n)


class ClientLastRow(NamedTuple):
    client_name: str
    vin: str
    model: str
    date_service: datetime
    odometer: int
    day_mean_km: int

    def __repr__(self) -> str:
        return f'<ClientLastRow client_name={self.client_name}, vin={self.vin}>, model={self.model},' \
               f' date_service={self.date_service}, odometer={self.odometer}, day_mean_km={self.day_mean_km}'
