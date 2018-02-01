# -*- coding: utf-8 -*-
from datetime import datetime
from typing import Iterable

from data_transform.odometer.common.common_func import date_range, ClientLastRow, calc_exp_work_type


def generate_gen(client_last_row: ClientLastRow, date_by: datetime) -> Iterable[dict]:
    assert client_last_row, 'client data empty'
    assert date_by, 'date_by empty'

    generated_days = date_range(client_last_row.date_service, date_by)
    next(generated_days)
    for _n, day in enumerate(generated_days, 1):
        new_odometer = client_last_row.odometer + _n * client_last_row.day_mean_km
        yield {'client_name': client_last_row.client_name, 'vin': client_last_row.vin,
               'model': client_last_row.model, 'date_service': day,
               'odometer': new_odometer, 'exp_work_type': calc_exp_work_type(new_odometer)}
