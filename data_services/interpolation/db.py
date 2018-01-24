# -*- coding: utf-8 -*-

import sqlalchemy as sa
# import sqlalchemy.dialects


metadata = sa.MetaData(schema='public')

tbl_odometer = sa.Table('odometer', metadata,
                        sa.Column('region', sa.String(1024)),
                        sa.Column('bir', sa.String(1024)),
                        sa.Column('dealer', sa.String(1024)),
                        sa.Column('client', sa.String(1024)),
                        sa.Column('client_type', sa.String(1024)),
                        sa.Column('vin', sa.String(1024)),
                        sa.Column('model', sa.String(1024)),
                        sa.Column('phone', sa.String(1024)),
                        sa.Column('email', sa.String(1024)),
                        sa.Column('date_service', sa.DateTime(timezone=True)),
                        sa.Column('odometer', sa.Integer),
                        sa.Column('odometer_segment', sa.String(1024)),
                        sa.Column('work_type', sa.String(1024)),
                        sa.Column('work_code', sa.String(1024))
                        )

tbl_odometer_interpolated = sa.Table('odometer_interpolated', metadata,
                                     sa.Column('client_name', sa.String(1024)),
                                     sa.Column('vin', sa.String(1024)),
                                     sa.Column('model', sa.String(1024)),
                                     sa.Column('date_service', sa.DateTime(timezone=True)),
                                     sa.Column('odometer', sa.Integer),
                                     sa.Column('km', sa.Integer),
                                     sa.Column('presence', sa.Integer),
                                     sa.Column('exp_work_type', sa.String(10))
                                     )

tbl_utest_interpolation = sa.Table('utest_interpolation', metadata,
                                   sa.Column('client_name', sa.String(1024)),
                                   sa.Column('vin', sa.String(1024)),
                                   sa.Column('model', sa.String(1024)),
                                   sa.Column('date_service', sa.DateTime(timezone=True)),
                                   sa.Column('odometer', sa.Integer)
                                   )
