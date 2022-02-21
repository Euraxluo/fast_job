# -*- coding: utf-8 -*- 
# Time: 2022-02-21 18:20
# Copyright (c) 2022
# author: Euraxluo

from unittest import TestCase


class TestJobSchedule(TestCase):
    def test_setup(self):
        from loguru import logger
        from example.jobs import schedule
        from example.conftest import rdb as redis

        schedule.setup(prefix='test:', logger=logger, redis=redis, distributed=True)
