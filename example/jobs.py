# -*- coding: utf-8 -*- 
# Time: 2021-06-23 16:58
# Copyright (c) 2021
# author: Euraxluo

from fast_job import schedule, task_api_router_init


@schedule.task('test', summer="test.py", site=1, description="test")
def test(site: int):
    if site == 67:
        print("报错")
        raise Exception("city error")
    if site == 1:
        import time
        time.sleep(4)
    print("test.py", site)
    return site


router = task_api_router_init()
