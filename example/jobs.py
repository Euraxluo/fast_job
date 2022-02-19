# -*- coding: utf-8 -*- 
# Time: 2021-06-23 16:58
# Copyright (c) 2021
# author: Euraxluo

from fast_job import schedule, task_api_router_init


@schedule.task('task_id', summer="task_test", tag=2, description="test")
def test(tag: int):
    print(tag)
    if tag == 67:
        print("报错")
        raise Exception("city error")
    if tag == 1:
        import time
        time.sleep(4)
    print("test.py", tag)
    return tag


@schedule.task('task_id2', summer="task_test", tag=2, description="test")
def test2(tag: int):
    print(tag)
    if tag == 67:
        print("报错")
        raise Exception("city error")
    if tag == 1:
        import time
        time.sleep(4)
    print("test.py", tag)
    return tag


router = task_api_router_init()
