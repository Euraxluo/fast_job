# -*- coding: utf-8 -*- 
# Time: 2021-06-23 16:58
# Copyright (c) 2021
# author: Euraxluo

from fast_job import *


@schedule.task('task1', summer="test_task_1", tag='test', description="test_task_1")
def test(tag: int):
    print({"msg": "test_task_1", "tag": tag})
    return {"msg": "test_task_1", "tag": tag}


@schedule.task('task2', summer="test_task_2", tag='test', description="test_task_2")
def test2(tag: int):
    print({"msg": "test_task_2", "tag": tag})
    return {"msg": "test_task_2", "tag": tag}


@schedule.task('task3', summer="test_task_3", tag='test', description="test_task_3")
def task3(tag: int):
    raise Exception(str({"msg": "test_task_2", "tag": tag}))
