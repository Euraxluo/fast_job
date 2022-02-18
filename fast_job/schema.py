# -*- coding: utf-8 -*- 
# Time: 2021-06-29 17:56
# Copyright (c) 2021
# author: Euraxluo

import datetime
from typing import List, Any, Mapping, Optional, Tuple
from pydantic import BaseModel
from apscheduler.job import Job


class SchedulesBase(BaseModel):
    tag: str
    job_id: str
    func_name: str
    func_args: Tuple
    cron_model: str
    next_run_time: Optional[datetime.datetime]


class TaskWorkRecord(BaseModel):
    run_id: str
    job_id: str
    task_id: str
    tag: str
    success: bool = False
    result: Any = None
    error: Any = None
    task_args: List[Any]
    task_kwargs: Mapping[str, Any]
    start_time: float
    end_time: float
    process_time: float


class Response(BaseModel):
    """
    API Response
    """
    code: Optional[int] = 200
    message: Optional[str] = ""
    data: Optional[dict] = {}


class Job(object):
    def __init__(self, job: Job):
        from fast_job.job_schedule import schedule
        tag, job_id = schedule.get_job_tag_with_job_id(job.id)
        self.tag = tag
        self.id = job_id
        self.trigger = str(job.trigger)
        self.func_ref = job.func_ref
        self.args = job.args
        self.kwargs = job.kwargs
        self.name = job.name
        self.misfire_grace_time = job.misfire_grace_time
        self.next_run_time = job.next_run_time
