# -*- coding: utf-8 -*- 
# Time: 2022-02-18 19:38
# Copyright (c) 2022
# author: Euraxluo

from fast_job.job_schedule import schedule
from fast_job.task_api import task_api_router, task_api_router_init
from fast_job.job_api import job_api_router

__version__ = "0.1.6"
__description__ = """Provides scheduling apis and scheduling and task-related services"""
__all__ = ["task_api_router", "task_api_router_init", "job_api_router", "schedule"]
