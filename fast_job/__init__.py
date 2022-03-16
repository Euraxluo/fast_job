# -*- coding: utf-8 -*- 
# Time: 2022-02-18 19:38
# Copyright (c) 2022
# author: Euraxluo

from fast_job.job_api import schedule, fast_job_api_router

__version__ = "0.1.12"
__description__ = """Provides scheduling apis and scheduling and task-related services"""
__all__ = ["fast_job_api_router", "schedule"]
