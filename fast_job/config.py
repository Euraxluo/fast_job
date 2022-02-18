# -*- coding: utf-8 -*- 
# Time: 2021-06-29 18:09
# Copyright (c) 2021
# author: Euraxluo
from enum import Enum


class ExceptionCode(Enum):
    ScheduleFailed = 8000
    ScheduleCreateExistsFailed = 8100
    ScheduleNotFoundFailed = 8101
    ScheduleRetryFailed = 8102


class SuccessMessage(Enum):
    TaskSubmitSuccess = "任务已经提交给后台执行"

    JobSetCronSuccess = "Cron调度器创建成功"
    JobSetDatetimeSuccess = "Datetime调度器创建成功"
    JobSetIntervalSuccess = "Interval调度器创建成功"

    JobGetAllSuccess = "获取所有任务信息成功"
    JobGetSuccess = "获取任务信息成功"

    JobModifySuccess = "修改任务调度信息成功"
    JobResumeSuccess = "恢复任务调度成功"
    JobPauseSuccess = "暂停任务调度成功"
    JobRemoveSuccess = "删除任务调度成功"

    JobRetrySuccess = "任务重试成功"
