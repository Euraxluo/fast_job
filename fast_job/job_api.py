# -*- coding: utf-8 -*- 
# Time: 2021-06-29 17:45
# Copyright (c) 2021
# author: Euraxluo


from datetime import datetime
import typing
from fastapi import APIRouter, Query, Body, Path

import apscheduler.util
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers import base as BaseTrigger
from apscheduler.triggers.interval import IntervalTrigger

from fast_job.schema import Response, SchedulesBase, TaskWorkRecord, Job
from fast_job.config import ExceptionCode, SuccessMessage
from fast_job.job_schedule import schedule

job_api_router = APIRouter()


class SecondsCronTrigger(CronTrigger):
    @classmethod
    def seconds_crontab(cls, expr, timezone=None):
        values = expr.split()
        if len(values) != 7:
            raise ValueError("Wrong number of fields; got {}, expected 7".format(len(values)))

        return cls(second=values[0], minute=values[1], hour=values[2], day=values[3], month=values[4],
                   day_of_week=values[5], year=values[6], timezone=timezone)


# interval 固定间隔时间调度
@job_api_router.post("/interval", response_model=Response, summary="Interval a periodic schedule that runs in a cycle")
async def interval(
        job_id: str = Body(..., description="job_id"),
        task_id: str = Body(..., description="task_id"),
        run_time: typing.Union[str, datetime] = Body(datetime.now(), description="First run time"),
        seconds: int = Body(120, description="Cycle interval / second, default 120S"),
        tag: str = 'default'):
    schedule.logger.info({"Add Interval Scheduling mode Task": {"job_id": job_id, "task_id": task_id, "run_time": run_time, "seconds": seconds, "tag": tag}})
    job = schedule.get_job(job_id=schedule.job_key(job_id=job_id, tag=tag))
    if job:
        return Response(code=ExceptionCode.ScheduleCreateExistsFailed.value, message=f"{job_id} job already exists")

    if run_time == '' or (isinstance(run_time, datetime) and run_time < datetime.now()):
        run_time = apscheduler.util.undefined

    job = schedule.add_job(schedule.task_scheduler,
                           'interval',
                           args=(job_id, task_id, tag),
                           seconds=seconds,
                           id=schedule.job_key(job_id=job_id, tag=tag),
                           next_run_time=run_time,
                           max_instances=1000,
                           misfire_grace_time=3600
                           )
    return Response(message=SuccessMessage.JobSetIntervalSuccess.value, data={"job": Job(job)})


# date 某个特定时间点只运行一次
@job_api_router.post("/datetime", response_model=Response, summary="指定时间点运行的定时调度")
async def datetimer(
        job_id: str = Body(..., description="job_id"),
        task_id: str = Body(..., description="task_id"),
        run_time: typing.Union[str, datetime] = Body(datetime.now(), description="指定的运行时间"),
        tag: str = 'default'):
    schedule.logger.info({"添加datetime调度模式的task": {"job_id": job_id, "task_id": task_id, "run_time": run_time, "tag": tag}})
    job = schedule.get_job(job_id=schedule.job_key(job_id=job_id, tag=tag))
    if job:
        return Response(code=ExceptionCode.ScheduleCreateExistsFailed.value, message=f"{job_id} job already exists")

    if run_time == '' or (isinstance(run_time, datetime) and run_time < datetime.now()):
        run_time = datetime.now()

    job = schedule.add_job(schedule.task_scheduler,
                           'date',
                           args=(job_id, task_id, tag),
                           run_date=run_time,
                           id=schedule.job_key(job_id=job_id, tag=tag),  # job ID
                           max_instances=1000,
                           misfire_grace_time=3600
                           )
    return Response(message=SuccessMessage.JobSetDatetimeSuccess.value, data={"job": Job(job)})


# cron 更灵活的定时任务 可以使用crontab表达式
@job_api_router.post("/cron", response_model=Response, summary="使用crontab表达式的定时调度")
async def cron(
        job_id: str = Body(..., description="job_id"),
        task_id: str = Body(..., description="task_id"),
        run_time: typing.Union[str, datetime] = Body(datetime.now(), description="第一次运行时间"),
        crontab: str = Body('*/10 * 8-20 * * * *', description="crontab 表达式"),
        tag: str = 'default'):
    schedule.logger.info({"添加cron调度模式的task": {"job_id": job_id, "task_id": task_id, "run_time": run_time, "crontab": crontab, "tag": tag}})
    job = schedule.get_job(job_id=schedule.job_key(job_id=job_id, tag=tag))
    if job:
        return Response(code=ExceptionCode.ScheduleCreateExistsFailed.value, message=f"{job_id} job already exists")

    if run_time == '' or (isinstance(run_time, datetime) and run_time < datetime.now()):
        run_time = apscheduler.util.undefined

    job = schedule.add_job(schedule.task_scheduler,
                           SecondsCronTrigger.seconds_crontab(crontab),
                           args=(job_id, task_id, tag),
                           id=schedule.job_key(job_id=job_id, tag=tag),
                           next_run_time=run_time,
                           max_instances=1000,
                           misfire_grace_time=3600
                           )
    return Response(message=SuccessMessage.JobSetCronSuccess.value, data={"job": Job(job)})


@job_api_router.get("/all", response_model=Response, summary="获取所有job信息")
async def all(tag: str = 'default'):
    all_schedule: typing.List[SchedulesBase] = []
    for job in schedule.get_jobs():
        if schedule.check_job_key(job_id=job.id, tag=tag):
            all_schedule.append(SchedulesBase(tag=tag, job_id=schedule.get_real_job_id(job.id), func_name=job.func_ref, func_args=job.args, cron_model=str(job.trigger), next_run_time=job.next_run_time))
    schedule.logger.info({"获取所有job信息": {"tag": tag, "all_job": all}})
    if len(all_schedule) == 0:
        return Response(code=ExceptionCode.ScheduleNotFoundFailed.value, message=f"job not found")
    return Response(message=SuccessMessage.JobGetAllSuccess.value, data={"schedules": all})


@job_api_router.get("/get", response_model=Response, summary="获取指定job信息")
async def get(
        job_id: str = Query(None, description="任务id"),
        tag: str = 'default'):
    schedule.logger.info({"获取指定job信息": {"tag": tag, "job_id": job_id}})
    job = schedule.get_job(job_id=schedule.job_key(job_id=job_id, tag=tag))
    if job:
        return Response(message=SuccessMessage.JobGetSuccess.value, data=SchedulesBase(tag=tag, job_id=job.id, func_name=job.func_ref, func_args=job.args, cron_model=str(job.trigger), next_run_time=job.next_run_time).dict())
    return Response(code=ExceptionCode.ScheduleNotFoundFailed.value, message=f"not found job {job_id}")


@job_api_router.post("/remove/{job_id}", response_model=Response, summary="Remove a task based on the job ID")
async def remove(
        job_id: str = Path(..., description="job_id"),
        tag: str = 'default'):
    schedule.logger.info({"根据job_id移除任务": {"tag": tag, "job_id": job_id}})
    schedule.remove_work_rdb(job_id=job_id, tag=tag)
    job = schedule.get_job(job_id=schedule.job_key(job_id=job_id, tag=tag))
    if not job:
        return Response(code=ExceptionCode.ScheduleNotFoundFailed.value, message=f"not found job {job_id}")

    schedule.remove_job(schedule.job_key(job_id=job_id, tag=tag))
    return Response(message=SuccessMessage.JobRemoveSuccess.value, data={"job": Job(job)})


@job_api_router.post("/pause/{job_id}", response_model=Response, summary="根据job_id暂停任务")
async def pause(
        job_id: str = Path(..., description="job_id"),
        tag: str = 'default'):
    schedule.logger.info({"根据job_id暂停任务": {"tag": tag, "job_id": job_id}})
    job = schedule.get_job(job_id=schedule.job_key(job_id=job_id, tag=tag))
    if not job:
        return Response(code=ExceptionCode.ScheduleNotFoundFailed.value, message=f"not found job {job_id}")
    schedule.pause_job(schedule.job_key(job_id=job_id, tag=tag))
    return Response(message=SuccessMessage.JobPauseSuccess.value, data={"job": Job(job)})


@job_api_router.post("/resume/{job_id}", response_model=Response, summary="根据job_id恢复任务")
async def resume(
        job_id: str = Path(..., description="job_id"),
        tag: str = 'default'):
    schedule.logger.info({"根据job_id恢复任务": {"tag": tag, "job_id": job_id}})
    job = schedule.get_job(job_id=schedule.job_key(job_id=job_id, tag=tag))
    if not job:
        return Response(code=ExceptionCode.ScheduleNotFoundFailed.value, message=f"not found job {job_id}")
    schedule.resume_job(schedule.job_key(job_id=job_id, tag=tag))
    return Response(message=SuccessMessage.JobResumeSuccess.value, data={"job": Job(job)})


@job_api_router.post("/modify", response_model=Response, summary="Modify a task based on the job ID")
async def modify(
        job_id: str = Body("", description="job_id"),
        task_id: typing.Optional[str] = Body("", description="task_id"),
        trigger: typing.Optional[str] = Body("interval", description="(e.g. 'date', `interval` or `cron`)"),
        run_time: typing.Union[str, datetime] = Body(datetime.now(), description="Expected first run time"),
        crontab: str = Body('*/10 * 8-20 * * * *', description="crontab expression"),
        seconds: int = Body(120, description="Cycle mode interval, unit / second, default 120S"),
        tag: str = 'default'):
    schedule.logger.info({"Modify a task based on the job ID": {"job_id": job_id, "task_id": task_id, "trigger": trigger, "run_time": run_time, "crontab": crontab, "seconds": seconds, "tag": tag}})
    job = schedule.get_job(job_id=schedule.job_key(job_id=job_id, tag=tag))
    if not job:
        return Response(code=ExceptionCode.ScheduleNotFoundFailed.value, message=f"not found job {job_id}")

    if run_time == '' or (isinstance(run_time, datetime) and run_time < datetime.now()):
        run_time = apscheduler.util.undefined

    new_trigger: BaseTrigger = None
    if trigger == "date":
        new_trigger = DateTrigger(run_date=run_time)
    elif trigger == "interval":
        new_trigger = IntervalTrigger(seconds=seconds)
    elif trigger == "cron":
        new_trigger = SecondsCronTrigger.seconds_crontab(crontab)
    elif trigger is not None:
        return Response(code=ExceptionCode.ScheduleNotFoundFailed.value, message=f"modify failed,because trigger wrong")
    args = job.args
    if task_id:
        args = (args[0], job_id, task_id, tag)
    try:
        job = schedule.modify_job(job_id=schedule.job_key(job_id=job_id, tag=tag), trigger=new_trigger, next_run_time=run_time, args=args)
        schedule.resume_job(schedule.job_key(job_id=job_id, tag=tag))  # might delete it failed
    except Exception as e:
        return Response(message=f"Task modification failed:{e}")
    return Response(message=SuccessMessage.JobModifySuccess.value, data={"job": Job(job)})


@job_api_router.get("/log", response_model=Response, summary="Obtain execution records corresponding to the job ID")
async def log(
        job_id: str = Query(..., description="job_id"),
        start: int = Query(0, description="Start index, starting at 0, default 0"),
        end: int = Query(99, description="End index, default is 99"),
        tag: str = 'default'):
    schedule.logger.info({"Obtain execution records corresponding to the job ID": {"job_id": job_id, "start": start, "end": end, "tag": tag}})
    job = schedule.get_job(job_id=schedule.job_key(job_id=job_id, tag=tag))
    if not job:
        return Response(code=ExceptionCode.ScheduleNotFoundFailed.value, message=f"not found job {job_id}")

    log = schedule.list_work_record(job_id=job_id, tag=tag, start=start, end=end)
    return Response(data={"log": log})


@job_api_router.post("/retry", response_model=Response, summary="Retry execution records based on the job ID and other parameters")
async def retry(work: TaskWorkRecord = Body(..., description="Execute record structure")):
    schedule.logger.info({"Retry execution records based on the job ID and other parameters": {"work": work.dict()}})
    try:
        result = schedule.task_retry(work=work)
    except Exception as e:
        return Response(code=ExceptionCode.ScheduleRetryFailed.value, message=str(e))
    return Response(message=SuccessMessage.JobRetrySuccess.value, data={"result": result})


__all__ = ["job_api_router"]
