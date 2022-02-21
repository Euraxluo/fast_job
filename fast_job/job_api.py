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

from fast_job.job_schedule import schedule, JobSchedule, fast_job_api_router
from fast_job.schema import Response, SchedulesBase, TaskWorkRecord, Job, ExceptionCode, SuccessMessage

job_api_prefix = "/schedule"


class SecondsCronTrigger(CronTrigger):
    @classmethod
    def seconds_crontab(cls, expr, timezone=None):
        values = expr.split()
        if len(values) != 7:
            raise ValueError("Wrong number of fields; got {}, expected 7".format(len(values)))

        return cls(second=values[0], minute=values[1], hour=values[2], day=values[3], month=values[4],
                   day_of_week=values[5], year=values[6], timezone=timezone)


# interval Fixed interval scheduling
@fast_job_api_router.post(job_api_prefix + "/interval", response_model=Response, summary="Interval a periodic schedule that runs in a cycle")
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

    job = schedule.add_job(JobSchedule().task_scheduler,
                           'interval',
                           args=(job_id, task_id, tag),
                           seconds=seconds,
                           id=schedule.job_key(job_id=job_id, tag=tag),
                           next_run_time=run_time,
                           max_instances=1000,
                           misfire_grace_time=3600
                           )
    return Response(message=SuccessMessage.JobSetIntervalSuccess.value, data={"job": Job(job)})


# date Run only once at a particular point in time
@fast_job_api_router.post(job_api_prefix + "/datetime", response_model=Response, summary="Scheduled scheduling that runs at a specified point in time")
async def datetimer(
        job_id: str = Body(..., description="job_id"),
        task_id: str = Body(..., description="task_id"),
        run_time: typing.Union[str, datetime] = Body(datetime.now(), description="Run time specified"),
        tag: str = 'default'):
    schedule.logger.info({"Add datetime scheduling mode Task": {"job_id": job_id, "task_id": task_id, "run_time": run_time, "tag": tag}})
    job = schedule.get_job(job_id=schedule.job_key(job_id=job_id, tag=tag))
    if job:
        return Response(code=ExceptionCode.ScheduleCreateExistsFailed.value, message=f"{job_id} job already exists")

    if run_time == '' or (isinstance(run_time, datetime) and run_time < datetime.now()):
        run_time = datetime.now()

    job = schedule.add_job(JobSchedule().task_scheduler,
                           'date',
                           args=(job_id, task_id, tag),
                           run_date=run_time,
                           id=schedule.job_key(job_id=job_id, tag=tag),  # job ID
                           max_instances=1000,
                           misfire_grace_time=3600
                           )
    return Response(message=SuccessMessage.JobSetDatetimeSuccess.value, data={"job": Job(job)})


# cron More flexible scheduled tasks can use the crontab expression
@fast_job_api_router.post(job_api_prefix + "/cron", response_model=Response, summary="Scheduled scheduling using crontab expression")
async def cron(
        job_id: str = Body(..., description="job_id"),
        task_id: str = Body(..., description="task_id"),
        run_time: typing.Union[str, datetime] = Body(datetime.now(), description="First run time"),
        crontab: str = Body('*/10 * 8-20 * * * *', description="crontab expression"),
        tag: str = 'default'):
    schedule.logger.info({"Add cron scheduled Task": {"job_id": job_id, "task_id": task_id, "run_time": run_time, "crontab": crontab, "tag": tag}})
    job = schedule.get_job(job_id=schedule.job_key(job_id=job_id, tag=tag))
    if job:
        return Response(code=ExceptionCode.ScheduleCreateExistsFailed.value, message=f"{job_id} job already exists")

    if run_time == '' or (isinstance(run_time, datetime) and run_time < datetime.now()):
        run_time = apscheduler.util.undefined

    job = schedule.add_job(JobSchedule().task_scheduler,
                           SecondsCronTrigger.seconds_crontab(crontab),
                           args=(job_id, task_id, tag),
                           id=schedule.job_key(job_id=job_id, tag=tag),
                           next_run_time=run_time,
                           max_instances=1000,
                           misfire_grace_time=3600
                           )
    return Response(message=SuccessMessage.JobSetCronSuccess.value, data={"job": Job(job)})


@fast_job_api_router.get(job_api_prefix + "/scheduling", response_model=Response, summary="Get information about all scheduling jobs")
async def scheduling(tag: str = 'default'):
    all_schedule: typing.List[SchedulesBase] = []
    for job in schedule.get_jobs():
        if schedule.check_job_key(job_id=job.id, tag=tag):
            all_schedule.append(SchedulesBase(tag=tag, job_id=schedule.get_real_job_id(job.id), func_name=job.func_ref, func_args=job.args, cron_model=str(job.trigger), next_run_time=job.next_run_time))
    schedule.logger.info({"Get information about all scheduling jobs": {"tag": tag, "all_job": all_schedule}})
    if len(all_schedule) == 0:
        return Response(code=ExceptionCode.ScheduleNotFoundFailed.value, message=f"job not found")
    return Response(message=SuccessMessage.JobGetAllSuccess.value, data={"schedules": all_schedule})


@fast_job_api_router.get(job_api_prefix + "/job", response_model=Response, summary="Get information about the specified job")
async def job_info(
        job_id: str = Query(None, description="任务id"),
        tag: str = 'default'):
    schedule.logger.info({"Get information about the specified job": {"tag": tag, "job_id": job_id}})
    job = schedule.get_job(job_id=schedule.job_key(job_id=job_id, tag=tag))
    if job:
        return Response(message=SuccessMessage.JobGetSuccess.value, data=SchedulesBase(tag=tag, job_id=job.id, func_name=job.func_ref, func_args=job.args, cron_model=str(job.trigger), next_run_time=job.next_run_time).dict())
    return Response(code=ExceptionCode.ScheduleNotFoundFailed.value, message=f"not found job {job_id},tag {tag},job_key {schedule.job_key(job_id=job_id, tag=tag)}")


@fast_job_api_router.post(job_api_prefix + "/remove/{job_id}", response_model=Response, summary="Remove a task based on the job ID")
async def remove(
        job_id: str = Path(..., description="job_id"),
        tag: str = 'default'):
    schedule.logger.info({"Remove a task based on the job ID": {"tag": tag, "job_id": job_id}})
    remove_work_rdb_resp = schedule.remove_work_rdb(job_id=job_id, tag=tag)
    job = schedule.get_job(job_id=schedule.job_key(job_id=job_id, tag=tag))
    if not job:
        return Response(code=ExceptionCode.ScheduleNotFoundFailed.value, message=f"not found job {job_id},work_log {remove_work_rdb_resp}")

    schedule.remove_job(schedule.job_key(job_id=job_id, tag=tag))
    return Response(message=SuccessMessage.JobRemoveSuccess.value, data={"job": Job(job), "work_log": remove_work_rdb_resp})


@fast_job_api_router.post(job_api_prefix + "/pause/{job_id}", response_model=Response, summary="Pause a job by job ID")
async def pause(
        job_id: str = Path(..., description="job_id"),
        tag: str = 'default'):
    schedule.logger.info({"Pause a job by job ID": {"tag": tag, "job_id": job_id}})
    job = schedule.get_job(job_id=schedule.job_key(job_id=job_id, tag=tag))
    if not job:
        return Response(code=ExceptionCode.ScheduleNotFoundFailed.value, message=f"not found job {job_id}")
    schedule.pause_job(schedule.job_key(job_id=job_id, tag=tag))
    return Response(message=SuccessMessage.JobPauseSuccess.value, data={"job": Job(job)})


@fast_job_api_router.post(job_api_prefix + "/resume/{job_id}", response_model=Response, summary="Resume a job by job ID")
async def resume(
        job_id: str = Path(..., description="job_id"),
        tag: str = 'default'):
    schedule.logger.info({"Resume a job by job ID": {"tag": tag, "job_id": job_id}})
    job = schedule.get_job(job_id=schedule.job_key(job_id=job_id, tag=tag))
    if not job:
        return Response(code=ExceptionCode.ScheduleNotFoundFailed.value, message=f"not found job {job_id}")
    schedule.resume_job(schedule.job_key(job_id=job_id, tag=tag))
    return Response(message=SuccessMessage.JobResumeSuccess.value, data={"job": Job(job)})


@fast_job_api_router.post(job_api_prefix + "/modify", response_model=Response, summary="Modify a task based on the job ID")
async def modify(
        job_id: str = Body("", description="job_id"),
        task_id: typing.Optional[str] = Body("", description="task_id"),
        trigger: typing.Optional[str] = Body("interval", description="(e.g. `date`, `interval` or `cron`)"),
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


@fast_job_api_router.get(job_api_prefix + "/log", response_model=Response, summary="Get execution records corresponding to the job ID")
async def log(
        job_id: str = Query(..., description="job_id"),
        start: int = Query(0, description="Start index, starting at 0, default 0"),
        end: int = Query(99, description="End index, default is 99"),
        tag: str = 'default'):
    schedule.logger.info({"Get execution records corresponding to the job ID": {"job_id": job_id, "start": start, "end": end, "tag": tag}})
    job = schedule.get_job(job_id=schedule.job_key(job_id=job_id, tag=tag))
    if not job:
        return Response(code=ExceptionCode.ScheduleNotFoundFailed.value, message=f"not found job {job_id}")

    work_records = schedule.list_work_record(job_id=job_id, tag=tag, start=start, end=end)
    return Response(data={"log": work_records})


@fast_job_api_router.post(job_api_prefix + "/retry", response_model=Response, summary="Retry execution records based on the job ID and other parameters")
async def retry(work: TaskWorkRecord = Body(..., description="Execute record structure")):
    schedule.logger.info({"Retry execution records based on the job ID and other parameters": {"work": work.dict()}})
    try:
        result = schedule.task_retry(work=work)
    except Exception as e:
        return Response(code=ExceptionCode.ScheduleRetryFailed.value, message=str(e))
    return Response(message=SuccessMessage.JobRetrySuccess.value, data={"result": result})


@fast_job_api_router.get(job_api_prefix + "/tasks", response_model=Response, summary="all load tasks")
async def tasks():
    return Response(data=schedule.__task_manage__)
