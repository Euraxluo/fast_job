# -*- coding: utf-8 -*- 
# Time: 2022-02-15 17:10
# Copyright (c) 2022
# author: Euraxluo

import json
import time
import uuid
import typing
import contextlib
import traceback
from functools import wraps
from fastapi import BackgroundTasks, APIRouter
from starlette.background import BackgroundTask
from threading import Thread
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.jobstores.redis import RedisJobStore

from fast_job.lock import *
from fast_job.schema import TaskWorkRecord, Response
from fast_job.sonyflake import SonyFlakeId

fast_job_api_router = APIRouter()


class JobSchedule(BackgroundTasks):
    __task_manage__ = {}  # task 管理器
    __schedule__ = None  # 调度器

    __redis__ = None  # Redis存储器
    __logger__ = None  # 日志管理器
    __redis_job_store__ = None  # Redis 调度存储器

    __distributed_id__ = str(uuid.uuid1())  # 分布式id

    __prefix = ""
    __registry_key = "job_schedule:registry"
    __run_times_key = "job_schedule:run_times"
    __jobs_key = "job_schedule:jobs"

    __setup_flag__ = False  # is init?

    __distributed = False

    def __init__(self):
        super().__init__()
        """调度器"""
        self.registry_time = time.time()
        self.min_registry_time = 30
        self.redis_job_store = None

        """执行器"""
        self.running_task = None
        self.running_id = None

        """收集器"""
        self.start_time = None
        self.result = None
        self.success = True
        self.error = None
        self.end_time = None
        self.result__collection = []

    def setup(self, prefix: str, redis, logger, distributed=False):
        """
        延迟初始化
        """
        self.logger = logger
        self.redis = redis
        self.prefix = prefix

        if not JobSchedule.__setup_flag__:
            self.registry_key = self.prefix + self.registry_key
            self.run_times_key = self.prefix + self.run_times_key
            self.jobs_key = self.prefix + self.jobs_key
        self.__redis_job_store__ = RedisJobStore(jobs_key=self.jobs_key, run_times_key=self.run_times_key)
        self.__redis_job_store__.redis = self.redis
        self.init_scheduler(distributed=distributed)
        JobSchedule.__setup_flag__ = True

    @classmethod
    def task(cls, task_id=None, summer=None, **params):
        """
        将function包装为 BackgroundTask,以准备运行

        task_id:task_id
        summer:简介
        params:endpoint_params
        """

        def decorator(func, _task_id=task_id, _summer=summer, _params=params or {}):
            if _task_id is None:
                _task_id = func.__name__

            @wraps(func)
            def wrapper_task(*args, background_task: BackgroundTasks = BackgroundTasks(), **kwargs):
                """
                task运行时包装函数
                """
                error = None  # 初始化错误为None
                run_result = None  # 初始化运行结果为None
                run_async = True if isinstance(background_task, JobSchedule) else False  # 默认为同步运行

                if run_async:
                    def run_function(result):
                        func_result = func(*args, **kwargs)
                        result.append(func_result)
                        return func_result
                else:
                    # noinspection PyBroadException
                    try:
                        run_result = func(*args, **kwargs)
                    except Exception as _:
                        error = traceback.format_exc()

                    def run_function(result):
                        result.append(run_result)
                        return run_result

                cls.__task_manage__[_task_id].update({"args": args, "kwargs": kwargs})
                background_task.add_task(func=run_function)
                if error is not None:
                    return Response(code=500, data={"func": func.__name__, "args": args, "kwargs": kwargs, "async": run_async, "result": run_result, "error": error})
                return Response(data={"func": func.__name__, "args": args, "kwargs": kwargs, "async": run_async, "result": run_result, "error": error})

            if _task_id in cls.__task_manage__:
                raise KeyError("Key Error,You cannot wrap two functions with the same task ID ")
            cls.__task_manage__[_task_id] = {"func": func, "wrap": wrapper_task, "summer": _summer, "params": {**_params}}
            fast_job_api_router.add_api_route(path="/task/" + _task_id,
                                              endpoint=wrapper_task,
                                              methods=["POST"],
                                              name=_task_id,
                                              summary=_summer)

            return wrapper_task

        return decorator

    @classmethod
    def task_retry(cls, work: TaskWorkRecord) -> TaskWorkRecord:
        """
        根据 TaskWorkRecord 任务重试,来对历史的运行数据进行重试
        """
        # 初始化
        start_time = time.time()
        result = []  # 默认结果为空
        success = False  # 默认失败
        error = None  # 默认没有错误

        # noinspection PyBroadException
        try:
            func = cls.__task_manage__[work.task_id]['func']
            result = [func(*work.task_args, **work.task_kwargs)]
            success = True
        except Exception as _:
            error = traceback.format_exc()
            cls.__logger__.error({"error": error, "retry_work": work.dict()})

        end_time = time.time()

        work = TaskWorkRecord(
            run_id=work.run_id,
            job_id=work.job_id,
            task_id=work.task_id,
            tag=work.tag,
            success=success,
            result=result,
            error=error,
            task_args=work.task_args,
            task_kwargs=work.task_kwargs,
            start_time=start_time,
            end_time=end_time,
            process_time=end_time - start_time,
        )
        return work

    def add_task(self, func: typing.Callable, *args: typing.Any, **kwargs: typing.Any) -> None:
        """
        重写 BackgroundTasks 的 add_task
        将运行的函数包装为 BackgroundTask
        该函数运行时会传入 result 参数进行结果存储
        """
        self.running_task = BackgroundTask(func, *args, result=self.result__collection, **kwargs)

    async def __call__(self) -> None:
        """
        重写 BackgroundTasks 的 __call__
        """
        self.result = None
        self.success = True
        self.error = None
        # noinspection PyBroadException
        try:
            await self.running_task()
        except Exception as _:
            self.result = None
            self.error = traceback.format_exc()
            self.__logger__.error({"error": self.error})
            self.success = False
        self.result = self.result__collection
        self.running_task = None

    def task_running_record(self, job_id: str, task_id: str, tag: str, job_key: str):
        """
        记录任务运行数据
        """
        work = TaskWorkRecord(
            run_id=SonyFlakeId(),
            job_id=job_id,
            task_id=task_id,
            tag=tag,
            success=self.success,
            result=self.result,
            error=self.error,
            task_args=self.__task_manage__[task_id]['args'] if task_id in self.__task_manage__ and "args" in self.__task_manage__[task_id] else (),
            task_kwargs=self.__task_manage__[task_id]['kwargs'] if task_id in self.__task_manage__ and "kwargs" in self.__task_manage__[task_id] else {},
            start_time=self.start_time,
            end_time=self.end_time,
            process_time=self.end_time - self.start_time,
        )
        self.__logger__.info({"work": work.dict(), "job_key": job_key})
        self.task_work_record_save(work=work)

    def task_work_record_save(self, work: TaskWorkRecord):
        """
        Saves task run logs to rdb
        """
        work_record_key = self.prefix + f"job_schedule:log:tag.{work.tag}:job.{work.job_id}"
        work_runtime_queue = self.prefix + f"job_schedule:log:tags:tag.{work.tag}"
        with self.__redis__.pipeline(transaction=False) as p:
            p.lpush(work_record_key, work.json())
            p.ltrim(work_record_key, 0, 99)
            p.expire(work_record_key, 7 * 60 * 60 * 24)
            p.lpush(work_runtime_queue, work.json())
            p.ltrim(work_runtime_queue, 0, 99)
            p.expire(work_runtime_queue, 7 * 60 * 60 * 24)
            p.execute()

    def list_work_record(self, job_id: str, tag: str, start: int = 0, end: int = 99):
        """
        list work record from rdb
        """
        work_record_key = self.prefix + f"job_schedule:log:tag.{tag}:job.{job_id}"
        result: typing.List[TaskWorkRecord] = []
        for work_byte in self.__redis__.lrange(work_record_key, start, end):
            work = json.loads(work_byte)
            result.append(TaskWorkRecord(**work))
        return result

    def remove_work_rdb(self, job_id: str, tag: str):
        """
        del work record from rdb
        """
        work_record_key = self.prefix + f"job_schedule:log:tag.{tag}:job.{job_id}"
        return self.redis.delete(work_record_key)

    def job_key(self, job_id: str, tag: str, ):
        return self.prefix + f"job_schedule:tag.{tag}:job.{job_id}"

    def check_job_key(self, job_id: str, tag: str) -> bool:
        return (self.prefix + f"job_schedule:tag.{tag}:") in job_id

    def get_real_job_id(self, job_id: str) -> str:
        if self.prefix in job_id:
            return job_id.split(':job.')[-1]
        else:
            return job_id

    def get_job_tag_with_job_id(self, job_id: str) -> tuple:
        if self.prefix in job_id:
            return tuple([job_id.split(':job.')[0].split(':tag.')[-1], job_id.split(':job.')[-1]])
        else:
            return tuple(['', job_id])

    def vote(self, job_key, lock_name, load_key):
        """
        投票
        :param job_key: 任务id
        :param lock_name: 分布式锁id
        :param load_key: 负载 hash_set_key
        :return: 1表示有执行权，0表示无执行权
        """
        vote_result = 0
        with self.__redis__.pipeline(transaction=False) as pipe:
            pipe.zrangebyscore(self.__registry_key, min=time.time() - 30, max=time.time())
            pipe.hexists(load_key, job_key)
            first_check = pipe.execute()
            if first_check[-1]:
                vote_result = 1
            else:
                pipe.hincrby(load_key, job_key, 1)  # 为自己投票
                pipe.expire(load_key, 30)  # 有效期30s
                pipe.execute()
                for i in range(2):
                    others = list(set([i.decode('utf8') for i in first_check[0]]) - {self.__distributed_id__})
                    for other in others:
                        pipe.hget(f"{self.__registry_key}.load:{other}", job_key)
                    other_data = pipe.execute()

                    if others:
                        for data, other in zip(other_data, others):
                            if data and int(data.decode('utf8')) >= 1:
                                pipe.hincrby(f"{self.__registry_key}.load:{other}", job_key, 1)
                        pipe.execute()

                    # 检查自己有没有锁
                    pipe.exists(lock_name)
                    pipe.hget(load_key, job_key)
                    vote_round_check = pipe.execute()

                    if vote_round_check[-2] == 0 and vote_round_check[-1] and int(vote_round_check[-1].decode('utf8')) > 1:
                        vote_result = 1
                        break
                    elif vote_round_check[-2] == 0:
                        pipe.hincrby(load_key, job_key, 1)
                        pipe.execute()
                    elif vote_round_check[-2] == 1:
                        vote_result = 0
                        break
            pipe.hdel(load_key, job_key)
            pipe.execute()
        return vote_result

    @contextlib.contextmanager
    def load_balance(self, job_key, next_job_key, distributed=False):
        """
        负载均衡
        """
        load_key = f"{self.__registry_key}.load:{self.__distributed_id__}"
        distributed_lock_key = f"{self.prefix}:lock:{job_key}"
        if distributed and self.vote(job_key=job_key, lock_name=distributed_lock_key, load_key=load_key):
            acquire = acquire_re_entrant_lock_with_timeout(conn=self.__redis__, lock_name=distributed_lock_key, identifier=self.__distributed_id__, acquire_timeout=0)
            flag = acquire(default=-1, transform=lambda x: self.__distributed_id__ if x is None else False)
            if not flag:
                with self.__redis__.pipeline(True) as pipe:
                    pipe.hset(load_key, next_job_key, 0)  # 不成功获取到锁，设置为0
                    pipe.expire(load_key, 30)
                    pipe.execute()
            yield flag
            if flag == self.__distributed_id__:
                release_re_entrant_lock(conn=self.__redis__, lock_name=distributed_lock_key, identifier=self.__distributed_id__)
        elif distributed:
            self.__redis__.hset(load_key, next_job_key, 0)  # 不成功获取到锁，设置为0
            yield False
        else:
            yield True

    async def task_scheduler(self, job_id: str, task_id: str, tag: str, *args, **kwargs) -> None:
        """
        job_id:待调度的任务
        task_id:可调度单元
        tag:标签
        """
        self.start_time = time.time()
        job = schedule.get_job(self.job_key(tag=tag, job_id=job_id))
        if job:
            current_job_next_run_time = job.next_run_time
            next_job_next_run_time = job.trigger.get_next_fire_time(current_job_next_run_time, current_job_next_run_time)
            current_job_key = job_id + ':' + str(current_job_next_run_time.timestamp())
            next_job_key = job_id + ':' + str(next_job_next_run_time.timestamp())
        else:
            current_job_key = job_id + ':' + str(time.time())
            next_job_key = job_id + ':' + str(time.time())
        with self.load_balance(job_key=current_job_key, next_job_key=next_job_key, distributed=self.__distributed) as run:
            if run:
                self.result = []
                self.success = False
                self.error = None
                # noinspection PyBroadException
                try:
                    task_func = self.__task_manage__[task_id]['wrap']
                    task_func(background_task=self, tag=tag)
                except Exception as _:
                    self.error = traceback.format_exc()
                    self.__logger__.error({"error": traceback.format_exc(), "task_manage": self.__task_manage__, "task_id": task_id})
                else:
                    await self()
                self.end_time = time.time()
                self.task_running_record(job_id=job_id, task_id=task_id, tag=tag, job_key=current_job_key)

    def init_scheduler(self, distributed=False) -> None:
        self.__distributed = distributed
        if self.__redis_job_store__:
            job_stores = {'default': self.__redis_job_store__}
        else:
            job_stores = {'default': SQLAlchemyJobStore(url='sqlite:///jobs.sqlite')}
        self.__schedule__ = AsyncIOScheduler(jobstores=job_stores)
        self.__schedule__.start()
        # 上线注册
        if self.__redis_job_store__ and self.__distributed:
            try:
                self.__redis_job_store__.redis.zadd(self.__registry_key, {self.__distributed_id__: time.time()})
                self.__redis_job_store__.redis.expire(self.__registry_key, 30)
                self.__logger__.info({"JobSchedule:register": {"id": self.__distributed_id__}})
                # 启动定时注册线程
                register_thread = Thread(target=self.cycle_registry)
                register_thread.setDaemon(True)
                register_thread.start()
            except Exception as e:
                self.__logger__.error({"JobSchedule:register_failed", e})

    def cycle_registry(self):
        while True:
            if (time.time() - self.registry_time) > self.min_registry_time / 2:
                with self.__redis_job_store__.redis.pipeline(transaction=True) as p:
                    p.zremrangebyscore(self.__registry_key, 0, time.time() - self.min_registry_time)
                    p.zadd(self.__registry_key, {self.__distributed_id__: time.time()})  # 更新上线时间
                    p.expire(self.__registry_key, 30)
                    res = p.execute()

                self.__logger__.info({"JobSchedule:register renew": {"id": self.__distributed_id__, "sleep": int(self.min_registry_time / 2), "expire": self.min_registry_time, "rem": res}})
                self.registry_time = time.time()
                time.sleep(int(self.min_registry_time / 2))
            else:
                time.sleep(1)

    def __getattr__(self, name):
        return getattr(self.__schedule__, name)

    @property
    def logger(self):
        return JobSchedule.__logger__

    @logger.setter
    def logger(self, value):
        JobSchedule.__logger__ = value

    @property
    def redis(self):
        return JobSchedule.__redis__

    @redis.setter
    def redis(self, value):
        JobSchedule.__redis__ = value

    @property
    def prefix(self):
        return JobSchedule.__prefix

    @prefix.setter
    def prefix(self, value):
        JobSchedule.__prefix = value

    @property
    def registry_key(self):
        return JobSchedule.__registry_key

    @registry_key.setter
    def registry_key(self, value):
        JobSchedule.__registry_key = value

    @property
    def run_times_key(self):
        return JobSchedule.__run_times_key

    @run_times_key.setter
    def run_times_key(self, value):
        JobSchedule.__run_times_key = value

    @property
    def jobs_key(self):
        return JobSchedule.__jobs_key

    @jobs_key.setter
    def jobs_key(self, value):
        JobSchedule.__jobs_key = value


# Create a Schedule object
schedule: typing.Union[AsyncIOScheduler, JobSchedule] = JobSchedule()
# Only instantiation objects are allowed to be exported
__all__ = ["schedule", "JobSchedule", "fast_job_api_router"]
