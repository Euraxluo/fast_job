# -*- coding: utf-8 -*-
# Time: 2021-04-19 10:59
# Copyright (c) 2021
# author: Euraxluo


import nest_asyncio

nest_asyncio.apply()

from fastapi import FastAPI
from loguru import logger

from fast_job import schedule, fast_job_api_router
from example.jobs import router
from example.conftest import rdb as redis



async def registry_schedule():
    # TasksRdbRecord.set_redis_client(redis)  # 设置redis客户端
    # TasksRdbRecord.get_record_rdb_key = lambda site, job_id: f'jobs:log:site:{site}:task:{job_id}'  # 设置日志key
    # TasksRdbRecord.get_record_runtime_queue_rdb_key = lambda site: f'jobs:log_view:site:{str(site)}'  # 设置key
    schedule.setup(prefix='test:', logger=logger, redis=redis, distributed=True)
    # 设置其他的key

    # 初始化调度器
    # schedule.init_scheduler(distributed=True)


async def shutdown_connect():
    logger.debug("server shutdown")
    schedule.shutdown()


app = FastAPI()
# fast_job_api_router.add_event_handler()

app.add_event_handler("startup", registry_schedule)  # 注册调度器
app.add_event_handler("shutdown", shutdown_connect)  # 关闭调度器
# fast_job_api_router.mount("/task", router)
app.include_router(fast_job_api_router, prefix='/test', tags=["jobs"])  # include router
app.include_router(router, prefix='/task', tags=["task"])  # include router

if __name__ == '__main__':
    import uvicorn

    uvicorn.run(app='test:app', access_log=True)
