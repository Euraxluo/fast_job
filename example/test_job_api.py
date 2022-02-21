# -*- coding: utf-8 -*- 
# Time: 2022-02-21 09:39
# Copyright (c) 2022
# author: Euraxluo


import contextlib
from unittest import TestCase
from fastapi.testclient import TestClient

from loguru import logger
from fastapi import FastAPI
from example.jobs import schedule, fast_job_api_router
from example.conftest import rdb as redis

app = FastAPI()


@app.on_event("startup")
async def registry_schedule():
    schedule.setup(prefix='test:', logger=logger, redis=redis, distributed=True)


@app.on_event("shutdown")  # 关闭调度器
async def shutdown_connect():
    schedule.shutdown()


prefix = "/test"
app.include_router(fast_job_api_router, prefix=prefix, tags=["jobs"])  # include router


@contextlib.contextmanager
def test_client(application):
    import asyncio
    new_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(new_loop)
    with TestClient(application) as client:
        yield client


class Test(TestCase):

    def test_event(self):
        with test_client(app) as _:
            print("schedule init finish")

    def test_app(self):
        for route in app.routes:
            if hasattr(route, "methods"):
                logger.info({'path': route.path, 'name': route.name, 'methods': route.methods})

    def test_task1(self):
        with test_client(app) as client:
            response = client.post(f"{prefix}/task/task1", params={"tag": "1"}, headers={'accept': 'application/json'})
            assert response.status_code == 200
            assert response.json().get('code') == 200

    def test_task2(self):
        with test_client(app) as client:
            response = client.post(f"{prefix}/task/task2", params={"tag": "1"}, headers={'accept': 'application/json'})
            assert response.status_code == 200
            assert response.json().get('code') == 200

    def test_tasks(self):
        with test_client(app) as client:
            response = client.get(f"{prefix}/schedule/tasks", headers={'accept': 'application/json'})
            assert response.status_code == 200
            assert response.json().get('code') == 200
            all_tasks = response.json()
            print(all_tasks)

    def test_interval(self):
        with test_client(app) as client:
            all_tasks = client.get(f"{prefix}/schedule/tasks", headers={'accept': 'application/json'}).json()
            for task_id, v in all_tasks.get('data').items():
                job_id = v.get("summer")
                tag = v.get("params").get("tag") + '_interval'
                response = client.post(f"{prefix}/schedule/interval", params={"tag": tag}, json={
                    "job_id": job_id,
                    "task_id": task_id,
                    "run_time": "2022-02-21T12:44:35.765510",
                    "seconds": 10,
                }, headers={'accept': 'application/json'})
                assert response.status_code == 200

                job_response = client.get(f"{prefix}/schedule/job", params={"job_id": job_id, "tag": tag}, headers={'accept': 'application/json'})
                assert job_response.status_code == 200
                assert job_response.json().get('code') == 200

    def test_cron(self):
        with test_client(app) as client:
            all_tasks = client.get(f"{prefix}/schedule/tasks", headers={'accept': 'application/json'}).json()
            for task_id, v in all_tasks.get('data').items():
                job_id = v.get("summer")
                tag = v.get("params").get("tag") + '_cron'
                response = client.post(f"{prefix}/schedule/cron", params={"tag": tag}, json={
                    "job_id": job_id,
                    "task_id": task_id,
                    "run_time": "2022-02-21T12:44:35.765510",
                    "crontab": "*/8 * * * * * *",
                }, headers={'accept': 'application/json'})
                assert response.status_code == 200

                job_response = client.get(f"{prefix}/schedule/job", params={"job_id": job_id, "tag": tag}, headers={'accept': 'application/json'})
                assert job_response.status_code == 200
                assert job_response.json().get('code') == 200

    def test_datetime(self):
        with test_client(app) as client:
            import time, datetime
            run_time = (datetime.datetime.now() + datetime.timedelta(seconds=2)).strftime("%Y-%m-%dT%H:%M:%S")
            all_tasks = client.get(f"{prefix}/schedule/tasks", headers={'accept': 'application/json'}).json()
            for task_id, v in all_tasks.get('data').items():
                job_id = v.get("summer")
                tag = v.get("params").get("tag") + '_datetime'

                response = client.post(f"{prefix}/schedule/datetime", params={"tag": tag}, json={
                    "job_id": job_id,
                    "task_id": task_id,
                    "run_time": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                }, headers={'accept': 'application/json'})
                assert response.status_code == 200
                print("test_datetime", response.json())

                response = client.post(f"{prefix}/schedule/datetime", params={"tag": tag}, json={
                    "job_id": job_id,
                    "task_id": task_id,
                    "run_time": run_time,
                }, headers={'accept': 'application/json'})
                assert response.status_code == 200
                print("test_datetime", response.json())

                job_response = client.get(f"{prefix}/schedule/job", params={"job_id": job_id, "tag": tag}, headers={'accept': 'application/json'})
                print("test_datetime", job_response.json())
            time.sleep(5)

    def test_scheduling(self):
        with test_client(app) as client:
            tags = ["test", "test_cron", "test_datetime", "test_interval"]
            for tag in tags:
                scheduling_response = client.get(f"{prefix}/schedule/scheduling", params={"tag": tag}, headers={'accept': 'application/json'})
                assert scheduling_response.status_code == 200
                print(scheduling_response.json())

    def test_log(self):
        with test_client(app) as client:
            tags = ["test", "test_cron", "test_datetime", "test_interval"]
            for tag in tags:
                scheduling_reps = client.get(f"{prefix}/schedule/scheduling", params={"tag": tag}, headers={'accept': 'application/json'}).json()
                if scheduling_reps.get("code") != 200:
                    continue
                for schedule_item in scheduling_reps.get('data').get("schedules"):
                    job_id = schedule_item.get("job_id")
                    tag = schedule_item.get("tag")

                    response = client.get(f"{prefix}/schedule/log", params={"tag": tag, "job_id": job_id, "start": 0, "end": 5}, headers={'accept': 'application/json'})
                    assert response.status_code == 200
                    assert response.json().get("code") == 200
                    print("test_log", response.json())

    def test_retry(self):
        with test_client(app) as client:
            tags = ["test", "test_cron", "test_datetime", "test_interval"]
            for tag in tags:
                scheduling_reps = client.get(f"{prefix}/schedule/scheduling", params={"tag": tag}, headers={'accept': 'application/json'}).json()
                if scheduling_reps.get("code") != 200:
                    continue
                for schedule_item in scheduling_reps.get('data').get("schedules"):
                    job_id = schedule_item.get("job_id")
                    tag = schedule_item.get("tag")

                    response = client.get(f"{prefix}/schedule/log", params={"tag": tag, "job_id": job_id, "start": 0, "end": 5}, headers={'accept': 'application/json'})
                    assert response.status_code == 200
                    assert response.json().get("code") == 200

                    response = client.post(f"{prefix}/schedule/retry", params={"tag": tag}, json=response.json().get("data").get("log")[0], headers={'accept': 'application/json'})
                    assert response.status_code == 200
                    assert response.json().get("code") == 200
                    print(f"{prefix}/schedule/retry", response.json())

    def test_pause(self):
        with test_client(app) as client:
            tags = ["test", "test_cron", "test_datetime", "test_interval"]
            for tag in tags:
                scheduling_reps = client.get(f"{prefix}/schedule/scheduling", params={"tag": tag}, headers={'accept': 'application/json'}).json()
                if scheduling_reps.get("code") != 200:
                    continue
                for schedule_item in scheduling_reps.get('data').get("schedules"):
                    job_id = schedule_item.get("job_id")
                    tag = schedule_item.get("tag")

                    response = client.post(f"{prefix}/schedule/pause/{job_id}", params={"tag": tag}, headers={'accept': 'application/json'})
                    assert response.status_code == 200
                    assert response.json().get("code") == 200

    def test_resume(self):
        with test_client(app) as client:
            tags = ["test", "test_cron", "test_datetime", "test_interval"]
            for tag in tags:
                scheduling_reps = client.get(f"{prefix}/schedule/scheduling", params={"tag": tag}, headers={'accept': 'application/json'}).json()
                if scheduling_reps.get("code") != 200:
                    continue
                for schedule_item in scheduling_reps.get('data').get("schedules"):
                    job_id = schedule_item.get("job_id")
                    tag = schedule_item.get("tag")

                    response = client.post(f"{prefix}/schedule/resume/{job_id}", params={"tag": tag}, headers={'accept': 'application/json'})
                    assert response.status_code == 200
                    assert response.json().get("code") == 200

    def test_remove(self):
        with test_client(app) as client:
            tags = ["test", "test_cron", "test_datetime", "test_interval"]
            for tag in tags:
                scheduling_reps = client.get(f"{prefix}/schedule/scheduling", params={"tag": tag}, headers={'accept': 'application/json'}).json()
                if scheduling_reps.get("code") != 200:
                    continue
                for schedule_item in scheduling_reps.get('data').get("schedules"):
                    job_id = schedule_item.get("job_id")
                    tag = schedule_item.get("tag")

                    response = client.post(f"{prefix}/schedule/remove/{job_id}", params={"tag": tag}, headers={'accept': 'application/json'})
                    print(response.json())
                    assert response.status_code == 200
                    assert response.json().get("code") == 200

    def test_modify(self):
        with test_client(app) as client:
            tags = ["test", "test_cron", "test_datetime", "test_interval"]
            for tag in tags:
                scheduling_reps = client.get(f"{prefix}/schedule/scheduling", params={"tag": tag}, headers={'accept': 'application/json'}).json()
                if scheduling_reps.get("code") != 200:
                    continue
                for schedule_item in scheduling_reps.get('data').get("schedules"):
                    job_id = schedule_item.get("job_id")
                    task_id = schedule_item.get("task_id")
                    tag = schedule_item.get("tag")
                    if "cron" not in tag:
                        response = client.post(f"{prefix}/schedule/modify", params={"tag": tag}, json={
                            "job_id": job_id,
                            "task_id": task_id,
                            "trigger": "cron",
                            "run_time": "2022-02-21T12:44:35.765510",
                            "crontab": "*/1 * * * * * *",
                            "seconds": 1,
                        }, headers={'accept': 'application/json'})
                    else:
                        response = client.post(f"{prefix}/schedule/modify", params={"tag": tag}, json={
                            "job_id": job_id,
                            "task_id": task_id,
                            "trigger": "interval",
                            "run_time": "2022-02-21T12:44:35.765510",
                            "crontab": "*/4 * * * * * *",
                            "seconds": 4,
                        }, headers={'accept': 'application/json'})
                    print(response.json())
                    assert response.status_code == 200

                    scheduling_reps = client.get(f"{prefix}/schedule/scheduling", params={"tag": tag}, headers={'accept': 'application/json'}).json()
                    print("modify finish", scheduling_reps)
