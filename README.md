### typing_environs 
- name = "fast_job"
- description = "Provides scheduling apis and scheduling and task-related services"
- authors = ["Euraxluo <euraxluo@qq.com>"]
- license = "The MIT LICENSE"
- repository = "https://github.com/Euraxluo/fast_job"

#### install
`pip install fast-job`

#### UseAge
```
from fast_job import schedule, task_api_router_init


@schedule.task('test', summer="test.py", site=1, description="test")
def test(site: int):
    if site == 67:
        print("报错")
        raise Exception("city error")
    if site == 1:
        import time
        time.sleep(4)
    print("test.py", site)
    return site


router = task_api_router_init()
```