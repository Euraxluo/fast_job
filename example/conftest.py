# -*- coding: utf-8 -*- 
# Time: 2022-02-11 11:59
# Copyright (c) 2022
# author: Euraxluo

import json
from typing import *
import redis  # type:ignore


class RedisHelper(object):
    def __init__(self, host: str = None, port: int = None, db: int = None,
                 password: str = None, decode_responses: bool = True, **kwargs: Dict):
        self._redis = redis.Redis(host=host, port=port, db=db,
                                  password=password,
                                  decode_responses=decode_responses)
    @property
    def redis(self) -> redis.Redis:
        return self._redis

    @staticmethod
    def encode(data: dict, default: dict = None) -> str:
        if default is None:
            default = {}
        if data:
            return json.dumps(data)
        return json.dumps(default)

    @staticmethod
    def decode(data: Union[str, bytes], instance: Callable = str) -> Any:
        if data:
            return json.loads(data)
        return instance().__dict__()


rdb = RedisHelper(host="127.0.0.1", port=6379, db=0, password="redis",decode_responses=False).redis
