# -*- coding: utf-8 -*- 
# Time: 2022-02-15 10:57
# Copyright (c) 2022
# author: Euraxluo

import redis
import time
import math
import threading
import typing
from redis import Redis


class _WatchThread(threading.Thread):
    def __init__(self, target, args=(), kwargs=None):
        super(_WatchThread, self).__init__()
        if kwargs is None:
            kwargs = {}
        self.func = target
        self.args = args
        self.kwargs = kwargs
        self.result = None

    def run(self):
        # 接受返回值
        self.result = self.func(*self.args, **self.kwargs)

    def get_result(self, default=None, transform=lambda x: x):
        # 线程不结束,返回值为None
        # noinspection PyBroadException
        try:
            return transform(self.result)
        except Exception as _:
            return transform(default)


# 加载脚本并执行的函数
def _script_load(script):
    sha = [None]

    def call(conn: Redis, keys=None, args=None, force_eval=False):
        if args is None:
            args = []
        if keys is None:
            keys = []
        if not force_eval:
            # 加载并缓存校验和
            if not sha[0]:
                sha[0] = conn.execute_command("SCRIPT", "LOAD", script, parse="LOAD")
                if isinstance(conn, redis.client.Pipeline):
                    sha[0] = conn.execute()[0]
            try:
                return conn.execute_command("EVALSHA", sha[0], len(keys), *(keys + args))
            except redis.exceptions.ResponseError as msg:
                if not msg.args[0].startswith("NOSCRIPT"):
                    raise
        # 如果需要强制执行或者脚本接收到错误时，会使用eval执行脚本，eval执行完脚本后，会把脚本的sha1值缓存下来
        return conn.execute_command("EVAL", script, len(keys), *(keys + args))

    return call


"""添加互斥锁"""
_acquire_lock_with_timeout_lua = _script_load(
    """
    if (redis.call('exists',KEYS[1]) == 0) then
        return redis.call('setex',KEYS[1],unpack(ARGV))
    end
    """
)


def acquire_lock_with_timeout(conn: Redis, lock_name: str, identifier: str, acquire_timeout: int = 10, lock_timeout: int = 10):
    lock_name = lock_name
    lock_timeout = int(math.ceil(lock_timeout))

    acquired = False
    end = time.time() + acquire_timeout
    while time.time() < end and not acquired:
        acquired = _acquire_lock_with_timeout_lua(conn, [lock_name], [lock_timeout, identifier]) == 'OK'
        time.sleep(0.001 * (not acquired))
    return acquired and identifier


"""释放互斥锁"""
_release_lock_lua = _script_load(
    """
    if (redis.call('get',KEYS[1])== ARGV[1]) then
        return redis.call('del',KEYS[1]) or true
    end
    """
)


def release_lock(conn: Redis, lock_name: str, identifier: str):
    lock_name = lock_name
    return _release_lock_lua(conn, [lock_name], [identifier])


"""为可重入锁续约"""
_renew_re_entrant_lock_lua = _script_load(
    # KEYS : lock_name
    # ARGV : lock_timeout,id
    """
    if (redis.call('hexists',KEYS[1],ARGV[2])==1) then
        redis.call('pexpire',KEYS[1],ARGV[1])
        return 1
    end
    return 0
    """
)


def _renew_re_entrant_lock(conn: Redis, lock_name: str, identifier: str, lock_timeout: int):
    """
    为锁续约
    :param conn:redis链接
    :param lock_name:lock_key
    :param identifier:lock_唯一id
    :param lock_timeout: 上锁超时时间ms
    :return:
    """
    while _renew_re_entrant_lock_lua(conn, [lock_name], [lock_timeout, identifier]):
        # 若未续约成功则直接退出
        time.sleep(lock_timeout / 3000)


"""添加可重入锁"""


def _watch_operation_time(limit_time: int):
    """
    监视运行时间
    :param limit_time:
    :return:
    """

    def functions(func):
        def run(*params):
            watch_thread = _WatchThread(target=func, args=params)
            watch_thread.setDaemon(True)
            watch_thread.start()
            for i in range(int(limit_time // 0.001)):
                if watch_thread.get_result(default=-1) != -1:
                    return watch_thread.get_result
                time.sleep(0.001)
            time.sleep(round(limit_time - int(limit_time // 0.001) * 0.001, 4))
            return watch_thread.get_result  # 时间结束后,返回thread对象

        return run

    return functions


_acquire_re_entrant_lock_with_timeout_lua = _script_load(
    # KEYS : lock_name
    # ARGV : lock_timeout,id

    # 如果exists 结果为 0 ，标识无锁，加锁，将本线程获取的id加到hset中，重置过期时间，返回nil
    # 相同线程加锁：exists 为 1，此时判断本进程id 是否存在于hset中，若存在，将信号量+1，并重置过期时间，返回nil
    # 其他线程加锁：直接pttl key，返回剩余过期时间，脚本未返回nil，加锁失败
    """
    if (redis.call('exists',KEYS[1]) == 0) then
        redis.call('hset',KEYS[1],ARGV[2],1)
        redis.call('pexpire',KEYS[1],ARGV[1])
        return nil
    end
    if (redis.call('hexists',KEYS[1],ARGV[2])==1) then
        redis.call('hincrby',KEYS[1],ARGV[2],1)
        redis.call('pexpire',KEYS[1],ARGV[1])
        return nil
    end
    return redis.call('pttl',KEYS[1])
    """
)


def acquire_re_entrant_lock_with_timeout(conn: Redis, lock_name: str, identifier: str, acquire_timeout: int = -1, lock_timeout: int = -1) -> typing.Callable:
    """
    :param conn: Redis链接
    :param lock_name: 锁名
    :param acquire_timeout: 加锁超时时间 单位s
    :param lock_timeout: 锁的原始租期 单位ms
    :return:
    """
    end = time.time() + acquire_timeout if acquire_timeout > 0 else float('inf')

    acquired = False
    identifier: str = identifier
    lock_name = lock_name
    lock_timeout = int(math.ceil(lock_timeout))

    # 判断是否超时监测
    def acquire_lock():
        acquire_lock_lua_result = _acquire_re_entrant_lock_with_timeout_lua(conn, [lock_name], [30 * 1000 if lock_timeout == -1 else lock_timeout, identifier])

        def result(*args, **kwargs):
            return acquire_lock_lua_result

        return result  # 如果不是返回ttl，则成功拿到锁

    # cant write this
    if end < float('inf'):
        acquire_lock = _watch_operation_time(limit_time=int(end - time.time()))(acquire_lock)

    lua_result = acquire_lock()
    if lua_result(default=-1) == -1:
        # 请求锁超时,但是依然将这个future对象传出
        return lua_result
    else:
        if lua_result(default=-1) is None:
            # 获取锁成功
            if lock_timeout == -1:
                watch_thread = _WatchThread(target=_renew_re_entrant_lock, args=[conn, lock_name, identifier, 30 * 1000])
                watch_thread.setDaemon(True)
                watch_thread.start()

            def result(*args, **kwargs):
                return identifier

            return result
        elif acquire_timeout != 0:
            while time.time() < end and not acquired:
                lua_result = _acquire_re_entrant_lock_with_timeout_lua(conn, [lock_name], [lock_timeout, identifier])
                acquired = lua_result is None
                time.sleep(0.001 * (not acquired))

            def result(*args, **kwargs):
                return acquired and identifier

            return result
        else:
            def result(*args, **kwargs):
                return acquired and identifier

            return result


"""释放可重入锁"""
_release_re_entrant_lock_lua = _script_load(
    # KEYS: lock name
    # ARGV ：lock_timeout,id

    # 1. 如果exists 结果为 0 ，**标识无锁**，发送释放锁的消息，返回2，释放成功
    # 2. 如果key存在，但是自己不是持锁者，**无权释放**，返回 nil
    # 3. 如果自己是持锁者，直接-1，然后**判断是否还有重入锁，刷新过期时间**，返回0
    # 4. 如果**无其他重入锁，则删除key**，释放锁，并发送释放消息，并返回1，释放成功
    """
    if (redis.call('exists',KEYS[1]) == 0) then
        return 2
    end
    if (redis.call('hexists',KEYS[1],ARGV[2])==0) then
        return nil
    end
    local counter = redis.call('hincrby',KEYS[1],ARGV[2],-1)
    if (counter > 0) then
        redis.call('pexpire',KEYS[1],ARGV[1])
        return 0
    else
        redis.call('del',KEYS[1])
        return 1
    end
    return nil
    """
)


def release_re_entrant_lock(conn: Redis, lock_name: str, identifier: str, lock_timeout: int = 30 * 1000) -> typing.Union[int, None]:
    """
    释放可重入锁
    :param conn:
    :param lock_name:
    :param identifier:
    :param lock_timeout:
    :return: 0：释放了自己的锁,当还有别的可重入锁 1：释放成功 2:无锁 None:无权释放
    """
    lock_name = lock_name
    return _release_re_entrant_lock_lua(conn, [lock_name], [lock_timeout, identifier])


__all__ = ["acquire_re_entrant_lock_with_timeout", "release_re_entrant_lock", "acquire_lock_with_timeout", "release_lock"]
