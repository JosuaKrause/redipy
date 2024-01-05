# Copyright 2024 Josua Krause
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Example showcasing producer and consumers with a dynamic amount of workers
and local vs. remote setup."""
import argparse
import random
import threading
import time
from typing import cast, Literal, NoReturn

import redipy
from redipy import ExecFunction, RedisClientAPI
from redipy.script import FnContext, RedisHash, RedisSortedSet, RedisVar, ToNum


def get_worker_id(client: RedisClientAPI, heartbeat_key: str) -> str:
    """
    Assigns a unique worker id.

    Args:
        client (RedisClientAPI): The redis client.

        heartbeat_key (str): The heartbeat key base.

    Returns:
        str: The unique worker id.
    """
    worker_num = 0
    while True:
        worker_id = f"w{worker_num:08x}"
        if not client.set(
                f"{heartbeat_key}:{worker_id}", "init", mode="if_missing"):
            worker_num += 1
            continue
        print(f"register worker {worker_id}")
        return worker_id


def worker_heartbeat(
        worker_id: str,
        client: RedisClientAPI,
        heartbeat_key: str) -> NoReturn:
    """
    Signals that the current worker is still active.

    Args:
        worker_id (str): The worker id.

        client (RedisClientAPI): The redis client.

        heartbeat_key (str): The heartbeat key base.
    """
    while True:
        client.set(f"{heartbeat_key}:{worker_id}", "alive", expire_in=2.0)
        time.sleep(1.0)


def detect_stale_workers(
        client: RedisClientAPI,
        queue_key: str,
        busy_key: str,
        heartbeat_key: str) -> NoReturn:
    """
    Goes through the list of active tasks and checks whether the associated
    workers are still alive. If not, the task is added back to the task queue
    with a higher priority.

    Args:
        client (RedisClientAPI): The redis client.

        queue_key (str): The task queue key.

        busy_key (str): The busy hash key.

        heartbeat_key (str): The heartbeat key base.
    """

    def _task_check() -> ExecFunction:
        ctx = FnContext()
        queue = RedisSortedSet(ctx.add_key("queue"))
        busy = RedisHash(ctx.add_key("busy"))
        heartbeat = RedisVar(ctx.add_key("heartbeat"))
        task = ctx.add_arg("task")

        res = ctx.add_local(None)
        b_then, _ = ctx.if_(heartbeat.exists().eq_(0))
        b_then.add(queue.add(2.0, task))
        b_then.add(busy.hdel(task))
        b_then.add(res.assign(queue.card()))

        ctx.set_return_value(res)

        return client.register_script(ctx)

    task_check = _task_check()
    while True:
        for task_id, worker_id in client.hgetall(busy_key).items():
            cur = cast(int | None, task_check(
                keys={
                    "queue": queue_key,
                    "busy": busy_key,
                    "heartbeat": f"{heartbeat_key}:{worker_id}",
                },
                args={
                    "task": task_id,
                }))
            if cur is not None:
                print(f"readd task {task_id} (total tasks {cur})")
        time.sleep(1.0)


def enqueue_task(
        client: RedisClientAPI,
        queue_key: str,
        info_key: str,
        priority: float,
        task_id: str,
        task_payload: float) -> None:
    """
    Adds a new task to the task queue.

    Args:
        client (RedisClientAPI): The redis client.

        queue_key (str): The task queue key.

        info_key (str): The task info hash key.

        priority (float): The priority of the task.

        task_id (str): The task id.

        task_payload (float): The task payload.
    """
    with client.pipeline() as pipe:
        pipe.hset(info_key, {task_id: f"{task_payload}"})
        pipe.zadd(queue_key, {task_id: priority})
        pipe.zcard(queue_key)
        _, _, count = pipe.execute()
    print(f"add task {task_id} (payload {task_payload}; total tasks {count})")


def execute_task(
        worker_id: str,
        task_id: str,
        task_payload: float) -> None:
    """
    Executes a task.

    Args:
        worker_id (str): The worker id.

        task_id (str): The task id.

        task_payload (float): The task payload.
    """
    print(f"[{worker_id}] start task {task_id} ({task_payload})")
    if task_payload > 0.0:
        time.sleep(task_payload)
    print(f"[{worker_id}] finished task {task_id}")


def consume_task_loop(
        worker_id: str,
        client: RedisClientAPI,
        queue_key: str,
        info_key: str,
        busy_key: str) -> NoReturn:
    """
    Consumes and executes tasks in a loop.

    Args:
        worker_id (str): The worker id.

        client (RedisClientAPI): The redis client.

        queue_key (str): The task queue key.

        info_key (str): The task info hash key.

        busy_key (str): The busy hash key.
    """

    def _pick_task() -> ExecFunction:
        ctx = FnContext()
        queue = RedisSortedSet(ctx.add_key("queue"))
        info = RedisHash(ctx.add_key("info"))
        busy = RedisHash(ctx.add_key("busy"))
        worker = ctx.add_arg("worker")

        res = ctx.add_local([])
        loop, _, task = ctx.for_(queue.pop_max())
        loop.add(res.set_at(res.len_(), task[0]))
        loop.add(res.set_at(res.len_(), ToNum(info.hget(task[0]))))
        loop.add(busy.hset({task[0]: worker}))

        ctx.set_return_value(res)
        return client.register_script(ctx)

    pick_task = _pick_task()
    while True:
        task = cast(list, pick_task(
            keys={
                "queue": queue_key,
                "info": info_key,
                "busy": busy_key,
            },
            args={
                "worker": worker_id,
            }))
        if not task:
            time.sleep(1.0)
            continue
        task_id, task_payload = task
        execute_task(worker_id, task_id, task_payload)
        with client.pipeline() as pipe:
            pipe.hdel(info_key, task_id)
            pipe.hdel(busy_key, task_id)


def produce_task_loop(
        client: RedisClientAPI, queue_key: str, info_key: str) -> NoReturn:
    """
    Produces new tasks in a loop.

    Args:
        client (RedisClientAPI): The redis client.

        queue_key (str): The task queue key.

        info_key (str): The task info hash key.
    """
    task_num = 0
    while True:
        priority = random.choice([0, 0.25, 0.5, 0.75, 1.0])
        task_id = f"t{task_num:08x}"
        task_payload = random.choice([0, 0.25, 0.5, 0.75, 1.0])
        enqueue_task(
            client, queue_key, info_key, priority, task_id, task_payload)
        time.sleep(0.25)
        task_num += 1


def parse_args() -> tuple[Literal["single", "producer", "worker"], bool]:
    """
    Parses the command line arguments.

    Returns:
        str: The mode.
    """
    parser = argparse.ArgumentParser(description="Worker Example")
    parser.add_argument(
        "mode",
        choices=["single", "producer", "worker"],
        help=(
            "Which mode to run. Single is fully self contained. "
            "Producer and worker run in separate processes together."
        ))
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="If set all generated lua scripts are printed to stdout.")

    args = parser.parse_args()
    return args.mode, args.verbose


def run() -> None:
    """Runs the app."""
    mode, lua_verbose = parse_args()
    queue_key = "task_queue"
    info_key = "task_info"
    busy_key = "busy_tasks"
    heartbeat_key = "heartbeat"

    def do_cleanup(client: RedisClientAPI) -> NoReturn:
        detect_stale_workers(client, queue_key, busy_key, heartbeat_key)

    def do_produce(client: RedisClientAPI) -> NoReturn:
        produce_task_loop(client, queue_key, info_key)

    def do_heartbeat(worker_id: str, client: RedisClientAPI) -> NoReturn:
        worker_heartbeat(worker_id, client, heartbeat_key)

    def do_consume(worker_id: str, client: RedisClientAPI) -> NoReturn:
        consume_task_loop(worker_id, client, queue_key, info_key, busy_key)

    def print_lua(code: list[str]) -> None:
        for line in code:
            print(line)

    if mode == "single":
        client = redipy.Redis()
        for wid in range(3):
            threading.Thread(
                target=do_consume,
                args=(f"w{wid:08x}", client),
                daemon=True).start()
        do_produce(client)
    else:
        client = redipy.Redis(
            cfg={
                "host": "localhost",
                "port": 6379,
                "passwd": "",
                "prefix": "",
            },
            lua_code_hook=print_lua if lua_verbose else None)
        if mode == "producer":
            threading.Thread(
                target=do_cleanup,
                args=(client,),
                daemon=True).start()
            do_produce(client)
        elif mode == "worker":
            worker_id = get_worker_id(client, heartbeat_key)
            threading.Thread(
                target=do_heartbeat,
                args=(worker_id, client),
                daemon=True).start()
            do_consume(worker_id, client)
        else:
            raise ValueError(f"invalid mode: {mode}")


if __name__ == "__main__":
    run()
