"""Example showcasing producer and consumers with a dynamic amount of workers
and local vs. remote setup."""
import argparse
import random
import threading
import time
from typing import Literal, NoReturn

import redipy
from redipy import RedisClientAPI


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
                f"{heartbeat_key}:{worker_id}", "1", mode="if_missing"):
            worker_num += 1
            continue
        print(f"register worker {worker_id}")
        return worker_id


def heartbeat(
        worker_id: str,
        client: RedisClientAPI,
        heartbeat_key: str) -> NoReturn:
    """
    Signals that the current worker is still active.

    Args:
        worker_id (str): The worker id.
        client (RedisClientAPI): The redis client.
        heartbeat_key (str): The heartbeat key base.

    Returns:
        NoReturn: If the function exits the worker will become inactive.
    """
    while True:
        client.set(f"{heartbeat_key}:{worker_id}", "1", expire_in=2.0)
        time.sleep(1.0)


def detect_stale_workers(
        client: RedisClientAPI,
        queue_key: str,
        busy_key: str,
        heartbeat_key: str) -> NoReturn:
    while True:
        for task_id, worker_id in client.hgetall(busy_key).items():
            if client.exists(f"{heartbeat_key}:{worker_id}"):
                continue
            with client.pipeline() as pipe:
                pipe.zadd(queue_key, {task_id: 2.0})
                pipe.hdel(busy_key, task_id)
                pipe.zcard(queue_key)
                _, _, count = pipe.execute()
            print(f"readd task {task_id} (total tasks {count})")


def enqueue_task(
        client: RedisClientAPI,
        queue_key: str,
        info_key: str,
        priority: float,
        task_id: str,
        task_payload: float) -> None:
    with client.pipeline() as pipe:
        pipe.hset(info_key, {task_id: f"{task_payload}"})
        pipe.zadd(queue_key, {task_id: priority})
        pipe.zcard(queue_key)
        _, _, count = pipe.execute()
    print(f"add task {task_id} (payload {task_payload}; total tasks {count})")


def pick_task(
        worker_id: str,
        client: RedisClientAPI,
        queue_key: str,
        info_key: str,
        busy_key: str) -> tuple[str, float] | None:
    res = client.zpop_max(queue_key)
    if not res:
        return None
    task_id = res[0][0]
    task_payload = client.hget(info_key, task_id)
    assert task_payload is not None
    client.hset(busy_key, {task_id: worker_id})
    return task_id, float(task_payload)


def execute_task(
        worker_id: str,
        client: RedisClientAPI,
        info_key: str,
        busy_key: str,
        task_id: str,
        task_payload: float) -> None:
    print(f"[{worker_id}] start task {task_id} ({task_payload})")
    if task_payload > 0.0:
        time.sleep(task_payload)
    print(f"[{worker_id}] finished task {task_id}")
    with client.pipeline() as pipe:
        pipe.hdel(info_key, task_id)
        pipe.hdel(busy_key, task_id)


def consume_task_loop(
        worker_id: str,
        client: RedisClientAPI,
        queue_key: str,
        info_key: str,
        busy_key: str) -> NoReturn:
    while True:
        task = pick_task(worker_id, client, queue_key, info_key, busy_key)
        if task is None:
            time.sleep(1.0)
            continue
        task_id, task_payload = task
        execute_task(
            worker_id, client, info_key, busy_key, task_id, task_payload)


def produce_task_loop(
        client: RedisClientAPI, queue_key: str, info_key: str) -> NoReturn:
    task_num = 0
    while True:
        priority = random.choice([0, 0.25, 0.5, 0.75, 1.0])
        task_id = f"t{task_num:08x}"
        task_payload = random.choice([0, 0.25, 0.5, 0.75, 1.0])
        enqueue_task(
            client, queue_key, info_key, priority, task_id, task_payload)
        time.sleep(0.25)
        task_num += 1


def parse_args() -> Literal["single", "producer", "worker"]:
    parser = argparse.ArgumentParser(
        description="Worker Example")
    parser.add_argument("mode", choices=["single", "producer", "worker"])

    args = parser.parse_args()
    return args.mode


def run() -> None:
    mode = parse_args()
    queue_key = "task_queue"
    info_key = "task_info"
    busy_key = "busy_tasks"
    heartbeat_key = "heartbeat"

    def do_cleanup(client: RedisClientAPI) -> NoReturn:
        detect_stale_workers(client, queue_key, busy_key, heartbeat_key)

    def do_produce(client: RedisClientAPI) -> NoReturn:
        produce_task_loop(client, queue_key, info_key)

    def do_heartbeat(worker_id: str, client: RedisClientAPI) -> NoReturn:
        heartbeat(worker_id, client, heartbeat_key)

    def do_consume(worker_id: str, client: RedisClientAPI) -> NoReturn:
        consume_task_loop(worker_id, client, queue_key, info_key, busy_key)

    client = redipy.Redis(cfg={
        "host": "localhost",
        "port": 6379,
        "passwd": "",
        "prefix": "",
    })

    if mode == "single":
        pass
    elif mode == "producer":
        threading.Thread(
            target=do_cleanup, args=(client,), daemon=True).start()
        threading.Thread(target=do_produce, args=(client,)).start()
    elif mode == "worker":
        worker_id = get_worker_id(client, heartbeat_key)
        threading.Thread(
            target=do_heartbeat, args=(worker_id, client), daemon=True).start()
        threading.Thread(target=do_consume, args=(worker_id, client)).start()
    else:
        raise ValueError(f"invalid mode: {mode}")


if __name__ == "__main__":
    run()
