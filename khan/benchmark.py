import time
import threading

from .synch import red
from .aws import CloudThread


class Task(object):
    """Task unit to perform the benchmark. One action over an object."""

    def __init__(self, task_id, parallelism, repetitions):
        self.task_id = task_id
        self.parallelism = parallelism
        self.repetitions = repetitions
        self.times = []
        self.object = None

    def __call__(self):
        beg = time.time()
        for _ in range(self.repetitions):
            start = time.time_ns()
            self.do_task()
            stop = time.time_ns() - start
            self.times.append(stop)
        return (time.time() - beg) / self.repetitions

    def do_task(self):
        raise NotImplementedError

    def new_object(self):
        raise NotImplementedError

    def set_object(self, obj):
        self.object = obj


def run_task(task_impl, task_id, parallelism, repetitions, redis_conf):

    red.RedisConn.set_up(**redis_conf)
    barrier = red.RedisBarrier('benchmark-'+str(task_id), parallelism)

    import importlib
    target_task = task_impl.get('class')
    target_m = task_impl.get('module')
    target_module = importlib.__import__(target_m)
    task_class = getattr(target_module, target_task)
    task = task_class(task_id, parallelism, repetitions)

    o = task.new_object()
    task.set_object(o)

    # Synch start
    barrier.wait()
    print("START")
    start = time.time()

    # Do task
    task_time = task()
    
    print(f"Total time: {time.time() - start} s")
    print(f"Task time: {task_time} s")

    return task_time


def benchmark(task_impl, parallelism, repetitions, local=False,
              server='localhost', redispass=''):
    print(f"Running experiment with {parallelism} workers for {repetitions}"
          f" repetitions.")

    redis_conf = {
        'host': server,
        'port': 6379,
        'password': redispass
    }
    r = red.RedisConn.set_up(**redis_conf)
    r.flushall()

    task_id = time.time_ns()

    task_times = []     # in seconds

    m_name = task_impl.__module__
    if m_name == '__main__':
        import sys, os
        filename = sys.modules[task_impl.__module__].__file__
        m_name = os.path.splitext(os.path.basename(filename))[0]
    task_class = {
        "class": task_impl.__name__,
        "module": m_name
    }

    def local_task():
        task_time = run_task(task_class, task_id,
                             parallelism, repetitions, redis_conf)
        task_times.append(task_time)

    def lambda_task():
        ct = CloudThread(run_task, task_class, task_id,
                         parallelism, repetitions, redis_conf)
        ct.run()
        task_time = ct.get_response()
        task_times.append(task_time)

    if local:
        task = local_task
    else:
        task = lambda_task
    threads = [threading.Thread(target=task) for _ in range(parallelism)]

    start_time = time.time()
    [t.start() for t in threads]
    [t.join() for t in threads]

    avg_time = sum(task_times) / len(task_times)
    print(f"Total benchmark time: {time.time() - start_time} s")
    print(f"Average task time: {avg_time} s")
