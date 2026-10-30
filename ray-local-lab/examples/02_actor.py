"""第二课：Actor 是持久化状态的远程 Python 对象。"""
import os
import ray

ray.init()

@ray.remote(num_cpus=1)
class Counter:
    def __init__(self, name):
        self.name = name
        self.value = 0
        self.pid = os.getpid()

    def increment(self):
        self.value += 1
        return (self.name, self.value, self.pid)

counter = Counter.remote("counter-A")
print("Actor 句柄类型:", type(counter).__name__)
refs = [counter.increment.remote() for _ in range(3)]
print("连续调用:", ray.get(refs))
print("同一个 Actor 内状态被保留，进程 PID 相同")
ray.shutdown()
