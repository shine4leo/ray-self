"""第一课：Task / Future / ObjectRef。单机 CPU 即可。"""
import os
import time
import ray

ray.init()
print("集群资源:", ray.cluster_resources())

@ray.remote(num_cpus=1)
def slow_square(x):
    time.sleep(2)
    return {"x": x, "result": x * x, "worker_pid": os.getpid()}

start = time.perf_counter()
r1 = slow_square.remote(3)
r2 = slow_square.remote(4)
print("远程任务已提交，引用类型:", type(r1).__name__)
print("提交后时间: %.2fs" % (time.perf_counter() - start))
print("结果:", ray.get([r1, r2]))
print("总时间: %.2fs" % (time.perf_counter() - start))
ray.shutdown()
