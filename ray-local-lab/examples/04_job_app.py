"""第四课：通过 Ray Jobs CLI 提交本文件。"""
import os
import socket
import ray

ray.init()

@ray.remote(num_cpus=1)
def identify():
    return {"hostname": socket.gethostname(), "worker_pid": os.getpid()}

print("Job Driver PID:", os.getpid())
print("远程 Worker:", ray.get(identify.remote()))
print("Ray Job 已成功执行")
ray.shutdown()
