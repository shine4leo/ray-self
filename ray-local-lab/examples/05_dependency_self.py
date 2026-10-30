import os
import time
import ray

ray.init(num_cpus=2)

@ray.remote(num_cpus=1)
def produce():
    print("produce PID:", os.getpid())
    time.sleep(2)
    return [10, 20, 30]

@ray.remote(num_cpus=1)
def consume(data):
    print("consume PID:", os.getpid())
    print("收到数据:", data)
    return sum(data)

# 提交任务
x_ref = produce.remote()

# 不在 Driver 上 ray.get(x_ref)
y_ref = consume.remote(x_ref)

print("x_ref:", type(x_ref).__name__)
print("y_ref:", type(y_ref).__name__)

# 只获取最终结果
print("最终结果:", ray.get(y_ref))

ray.shutdown()
