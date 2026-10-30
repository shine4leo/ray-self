"""第三课：Placement Group 成组预留 CPU 资源（不需要 GPU）。"""
import os
import socket
import ray
from ray.util import placement_group, remove_placement_group
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

ray.init()
print("集群资源:", ray.cluster_resources())
print("需要至少 2 个可用 Ray CPU 资源")

pg = placement_group([{"CPU": 1}, {"CPU": 1}], strategy="STRICT_PACK")
workers = []
try:
    ray.get(pg.ready(), timeout=60)

    @ray.remote(num_cpus=1)
    class Inspector:
        def location(self):
            return {"host": socket.gethostname(), "pid": os.getpid()}

    for i in range(2):
        w = Inspector.options(
            scheduling_strategy=PlacementGroupSchedulingStrategy(
                placement_group=pg,
                placement_group_bundle_index=i,
            )
        ).remote()
        workers.append(w)

    locations = ray.get([w.location.remote() for w in workers])
    print("Actor 执行位置:", locations)
    print("STRICT_PACK: 两个 bundle 必须在同一物理节点")
finally:
    for w in workers:
        ray.kill(w, no_restart=True)
    remove_placement_group(pg)
    ray.shutdown()
