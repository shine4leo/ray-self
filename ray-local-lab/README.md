# Ray 单机架构实验包

对应伯克利 Ray 课件：第 22-28 页 Task 和 Future；29-31 页 Actor；56-60 页系统架构与资源调度。适合 CPU 笔记本，不需要 GPU。

## 1. 前置环境

推荐 Python 3.12，并使用独立虚拟环境（尽量避免把 `.venv` 放在准备提交的 `examples` 文件夹内）。

Linux / macOS / WSL2:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -c 'import ray; print(ray.__version__)'
```

Windows PowerShell（如果装有 Python Launcher）：

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -c "import ray; print(ray.__version__)"
```

Windows 支持仍为 Beta。如果遇到环境问题建议使用 Windows 11 + WSL2 Ubuntu。

## 2. 第一、二课：单机自动启动 Ray

```bash
python examples/01_task_future.py
python examples/02_actor.py
```

- `01_task_future.py`: 两个 sleep 2 秒的 Task 同时提交，检查 ObjectRef 与 worker PID；至少 2 个可用逻辑 CPU 时可并发完成。
- `02_actor.py`: 验证 Actor 持久化状态：连续得到 1、2、3，且 PID 相同。

`ray.init()` 会自动启动本地 Ray 运行时，进程结束前 `ray.shutdown()` 关闭连接/本地运行时。若已有集群，`ray.init()` 可能自动连接到它。

## 3. 第三课：Placement Group

```bash
python examples/03_placement_group.py
```

该实验使用两个 CPU bundle 和 STRICT_PACK，把两个 Actor 放到同一节点。需要至少 2 CPU slot。

## 4. 第四课：Job CLI 提交（和 ray-train 项目接近）

启动一个长期存在的本地 head 节点：

```bash
ray start --head --num-cpus=2
ray status
```

浏览器打开 `http://127.0.0.1:8265` 查看 Ray Dashboard。

提交 Job，注意 `--working-dir` 上传的是 `examples/` 目录；Windows PowerShell / Linux / macOS / WSL2 都可执行：

```bash
ray job submit --address http://127.0.0.1:8265 --working-dir ./examples -- python 04_job_app.py
```

Job 完成后可在 Dashboard 的 Jobs 页面查看运行状态和日志。

结束实验：

```bash
ray stop
```

## 5. 对照原仓库

- `ray-train/submit_train.py` 对应本实验的 `ray job submit`，区别是原仓库通过 `JobSubmissionClient` SDK 提交。
- `ray-train/train/train_entry.py` 对应资源预留和创建 Actor。
- `ray-train/train/ddp_worker.py` 的 `@ray.remote(num_gpus=1, num_cpus=16)` 需要真实 GPU、PyTorch/NCCL，并非本 CPU 实验可直接执行。
- 原仓库还依赖共享 NAS 和可从节点访问的数据路径；不能在无 GPU 的笔记本上原样运行。

## 6. 常见问题

- Python 找不到 Ray：确认激活了虚拟环境，运行 `python -m pip show ray`。
- Dashboard 打不开：确认安装的是 `ray[default]`，执行 `ray status` 查看当前集群状态，并检查 8265 端口。
- 两个 Task 执行没有接近并行：检查 `ray.cluster_resources()` 至少有 2 个 CPU slot。
- `ray start --head` 提示已有实例：先 `ray stop`，再重新启动（会中断该机器现有 Ray 作业）。
- 不要把 Dashboard 绑定到公网：本地实验默认通过 `127.0.0.1` 访问。
