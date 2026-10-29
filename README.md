# ray-self

这个项目记录自己学习ray 这个分布式项目的过程 

三大趋势
第一个趋势：AI demands growing faster than single node capabilities -- 一台机器越来越难承担大型 AI 任务。

第二个趋势：Clusters becoming heterogeneous: GPUs + CPUs

<img width="494" height="213" alt="image" src="https://github.com/user-attachments/assets/8e246e24-671a-4278-86b8-1be3194f1bcf" />

上层：Libraries. 包括数据预处理、训练、调优、批量预测、服务等不同应用领域。

下层：Unified compute framework for distributed application  也就是 Ray 统一的分布式计算基础框架。

[将通用的分布式计算能力与具体业务功能分开。] -->Ray Core 提供基础分布式计算抽象，上层库利用这些抽象实现各自的功能。

在 Ray 生态中，对应的上层库包括 Ray Data、Ray Train、Ray Tune、Ray Serve 等。 Ray 可以提供统一的进程编排、资源调度和任务执行能力，而深度学习框架继续负责模型计算等专业工作。

## Minimalist API — 最小化编程接口

<img width="518" height="268" alt="image" src="https://github.com/user-attachments/assets/af3ab5b8-9f77-4d38-bf6b-69eb4c01f77d" />


ray.init()	      初始化 Ray Context	        建立与 Ray 运行时的连接
@ray.remote	      标记远程函数或类	            声明 Task 或 Actor
.remote()	        发起异步远程操作	            提交任务或创建 Actor
ray.put()	        把对象存入对象存储	           返回对象引用
ray.get()	        根据对象引用取回结果	        必要时阻塞等待
ray.wait()	      区分已就绪和未就绪的引用	    不必等待所有任务完成


