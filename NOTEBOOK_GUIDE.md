# 使用 Jupyter 展示项目

## 首次安装

在仓库根目录运行：

```bash
bash scripts/setup_notebook.sh
```

## 启动 Notebook

```bash
bash scripts/start_notebook.sh
```

打开 `notebooks/Project_Demo.ipynb`，并选择 **Cassandra Lab (project)** 内核。
建议用 Shift+Enter 逐个运行单元格。`Run All` 只展示历史结果和操作按钮；只有点击相应按钮时，才会启动容器或运行实验。

## 推荐演示顺序

1. 查看历史结果，熟悉四种 client-centric consistency 模型与实验参数。
2. 需要现场实验时启动 Docker Desktop，然后在 Notebook 中初始化环境。
3. 默认先用较少的迭代次数演示 normal 场景下的 `ONE/ONE` 与 `QUORUM/QUORUM`。
4. 节点故障和网络分区实验严格按照 Notebook 中的顺序执行，并在结束后恢复集群。
5. 刷新结果。Notebook 发起的新实验保存在 `results/notebook_runs/<唯一 ID>/`。

Notebook 通过独立子进程调用原有实验代码。`notebooks/run_demo.py` 只负责隔离输出目录并生成唯一运行标识，不改变一致性判定逻辑，也不会覆盖历史 `results/*.csv`。

Notebook 对零有效样本显示 `N/A`，图表按独立运行绘制，不混合不同配置。原实验程序的已知局限也在 Notebook 中注明。

`requirements-notebook.txt` 在原项目依赖上增加 JupyterLab、项目内核、表格展示和按钮组件。安装完成后，环境信息可记录到 `results/notebook-environment.txt`。
