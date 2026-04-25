## Participant Evaluation 使用说明

这个版本已经把入口收成更少的文件。

### 用户主要操作的文件

- `participant_evaluation_config.xlsx`
- 运行入口：`participant_evaluation_runner.py`（见下文命令）
- 说明：`README.md`

### 配置文件如何填写

推荐直接改 `participant_evaluation_config.xlsx`。

默认配置已经改成相对路径：

- `export_folder`：`./data/20260411_OnlyAI&Control`（平台原始导出放这里；下次批次可改为 `./data/20260425_OnlyAI&Control`）
- `output_project_dir`：`./eval_runs/20260411_OnlyAI&Control`（评估整包输出；与 `export_folder` 可不同；下次改为 `./eval_runs/20260425_OnlyAI&Control`）
- `product_match_file`：`./unique_2024-31-40_merged.csv`

这表示：

- 原始导出在 `participant_evaluation/data/<批次名>/`
- 评估生成的 `0_Raw_Data`、`1_Merged_Data`、`4_Final_Outputs` 等在 `participant_evaluation/eval_runs/<批次名>/`
- 若仍希望「导出与输出同一文件夹」，可将两行填同一路径；pipeline 会跳过工程顶层目录，避免自我嵌套拷贝
- 产品匹配文件仍在 `participant_evaluation` 根目录下由 `product_match_file` 指定

### 如何运行

在仓库的 **`code`** 目录下打开终端，执行（默认使用同目录下的 `participant_evaluation_config.xlsx`，一般无需加参数）：

```bash
python3 participant_evaluation/participant_evaluation_runner.py
```

指定其它配置文件时：

```bash
python3 participant_evaluation/participant_evaluation_runner.py --config participant_evaluation/participant_evaluation_config.csv
```

若终端当前目录已是 **`participant_evaluation`**，也可以：

```bash
python3 participant_evaluation_runner.py
```

Windows 若未配置 `python3`，可把上述命令里的 `python3` 换成 `python` 或 `py`。

### 主要输出位置

默认整包结果在 `participant_evaluation/eval_runs/<批次文件夹>/` 下（由 `output_project_dir` 指定）。最方便直接查看的文件在：

- `4_Final_Outputs/run_summary.txt`
- `4_Final_Outputs/user_cumulative_performance_summary.csv`
- `4_Final_Outputs/user_round_performance_score.csv`
- `4_Final_Outputs/2_feedback_detail.csv`

### 当前内部结构

这次重构后，核心代码主要集中在：

- `participant_evaluation_runner.py`
- `participant_evaluation_pipeline.py`

其中：

- `participant_evaluation_runner.py` 负责读取配置、串联流程、输出摘要
- `participant_evaluation_pipeline.py` 已经合并了原来的 config / utils / scoring 辅助逻辑

为了让顶层目录更干净，筛选逻辑和底层 scoring 统计已经收进 `internal/` 子目录：

- `internal/filter_engine.py`
- `internal/scoring_common.py`
- `internal/scoring_measure.py`
