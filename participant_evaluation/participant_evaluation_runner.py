#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
============================================================
Participant Evaluation Runner
============================================================

功能：
- 读取配置（默认：与本脚本同目录的 participant_evaluation_config.xlsx）
- 串联 prepare / merge / filter / expand / match / score 全流程
- 输出最终结果摘要

运行示例（在仓库 code 目录下）：
  python3 participant_evaluation/participant_evaluation_runner.py
  python3 participant_evaluation/participant_evaluation_runner.py --config participant_evaluation/participant_evaluation_config.csv

作者：Elaine
日期：2026-04-21
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
CODE_DIR = SCRIPT_DIR.parent

if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))
if str(CODE_DIR) not in sys.path:
    sys.path.insert(0, str(CODE_DIR))

from participant_evaluation_pipeline import (
    copy_key_output_files,
    export_final_selected_performance,
    load_config,
    prepare_project_input_data,
    run_expand_stage,
    run_filter_stage,
    run_match_stage,
    run_merge_stage,
    run_scoring,
    write_run_summary,
)


DEFAULT_CONFIG_FILE = SCRIPT_DIR / "participant_evaluation_config.xlsx"


def run_pipeline(config_path: Path) -> dict[str, Path]:
    """执行完整评估流程。"""
    config = load_config(config_path)
    prepared = prepare_project_input_data(config)
    project_root = prepared["project_root"]
    product_file = prepared["product_file"]
    final_output_dir = prepared["final_output_dir"]

    print("=" * 70)
    print("Participant Evaluation")
    print("=" * 70)
    print(f"配置文件: {config.config_path}")
    print(f"导出目录: {config.export_folder}")
    print(f"输出目录: {project_root}")
    print(f"产品匹配文件: {product_file}")
    print("=" * 70)

    run_merge_stage(project_root=project_root, combined_dirname=config.combined_dirname)
    run_filter_stage(config=config, project_root=project_root)
    run_expand_stage(project_root=project_root)
    run_match_stage(project_root=project_root, product_file=product_file)
    run_scoring(project_root=project_root, run_figures=config.run_figures)
    final_selected_outputs = export_final_selected_performance(project_root=project_root)

    copied_outputs = copy_key_output_files(project_root=project_root, final_output_dir=final_output_dir)
    copied_outputs.update(final_selected_outputs)
    run_summary_path = final_output_dir / "run_summary.txt"
    summary_lines = [
        "Participant Evaluation 运行完成",
        "=" * 60,
        f"配置文件: {config.config_path}",
        f"导出目录: {config.export_folder}",
        f"项目输出目录: {project_root}",
        f"产品匹配文件: {product_file}",
        f"启用规则: {sorted(config.enabled_rules)}",
        f"连续3轮同品是否剔除: {'是' if config.exclude_feedback_consecutive_3_same else '否'}",
        f"pre rounds: {sorted(config.pre_rounds)}",
        f"是否生成图: {'是' if config.run_figures else '否'}",
        f"规则5 credemo 是否检查 post 问卷: {'是' if config.credemo_check_post else '否（仅 pre）'}",
        "",
        "关键输出：",
    ]
    for label, path in copied_outputs.items():
        summary_lines.append(f"- {label}: {path}")
    write_run_summary(run_summary_path, summary_lines)
    copied_outputs["run_summary"] = run_summary_path
    return copied_outputs


def main() -> None:
    """命令行入口。"""
    parser = argparse.ArgumentParser(
        description="Run participant evaluation.",
        epilog=f"默认配置文件: {DEFAULT_CONFIG_FILE.name}（与本脚本同目录）。",
    )
    parser.add_argument(
        "--config",
        type=str,
        default=str(DEFAULT_CONFIG_FILE),
        help="Excel 或 CSV 配置文件路径；省略时使用同目录下的 participant_evaluation_config.xlsx。",
    )
    args = parser.parse_args()
    outputs = run_pipeline(Path(args.config).expanduser().resolve())
    print("=" * 70)
    print("最终输出:")
    for label, path in outputs.items():
        print(f"  - {label}: {path}")
    print("=" * 70)


if __name__ == "__main__":
    main()
