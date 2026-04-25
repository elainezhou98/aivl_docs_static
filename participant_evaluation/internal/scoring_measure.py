#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
============================================================
DV Performance Score — User-round 粒度得分表（测量层）
============================================================

功能：
- 基于 `3_DP_feedback/2_feedback_detail.csv` 读入并校验
- 聚合为 user-round 粒度：`round_score`、`round_score_raw`、`feedback_item_count` 等
- 在每轮内生成 `rank_within_round`、`percentile_from_top`、`ranking`、`ranking_label`、`users_in_round`
- 按用户累加 `cumulative_score_to_round`；再在**每一轮内**按累计分对全体用户重排得到 `rank_cumulative_score_within_round`（1=该轮累计分最高）；上述两列置于表末

输出为**一张** user-round 表（不返回 feedback item 明细表）。组间检验与多表导出见 `user_round_performance_score_common.py`，统一入口为 `participant_evaluation_runner.py`。

作者：Elaine
日期：2026-04-21
"""

from __future__ import annotations

from pathlib import Path
from typing import List, Tuple
import sys

import numpy as np
import pandas as pd


SCRIPT_DIR = Path(__file__).resolve().parent
CODE_ROOT = SCRIPT_DIR.parent.parent

if str(CODE_ROOT) not in sys.path:
    sys.path.append(str(CODE_ROOT))

from shared.project_config import PROJECT_ROOT  # noqa: E402


# 默认输入（调用方可传入其他 Path）
FEEDBACK_DETAIL_CSV = PROJECT_ROOT / "3_DP_feedback" / "2_feedback_detail.csv"


def configure_runtime(
    project_root: Path | None = None,
    input_file: Path | None = None,
) -> None:
    """覆盖默认输入目录，便于统一入口调度。"""
    global FEEDBACK_DETAIL_CSV

    if input_file is not None:
        FEEDBACK_DETAIL_CSV = Path(input_file).resolve()
        return

    if project_root is not None:
        project_root = Path(project_root).resolve()
        FEEDBACK_DETAIL_CSV = project_root / "3_DP_feedback" / "2_feedback_detail.csv"

RANKING_LABELS = [
    "top10%",
    "10%-20%",
    "20%-30%",
    "30%-40%",
    "40%-50%",
    "50%-60%",
    "60%-70%",
    "70%-80%",
    "80%-100%",
]
# 使用 0-1 的百分位边界，边界数必须比标签数多 1。
RANKING_BIN_EDGES = [0.0, 0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 1.0]


def load_feedback_detail(input_file: Path) -> pd.DataFrame:
    """读取并校验 feedback-detail 行级数据。"""
    if not input_file.exists():
        raise FileNotFoundError(f"输入文件不存在: {input_file}")

    df = pd.read_csv(input_file, low_memory=False)
    required_cols = [
        "username",
        "round",
        "Group",
        "rule3_score",
        "rule3_score_adjusted",
    ]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise KeyError(f"输入数据缺少必要列: {missing_cols}")

    df = df.copy()
    df["username"] = df["username"].astype(str)
    df["round"] = pd.to_numeric(df["round"], errors="coerce")
    df["rule3_score"] = pd.to_numeric(df["rule3_score"], errors="coerce")
    df["rule3_score_adjusted"] = pd.to_numeric(df["rule3_score_adjusted"], errors="coerce")
    df = df[df["round"].notna() & df["Group"].notna()].copy()
    df["round"] = df["round"].astype(int)
    return df


def aggregate_user_round_scores(df: pd.DataFrame) -> pd.DataFrame:
    """将 feedback item 粒度聚合为 user-round 粒度得分表。"""
    grouped = (
        df.groupby(["username", "round", "Group", "LetterGroup"], dropna=False)
        .agg(
            feedback_item_count=("feedback_item", "count"),
            round_score_raw=("rule3_score", "sum"),
            round_score=("rule3_score_adjusted", "sum"),
        )
        .reset_index()
        .sort_values(["round", "Group", "username"])
        .reset_index(drop=True)
    )
    return grouped


def assign_round_rankings(user_round_df: pd.DataFrame) -> pd.DataFrame:
    """在每一轮内按得分从高到低生成 ranking 相关列。"""
    ranked_parts: List[pd.DataFrame] = []

    if len(RANKING_BIN_EDGES) != len(RANKING_LABELS) + 1:
        raise ValueError(
            "RANKING_BIN_EDGES must have exactly one more value than RANKING_LABELS. "
            f"Got {len(RANKING_BIN_EDGES)} edges and {len(RANKING_LABELS)} labels."
        )

    for round_value, round_df in user_round_df.groupby("round", sort=True):
        sub_df = round_df.sort_values(
            ["round_score", "username"],
            ascending=[False, True],
        ).reset_index(drop=True)

        total_users = len(sub_df)
        sub_df["rank_within_round"] = np.arange(1, total_users + 1)
        sub_df["percentile_from_top"] = sub_df["rank_within_round"] / total_users

        ranking_codes = pd.cut(
            sub_df["percentile_from_top"],
            bins=RANKING_BIN_EDGES,
            labels=list(range(len(RANKING_LABELS))),
            include_lowest=True,
            right=True,
        )
        sub_df["ranking"] = ranking_codes.astype(int)
        sub_df["ranking_label"] = sub_df["ranking"].map(dict(enumerate(RANKING_LABELS)))
        sub_df["users_in_round"] = total_users
        ranked_parts.append(sub_df)

        print(
            f"✅ Round {round_value}: {total_users} users ranked, "
            f"score range {sub_df['round_score'].min():.3f} to {sub_df['round_score'].max():.3f}"
        )

    ranked_df = pd.concat(ranked_parts, ignore_index=True)
    return ranked_df.sort_values(["round", "ranking", "username"]).reset_index(drop=True)


def add_cumulative_score_by_round(user_round_df: pd.DataFrame) -> pd.DataFrame:
    """
    每个用户按 round 升序对 `round_score` 累加，写入 `cumulative_score_to_round`（从最早出现轮次到当前轮之和）。
    """
    df = user_round_df.copy()
    df = df.sort_values(["username", "round"], kind="mergesort")
    df["cumulative_score_to_round"] = df.groupby("username", sort=False)["round_score"].cumsum()
    df = df.sort_values(["round", "ranking", "username"]).reset_index(drop=True)
    return df


def assign_rank_by_cumulative_score_within_round(user_round_df: pd.DataFrame) -> pd.DataFrame:
    """
    每一轮内，按 `cumulative_score_to_round` 从高到低重排名次（同分按 `username` 升序），
    写入 `rank_cumulative_score_within_round`（1 表示该轮全体中累计分最高）。
    """
    ranked_parts: List[pd.DataFrame] = []
    for _round_value, round_df in user_round_df.groupby("round", sort=True):
        sub_df = round_df.sort_values(
            ["cumulative_score_to_round", "username"],
            ascending=[False, True],
        ).reset_index(drop=True)
        n = len(sub_df)
        sub_df["rank_cumulative_score_within_round"] = np.arange(1, n + 1)
        ranked_parts.append(sub_df)

    out = pd.concat(ranked_parts, ignore_index=True)
    return out.sort_values(["round", "ranking", "username"]).reset_index(drop=True)


# 表末：累计分，再为按累计分在当前轮内的名次
_TRAILING_CUMULATIVE_COLS = ("cumulative_score_to_round", "rank_cumulative_score_within_round")


def _move_trailing_cumulative_columns(df: pd.DataFrame) -> pd.DataFrame:
    """将累计分与按累计分排名列置于表末（保持固定顺序）。"""
    present = [c for c in _TRAILING_CUMULATIVE_COLS if c in df.columns]
    if not present:
        return df
    others = [c for c in df.columns if c not in present]
    return df[others + present]


def build_user_round_performance_table(input_file: Path | None = None) -> Tuple[pd.DataFrame, int]:
    """
    从 feedback_detail 生成完整 user-round 表（含 ranking 与表末累计分及按累计分重排名次）。

    Returns
    -------
    user_round_df : pd.DataFrame
        user-round 粒度主表；末尾列为 `cumulative_score_to_round`、`rank_cumulative_score_within_round`。
    feedback_detail_row_count : int
        输入 feedback item 行数（用于报告中的原始行数）。
    """
    actual_input = Path(input_file).resolve() if input_file is not None else FEEDBACK_DETAIL_CSV
    detail_df = load_feedback_detail(actual_input)
    feedback_detail_row_count = len(detail_df)
    user_round_df = aggregate_user_round_scores(detail_df)
    user_round_df = assign_round_rankings(user_round_df)
    user_round_df = add_cumulative_score_by_round(user_round_df)
    user_round_df = assign_rank_by_cumulative_score_within_round(user_round_df)
    user_round_df = _move_trailing_cumulative_columns(user_round_df)
    return user_round_df, feedback_detail_row_count
