#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
============================================================
DV Performance Score — 共享：汇总、检验与导出（非 user-round 测量层）
============================================================

功能：
- 轮次×组别汇总、累计用户得分、ranking 分布
- 组间检验（Welch / Mann–Whitney / Levene、ranking 卡方）
- 输出 CSV / Excel / Markdown；同文件内含 matplotlib 出图（`create_visualizations`）

User-round 粒度表及 ranking 等变量定义见：
`user_round_performance_score_user_round_measure.py`

作者：Elaine
日期：2026-04-18
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List
import sys

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy import stats


# ============================================================================
# 路径配置
# ============================================================================

SCRIPT_DIR = Path(__file__).resolve().parent
CODE_ROOT = SCRIPT_DIR.parent.parent

if str(CODE_ROOT) not in sys.path:
    sys.path.append(str(CODE_ROOT))

from shared.group_comparison_config import (  # noqa: E402
    FIGURE_DPI,
    GROUP_COLORS,
    GROUP_ORDER,
    p_value_to_star_label,
    p_value_to_yes_no,
)
from shared.project_config import PROJECT_ROOT  # noqa: E402

from internal.scoring_measure import (  # noqa: E402
    RANKING_LABELS,
    configure_runtime as configure_measure_runtime,
    build_user_round_performance_table,
)

INPUT_FILE = PROJECT_ROOT / "3_DP_feedback" / "2_feedback_detail.csv"
OUTPUT_ROOT = PROJECT_ROOT / "3_DP_feedback" / "dv_performance_score"
DERIVED_DIR = OUTPUT_ROOT / "derived"
ANALYSIS_DIR = OUTPUT_ROOT / "analysis"
FIGURES_DIR = OUTPUT_ROOT / "figures"

USER_ROUND_OUTPUT = DERIVED_DIR / "user_round_performance_score.csv"
ROUND_GROUP_SUMMARY_OUTPUT = ANALYSIS_DIR / "round_group_score_summary.csv"
ROUND_GROUP_TEST_OUTPUT = ANALYSIS_DIR / "round_group_significance_tests.csv"
RANKING_DISTRIBUTION_OUTPUT = ANALYSIS_DIR / "round_group_ranking_distribution.csv"
RANKING_DISTRIBUTION_TEST_OUTPUT = ANALYSIS_DIR / "round_ranking_distribution_significance_tests.csv"
CUMULATIVE_USER_OUTPUT = DERIVED_DIR / "user_cumulative_performance_summary.csv"
CUMULATIVE_GROUP_SUMMARY_OUTPUT = ANALYSIS_DIR / "cumulative_group_score_summary.csv"
CUMULATIVE_GROUP_TEST_OUTPUT = ANALYSIS_DIR / "cumulative_group_significance_tests.csv"
WORKBOOK_OUTPUT = ANALYSIS_DIR / "dv_performance_score_tables.xlsx"
REPORT_OUTPUT = ANALYSIS_DIR / "DV_PERFORMANCE_SCORE_REPORT.md"


def configure_runtime(
    project_root: Path | None = None,
    input_file: Path | None = None,
    output_root: Path | None = None,
) -> None:
    """覆盖默认输入输出路径，便于统一入口调度。"""
    global INPUT_FILE
    global OUTPUT_ROOT
    global DERIVED_DIR
    global ANALYSIS_DIR
    global FIGURES_DIR
    global USER_ROUND_OUTPUT
    global ROUND_GROUP_SUMMARY_OUTPUT
    global ROUND_GROUP_TEST_OUTPUT
    global RANKING_DISTRIBUTION_OUTPUT
    global RANKING_DISTRIBUTION_TEST_OUTPUT
    global CUMULATIVE_USER_OUTPUT
    global CUMULATIVE_GROUP_SUMMARY_OUTPUT
    global CUMULATIVE_GROUP_TEST_OUTPUT
    global WORKBOOK_OUTPUT
    global REPORT_OUTPUT

    resolved_project_root = Path(project_root).resolve() if project_root is not None else None
    if input_file is not None:
        INPUT_FILE = Path(input_file).resolve()
    elif resolved_project_root is not None:
        INPUT_FILE = resolved_project_root / "3_DP_feedback" / "2_feedback_detail.csv"

    if output_root is not None:
        OUTPUT_ROOT = Path(output_root).resolve()
    elif resolved_project_root is not None:
        OUTPUT_ROOT = resolved_project_root / "3_DP_feedback" / "dv_performance_score"

    DERIVED_DIR = OUTPUT_ROOT / "derived"
    ANALYSIS_DIR = OUTPUT_ROOT / "analysis"
    FIGURES_DIR = OUTPUT_ROOT / "figures"

    USER_ROUND_OUTPUT = DERIVED_DIR / "user_round_performance_score.csv"
    ROUND_GROUP_SUMMARY_OUTPUT = ANALYSIS_DIR / "round_group_score_summary.csv"
    ROUND_GROUP_TEST_OUTPUT = ANALYSIS_DIR / "round_group_significance_tests.csv"
    RANKING_DISTRIBUTION_OUTPUT = ANALYSIS_DIR / "round_group_ranking_distribution.csv"
    RANKING_DISTRIBUTION_TEST_OUTPUT = ANALYSIS_DIR / "round_ranking_distribution_significance_tests.csv"
    CUMULATIVE_USER_OUTPUT = DERIVED_DIR / "user_cumulative_performance_summary.csv"
    CUMULATIVE_GROUP_SUMMARY_OUTPUT = ANALYSIS_DIR / "cumulative_group_score_summary.csv"
    CUMULATIVE_GROUP_TEST_OUTPUT = ANALYSIS_DIR / "cumulative_group_significance_tests.csv"
    WORKBOOK_OUTPUT = ANALYSIS_DIR / "dv_performance_score_tables.xlsx"
    REPORT_OUTPUT = ANALYSIS_DIR / "DV_PERFORMANCE_SCORE_REPORT.md"
    configure_measure_runtime(project_root=resolved_project_root, input_file=INPUT_FILE)


# ============================================================================
# 基础函数
# ============================================================================

def ensure_output_dirs() -> None:
    """创建输出目录。"""
    for folder in [DERIVED_DIR, ANALYSIS_DIR, FIGURES_DIR]:
        folder.mkdir(parents=True, exist_ok=True)


def build_round_group_summary(user_round_df: pd.DataFrame) -> pd.DataFrame:
    """统计每一轮、每一组的均值和方差。"""
    summary_df = (
        user_round_df.groupby(["round", "Group"], dropna=False)
        .agg(
            n_users=("username", "nunique"),
            mean_score=("round_score", "mean"),
            variance_score=("round_score", "var"),
            std_score=("round_score", "std"),
            median_score=("round_score", "median"),
            min_score=("round_score", "min"),
            max_score=("round_score", "max"),
        )
        .reset_index()
        .sort_values(["round", "Group"])
        .reset_index(drop=True)
    )
    return summary_df


def build_cumulative_user_scores(user_round_df: pd.DataFrame) -> pd.DataFrame:
    """计算用户跨轮累计得分。"""
    cumulative_df = (
        user_round_df.groupby(["username", "Group", "LetterGroup"], dropna=False)
        .agg(
            rounds_observed=("round", "nunique"),
            cumulative_score=("round_score", "sum"),
            average_round_score=("round_score", "mean"),
        )
        .reset_index()
        .sort_values(["Group", "username"])
        .reset_index(drop=True)
    )
    return cumulative_df


def build_cumulative_group_summary(cumulative_df: pd.DataFrame) -> pd.DataFrame:
    """统计累计得分的组别均值和方差。"""
    summary_df = (
        cumulative_df.groupby("Group", dropna=False)
        .agg(
            n_users=("username", "nunique"),
            mean_cumulative_score=("cumulative_score", "mean"),
            variance_cumulative_score=("cumulative_score", "var"),
            std_cumulative_score=("cumulative_score", "std"),
            median_cumulative_score=("cumulative_score", "median"),
            min_cumulative_score=("cumulative_score", "min"),
            max_cumulative_score=("cumulative_score", "max"),
        )
        .reset_index()
        .sort_values("Group")
        .reset_index(drop=True)
    )
    return summary_df


def build_ranking_distribution(user_round_df: pd.DataFrame) -> pd.DataFrame:
    """统计每轮每组 ranking 分布。"""
    dist_df = (
        user_round_df.groupby(["round", "Group", "ranking", "ranking_label"], dropna=False)
        .size()
        .reset_index(name="user_count")
        .sort_values(["round", "Group", "ranking"])
        .reset_index(drop=True)
    )
    return dist_df


def _cohens_d(control_values: np.ndarray, onlyai_values: np.ndarray) -> float:
    """计算 Cohen's d。"""
    control_std = control_values.std(ddof=1)
    onlyai_std = onlyai_values.std(ddof=1)
    denom_df = len(control_values) + len(onlyai_values) - 2
    if denom_df <= 0:
        return np.nan

    pooled_std = np.sqrt(
        (
            (len(control_values) - 1) * control_std ** 2
            + (len(onlyai_values) - 1) * onlyai_std ** 2
        )
        / denom_df
    )
    if pooled_std == 0 or np.isnan(pooled_std):
        return np.nan
    return (onlyai_values.mean() - control_values.mean()) / pooled_std


def run_group_difference_tests(
    df: pd.DataFrame,
    scope_col: str,
    value_col: str,
) -> pd.DataFrame:
    """对 Control vs OnlyAI 进行显著性检验。"""
    test_rows: List[Dict[str, float]] = []

    for scope_value in sorted(df[scope_col].dropna().unique().tolist()):
        subset = df[df[scope_col] == scope_value].copy()
        control_values = subset.loc[subset["Group"] == "Control", value_col].dropna().to_numpy()
        onlyai_values = subset.loc[subset["Group"] == "OnlyAI", value_col].dropna().to_numpy()

        result_row: Dict[str, float] = {
            scope_col: scope_value,
            "control_n": len(control_values),
            "onlyai_n": len(onlyai_values),
            "control_mean": np.nan if len(control_values) == 0 else float(control_values.mean()),
            "onlyai_mean": np.nan if len(onlyai_values) == 0 else float(onlyai_values.mean()),
            "control_variance": np.nan if len(control_values) <= 1 else float(np.var(control_values, ddof=1)),
            "onlyai_variance": np.nan if len(onlyai_values) <= 1 else float(np.var(onlyai_values, ddof=1)),
            "mean_diff_onlyai_minus_control": np.nan,
            "welch_t_stat": np.nan,
            "welch_t_p_value": np.nan,
            "mannwhitney_u_stat": np.nan,
            "mannwhitney_u_p_value": np.nan,
            "levene_stat": np.nan,
            "levene_p_value": np.nan,
            "cohens_d": np.nan,
        }

        if len(control_values) > 0 and len(onlyai_values) > 0:
            result_row["mean_diff_onlyai_minus_control"] = (
                result_row["onlyai_mean"] - result_row["control_mean"]
            )

        if len(control_values) >= 2 and len(onlyai_values) >= 2:
            welch_stat, welch_p = stats.ttest_ind(
                control_values,
                onlyai_values,
                equal_var=False,
                nan_policy="omit",
            )
            mw_stat, mw_p = stats.mannwhitneyu(
                control_values,
                onlyai_values,
                alternative="two-sided",
            )
            levene_stat, levene_p = stats.levene(
                control_values,
                onlyai_values,
                center="median",
            )

            result_row["welch_t_stat"] = float(welch_stat)
            result_row["welch_t_p_value"] = float(welch_p)
            result_row["mannwhitney_u_stat"] = float(mw_stat)
            result_row["mannwhitney_u_p_value"] = float(mw_p)
            result_row["levene_stat"] = float(levene_stat)
            result_row["levene_p_value"] = float(levene_p)
            result_row["cohens_d"] = float(_cohens_d(control_values, onlyai_values))

        result_row["significant_mean_welch_0_05"] = p_value_to_yes_no(result_row["welch_t_p_value"])
        result_row["significant_distribution_mw_0_05"] = p_value_to_yes_no(
            result_row["mannwhitney_u_p_value"]
        )
        result_row["significant_variance_levene_0_05"] = p_value_to_yes_no(result_row["levene_p_value"])
        result_row["sig_label_mean_welch"] = p_value_to_star_label(result_row["welch_t_p_value"])
        result_row["sig_label_distribution_mw"] = p_value_to_star_label(result_row["mannwhitney_u_p_value"])
        result_row["sig_label_variance_levene"] = p_value_to_star_label(result_row["levene_p_value"])
        test_rows.append(result_row)

    return pd.DataFrame(test_rows)


def build_ranking_distribution_tests(ranking_dist_df: pd.DataFrame) -> pd.DataFrame:
    """按 round 检验两组 ranking 分布是否显著不同。"""
    test_rows: List[Dict[str, float]] = []

    for round_value in sorted(ranking_dist_df["round"].dropna().unique().tolist()):
        round_df = ranking_dist_df[ranking_dist_df["round"] == round_value].copy()
        pivot_df = (
            round_df.pivot(index="Group", columns="ranking_label", values="user_count")
            .reindex(index=GROUP_ORDER, columns=RANKING_LABELS)
            .fillna(0)
        )

        row: Dict[str, float] = {
            "round": round_value,
            "chi2_stat": np.nan,
            "chi2_p_value": np.nan,
            "degrees_of_freedom": np.nan,
            "significant_ranking_chi2_0_05": "NA",
            "sig_label_ranking_chi2": "NA",
        }

        if not pivot_df.empty and (pivot_df.sum(axis=1) > 0).all():
            chi2_stat, chi2_p_value, dof, _ = stats.chi2_contingency(pivot_df.values)
            row["chi2_stat"] = float(chi2_stat)
            row["chi2_p_value"] = float(chi2_p_value)
            row["degrees_of_freedom"] = float(dof)
            row["significant_ranking_chi2_0_05"] = p_value_to_yes_no(chi2_p_value)
            row["sig_label_ranking_chi2"] = p_value_to_star_label(chi2_p_value)

        test_rows.append(row)

    return pd.DataFrame(test_rows)


# ============================================================================
# 输出函数
# ============================================================================

def write_excel_workbook(dataframes: Dict[str, pd.DataFrame], output_file: Path) -> None:
    """将多个表写入一个 Excel 工作簿。"""
    with pd.ExcelWriter(output_file) as writer:
        for sheet_name, df in dataframes.items():
            df.to_excel(writer, sheet_name=sheet_name[:31], index=False)


def _format_p_value(value: float) -> str:
    """格式化 p 值。"""
    if pd.isna(value):
        return "NA"
    return f"{value:.4f}"


def write_markdown_report(
    raw_row_count: int,
    user_round_df: pd.DataFrame,
    round_summary_df: pd.DataFrame,
    round_tests_df: pd.DataFrame,
    cumulative_df: pd.DataFrame,
    cumulative_summary_df: pd.DataFrame,
    cumulative_tests_df: pd.DataFrame,
) -> None:
    """输出简明 Markdown 报告。"""
    input_rel = INPUT_FILE.relative_to(PROJECT_ROOT.parent)
    output_rel = OUTPUT_ROOT.relative_to(PROJECT_ROOT.parent)

    round_lines = []
    for _, row in round_tests_df.iterrows():
        round_lines.append(
            "- Round {round_value}: Control mean={control_mean:.3f}, OnlyAI mean={onlyai_mean:.3f}, "
            "Welch p={welch_p}, MW p={mw_p}, Levene p={levene_p}".format(
                round_value=int(row["round"]),
                control_mean=row["control_mean"],
                onlyai_mean=row["onlyai_mean"],
                welch_p=_format_p_value(row["welch_t_p_value"]),
                mw_p=_format_p_value(row["mannwhitney_u_p_value"]),
                levene_p=_format_p_value(row["levene_p_value"]),
            )
        )

    cumulative_row = cumulative_tests_df.iloc[0] if not cumulative_tests_df.empty else None
    cumulative_line = (
        "Cumulative: Control mean={control_mean:.3f}, OnlyAI mean={onlyai_mean:.3f}, "
        "Welch p={welch_p}, MW p={mw_p}, Levene p={levene_p}".format(
            control_mean=cumulative_row["control_mean"],
            onlyai_mean=cumulative_row["onlyai_mean"],
            welch_p=_format_p_value(cumulative_row["welch_t_p_value"]),
            mw_p=_format_p_value(cumulative_row["mannwhitney_u_p_value"]),
            levene_p=_format_p_value(cumulative_row["levene_p_value"]),
        )
        if cumulative_row is not None
        else "Cumulative: NA"
    )

    figures_cmd = "participant_evaluation_runner.py --config participant_evaluation_config.xlsx"

    report_text = f"""# DV Performance Score Report

## 1. Current Stage
- Stage 2 descriptive statistics for `1_DP_feedback`.

## 2. Input and Scope
- Input file: `{input_rel}`
- Output root: `{output_rel}`
- Raw rows: {raw_row_count} (source is feedback-item level; final table is user-round level)
- User-round observations: {len(user_round_df)}
- Unique users: {user_round_df['username'].nunique()}
- Rounds covered: {", ".join(map(str, sorted(user_round_df['round'].unique().tolist())))}

## 3. Ranking Rule
- Current script creates a reusable numeric `ranking` column from `0` to `8`, where `0` is the best segment.
- Because the original request listed 9 codes (`0-8`) but the textual percentile bands were not fully consistent, this script stores:
  - exact `percentile_from_top`
  - numeric `ranking`
  - text `ranking_label`
- Current label set is: {", ".join(RANKING_LABELS)}
- `cumulative_score_to_round`: per user, running sum of `round_score` from the earliest observed round through the current round (inclusive).
- `rank_cumulative_score_within_round`: within each round, rank all users by `cumulative_score_to_round` (1 = highest cumulative score in that round; ties broken by `username`).

## 4. Round-Level Group Summary
- Summary file: `{ROUND_GROUP_SUMMARY_OUTPUT.relative_to(PROJECT_ROOT.parent)}`
- Significance file: `{ROUND_GROUP_TEST_OUTPUT.relative_to(PROJECT_ROOT.parent)}`
- Per-round results:
{chr(10).join(round_lines)}

## 5. Cumulative User Summary
- Per user-round cumulative score and rank-by-cumulative-score are in the last columns of `{USER_ROUND_OUTPUT.relative_to(PROJECT_ROOT.parent)}` (`cumulative_score_to_round`, `rank_cumulative_score_within_round`).
- Separate user-level cumulative CSV: `{CUMULATIVE_USER_OUTPUT.relative_to(PROJECT_ROOT.parent)}`
- Group summary file: `{CUMULATIVE_GROUP_SUMMARY_OUTPUT.relative_to(PROJECT_ROOT.parent)}`
- Group test file: `{CUMULATIVE_GROUP_TEST_OUTPUT.relative_to(PROJECT_ROOT.parent)}`
- {cumulative_line}

## 6. Visualization Outputs
- Run `{figures_cmd}` after this tables step to generate PNGs under `figures/`.
- Expected paths:
- `{(FIGURES_DIR / 'round_mean_score_by_group.png').relative_to(PROJECT_ROOT.parent)}`
- `{(FIGURES_DIR / 'round_variance_score_by_group.png').relative_to(PROJECT_ROOT.parent)}`
- `{(FIGURES_DIR / 'round_score_boxplot_by_group.png').relative_to(PROJECT_ROOT.parent)}`
- `{(FIGURES_DIR / 'round_ranking_heatmap_by_group.png').relative_to(PROJECT_ROOT.parent)}`
- `{(FIGURES_DIR / 'cumulative_score_boxplot_by_group.png').relative_to(PROJECT_ROOT.parent)}`
- `{(FIGURES_DIR / 'cumulative_mean_variance_by_group.png').relative_to(PROJECT_ROOT.parent)}`
"""

    REPORT_OUTPUT.write_text(report_text, encoding="utf-8")


def load_saved_tables_for_figures() -> Dict[str, pd.DataFrame]:
    """从磁盘加载已保存的表格，供独立绘图使用。请先运行主流程表格步骤。"""
    required = [
        USER_ROUND_OUTPUT,
        ROUND_GROUP_SUMMARY_OUTPUT,
        ROUND_GROUP_TEST_OUTPUT,
        RANKING_DISTRIBUTION_OUTPUT,
        RANKING_DISTRIBUTION_TEST_OUTPUT,
        CUMULATIVE_USER_OUTPUT,
        CUMULATIVE_GROUP_SUMMARY_OUTPUT,
        CUMULATIVE_GROUP_TEST_OUTPUT,
    ]
    missing = [p for p in required if not p.exists()]
    if missing:
        raise FileNotFoundError(
            "缺少表格输出，请先运行 `participant_evaluation_runner.py --config participant_evaluation_config.xlsx`:\n"
            + "\n".join(str(p) for p in missing)
        )

    user_round_df = pd.read_csv(USER_ROUND_OUTPUT, low_memory=False)
    user_round_df["username"] = user_round_df["username"].astype(str)
    user_round_df["round"] = pd.to_numeric(user_round_df["round"], errors="coerce").astype(int)
    if "ranking" in user_round_df.columns:
        user_round_df["ranking"] = pd.to_numeric(user_round_df["ranking"], errors="coerce").astype(int)
    for col in [
        "round_score",
        "round_score_raw",
        "cumulative_score_to_round",
        "rank_cumulative_score_within_round",
    ]:
        if col in user_round_df.columns:
            user_round_df[col] = pd.to_numeric(user_round_df[col], errors="coerce")

    round_summary_df = pd.read_csv(ROUND_GROUP_SUMMARY_OUTPUT, low_memory=False)
    round_summary_df["round"] = pd.to_numeric(round_summary_df["round"], errors="coerce").astype(int)

    round_tests_df = pd.read_csv(ROUND_GROUP_TEST_OUTPUT, low_memory=False)
    round_tests_df["round"] = pd.to_numeric(round_tests_df["round"], errors="coerce").astype(int)

    ranking_dist_df = pd.read_csv(RANKING_DISTRIBUTION_OUTPUT, low_memory=False)
    ranking_dist_df["round"] = pd.to_numeric(ranking_dist_df["round"], errors="coerce").astype(int)
    if "ranking" in ranking_dist_df.columns:
        ranking_dist_df["ranking"] = pd.to_numeric(ranking_dist_df["ranking"], errors="coerce").astype(int)

    ranking_tests_df = pd.read_csv(RANKING_DISTRIBUTION_TEST_OUTPUT, low_memory=False)
    ranking_tests_df["round"] = pd.to_numeric(ranking_tests_df["round"], errors="coerce").astype(int)

    cumulative_df = pd.read_csv(CUMULATIVE_USER_OUTPUT, low_memory=False)

    cumulative_summary_df = pd.read_csv(CUMULATIVE_GROUP_SUMMARY_OUTPUT, low_memory=False)
    cumulative_tests_df = pd.read_csv(CUMULATIVE_GROUP_TEST_OUTPUT, low_memory=False)

    return {
        "user_round_df": user_round_df,
        "round_summary_df": round_summary_df,
        "round_tests_df": round_tests_df,
        "ranking_dist_df": ranking_dist_df,
        "ranking_tests_df": ranking_tests_df,
        "cumulative_df": cumulative_df,
        "cumulative_summary_df": cumulative_summary_df,
        "cumulative_tests_df": cumulative_tests_df,
    }


def run_performance_score_tables(
    project_root: Path | None = None,
    input_file: Path | None = None,
    output_root: Path | None = None,
) -> None:
    """计算指标并写入 CSV / Excel / Markdown（不绘图）。"""
    configure_runtime(project_root=project_root, input_file=input_file, output_root=output_root)
    print("📊 DV Performance Score — Tables")
    print(f"📂 Input: {INPUT_FILE}")
    print(f"📁 Output root: {OUTPUT_ROOT}")

    ensure_output_dirs()

    user_round_df, feedback_detail_row_count = build_user_round_performance_table(INPUT_FILE)
    round_summary_df = build_round_group_summary(user_round_df)
    round_tests_df = run_group_difference_tests(
        df=user_round_df,
        scope_col="round",
        value_col="round_score",
    )
    ranking_dist_df = build_ranking_distribution(user_round_df)
    ranking_tests_df = build_ranking_distribution_tests(ranking_dist_df)

    cumulative_df = build_cumulative_user_scores(user_round_df)
    cumulative_summary_df = build_cumulative_group_summary(cumulative_df)
    cumulative_test_df = cumulative_df.assign(scope="all_rounds")
    cumulative_tests_df = run_group_difference_tests(
        df=cumulative_test_df,
        scope_col="scope",
        value_col="cumulative_score",
    )

    user_round_df.to_csv(USER_ROUND_OUTPUT, index=False, encoding="utf-8-sig")
    round_summary_df.to_csv(ROUND_GROUP_SUMMARY_OUTPUT, index=False, encoding="utf-8-sig")
    round_tests_df.to_csv(ROUND_GROUP_TEST_OUTPUT, index=False, encoding="utf-8-sig")
    ranking_dist_df.to_csv(RANKING_DISTRIBUTION_OUTPUT, index=False, encoding="utf-8-sig")
    ranking_tests_df.to_csv(RANKING_DISTRIBUTION_TEST_OUTPUT, index=False, encoding="utf-8-sig")
    cumulative_df.to_csv(CUMULATIVE_USER_OUTPUT, index=False, encoding="utf-8-sig")
    cumulative_summary_df.to_csv(CUMULATIVE_GROUP_SUMMARY_OUTPUT, index=False, encoding="utf-8-sig")
    cumulative_tests_df.to_csv(CUMULATIVE_GROUP_TEST_OUTPUT, index=False, encoding="utf-8-sig")

    workbook_frames = {
        "user_round_scores": user_round_df,
        "round_group_summary": round_summary_df,
        "round_tests": round_tests_df,
        "ranking_distribution": ranking_dist_df,
        "ranking_tests": ranking_tests_df,
        "cumulative_user_summary": cumulative_df,
        "cumulative_group_summary": cumulative_summary_df,
        "cumulative_tests": cumulative_tests_df,
    }
    write_excel_workbook(workbook_frames, WORKBOOK_OUTPUT)

    write_markdown_report(
        raw_row_count=feedback_detail_row_count,
        user_round_df=user_round_df,
        round_summary_df=round_summary_df,
        round_tests_df=round_tests_df,
        cumulative_df=cumulative_df,
        cumulative_summary_df=cumulative_summary_df,
        cumulative_tests_df=cumulative_tests_df,
    )

    print("✅ Tables completed.")
    print(f"   • User-round table: {USER_ROUND_OUTPUT}")
    print(f"   • Round summary: {ROUND_GROUP_SUMMARY_OUTPUT}")
    print(f"   • Round tests: {ROUND_GROUP_TEST_OUTPUT}")
    print(f"   • Ranking distribution tests: {RANKING_DISTRIBUTION_TEST_OUTPUT}")
    print(f"   • Cumulative user summary: {CUMULATIVE_USER_OUTPUT}")
    print(f"   • Cumulative summary: {CUMULATIVE_GROUP_SUMMARY_OUTPUT}")
    print(f"   • Workbook: {WORKBOOK_OUTPUT}")
    print(f"   • Report: {REPORT_OUTPUT}")


# ============================================================================
# 可视化
# ============================================================================


def _build_scope_label_map(test_df: pd.DataFrame, scope_col: str, label_col: str) -> Dict[float, str]:
    """构建 scope 到显著性标签的映射。"""
    label_map: Dict[float, str] = {}
    for _, row in test_df.iterrows():
        scope_value = row.get(scope_col)
        if pd.isna(scope_value):
            continue
        label = str(row.get(label_col, "NA")).strip() or "NA"
        label_map[scope_value] = label
    return label_map


def _add_pair_annotation(
    ax: plt.Axes,
    left_x: float,
    right_x: float,
    left_top: float,
    right_top: float,
    label: str,
) -> None:
    """在两组图形上方添加显著性括号。"""
    values = [v for v in [left_top, right_top] if not pd.isna(v)]
    if not values:
        return

    y_max = max(values)
    y_min = min(values)
    y_range = y_max - y_min
    line_pad = max(y_range * 0.08, 0.10)
    text_pad = max(y_range * 0.03, 0.05)
    top_extension = line_pad * 2.8

    y_line = y_max + line_pad
    ax.plot(
        [left_x, left_x, right_x, right_x],
        [y_line - text_pad, y_line, y_line, y_line - text_pad],
        color="black",
        linewidth=1.1,
    )
    ax.text(
        (left_x + right_x) / 2,
        y_line + text_pad,
        label,
        ha="center",
        va="bottom",
        fontsize=10,
        fontweight="bold",
    )
    ax.set_ylim(top=ax.get_ylim()[1] + top_extension)


def _add_summary_box(ax: plt.Axes, lines: List[str]) -> None:
    """在图右上角添加显著性摘要框。"""
    summary_text = "\n".join(lines)
    ax.text(
        0.98,
        0.98,
        summary_text,
        transform=ax.transAxes,
        ha="right",
        va="top",
        fontsize=9,
        bbox={
            "boxstyle": "round,pad=0.3",
            "facecolor": "white",
            "alpha": 0.82,
            "edgecolor": "gray",
        },
    )


def plot_round_metric(
    summary_df: pd.DataFrame,
    round_tests_df: pd.DataFrame,
    value_col: str,
    ylabel: str,
    title: str,
    output_file: Path,
    sig_label_col: str,
    summary_label: str,
) -> None:
    """绘制按轮次分组的折线图。"""
    fig, ax = plt.subplots(figsize=(10, 6))
    rounds = sorted(summary_df["round"].unique().tolist())
    series_map: Dict[str, List[float]] = {}

    for group_name in GROUP_ORDER:
        group_df = summary_df[summary_df["Group"] == group_name].sort_values("round")
        if group_df.empty:
            continue
        series_map[group_name] = [
            group_df.loc[group_df["round"] == round_value, value_col].iloc[0]
            if round_value in group_df["round"].values
            else np.nan
            for round_value in rounds
        ]
        ax.plot(
            group_df["round"],
            group_df[value_col],
            marker="o",
            linewidth=2.2,
            label=group_name,
            color=GROUP_COLORS[group_name],
        )

    ax.set_title(title, fontsize=14, fontweight="bold")
    ax.set_xlabel("Round")
    ax.set_ylabel(ylabel)
    ax.grid(axis="y", alpha=0.3)
    ax.legend()
    label_map = _build_scope_label_map(round_tests_df, "round", sig_label_col)
    for round_value, control_value, onlyai_value in zip(
        rounds,
        series_map.get("Control", []),
        series_map.get("OnlyAI", []),
    ):
        _add_pair_annotation(
            ax=ax,
            left_x=float(round_value) - 0.12,
            right_x=float(round_value) + 0.12,
            left_top=control_value,
            right_top=onlyai_value,
            label=label_map.get(round_value, "NA"),
        )
    _add_summary_box(
        ax,
        [f"{summary_label}: {round_value}={label_map.get(round_value, 'NA')}" for round_value in rounds],
    )
    plt.tight_layout()
    plt.savefig(output_file, dpi=FIGURE_DPI, bbox_inches="tight")
    plt.close(fig)


def plot_round_score_boxplot(
    user_round_df: pd.DataFrame,
    round_tests_df: pd.DataFrame,
    output_file: Path,
) -> None:
    """绘制每轮两组得分分布箱线图。"""
    rounds = sorted(user_round_df["round"].unique().tolist())
    fig, ax = plt.subplots(figsize=(12, 6))

    positions: List[float] = []
    box_values: List[Iterable[float]] = []
    box_colors: List[str] = []

    for round_value in rounds:
        base_position = round_value * 2
        for offset, group_name in zip([-0.35, 0.35], GROUP_ORDER):
            values = user_round_df.loc[
                (user_round_df["round"] == round_value) & (user_round_df["Group"] == group_name),
                "round_score",
            ].dropna()
            positions.append(base_position + offset)
            box_values.append(values.to_numpy())
            box_colors.append(GROUP_COLORS[group_name])

    boxplot = ax.boxplot(
        box_values,
        positions=positions,
        widths=0.55,
        patch_artist=True,
        medianprops={"color": "black", "linewidth": 1.2},
    )
    for patch, color in zip(boxplot["boxes"], box_colors):
        patch.set_facecolor(color)
        patch.set_alpha(0.75)

    tick_positions = [round_value * 2 for round_value in rounds]
    ax.set_xticks(tick_positions)
    ax.set_xticklabels(rounds)
    ax.set_title("User-round score distribution by round and group", fontsize=14, fontweight="bold")
    ax.set_xlabel("Round")
    ax.set_ylabel("Round score")
    ax.grid(axis="y", alpha=0.3)
    ax.legend(
        handles=[
            plt.Rectangle((0, 0), 1, 1, color=GROUP_COLORS[group_name], alpha=0.75)
            for group_name in GROUP_ORDER
        ],
        labels=GROUP_ORDER,
        title="Group",
        loc="upper left",
    )
    label_map = _build_scope_label_map(round_tests_df, "round", "sig_label_distribution_mw")
    for round_value in rounds:
        control_values = user_round_df.loc[
            (user_round_df["round"] == round_value) & (user_round_df["Group"] == "Control"),
            "round_score",
        ].dropna()
        onlyai_values = user_round_df.loc[
            (user_round_df["round"] == round_value) & (user_round_df["Group"] == "OnlyAI"),
            "round_score",
        ].dropna()
        _add_pair_annotation(
            ax=ax,
            left_x=round_value * 2 - 0.35,
            right_x=round_value * 2 + 0.35,
            left_top=control_values.max() if not control_values.empty else np.nan,
            right_top=onlyai_values.max() if not onlyai_values.empty else np.nan,
            label=label_map.get(round_value, "NA"),
        )
    _add_summary_box(
        ax,
        [f"MW: {round_value}={label_map.get(round_value, 'NA')}" for round_value in rounds],
    )

    plt.tight_layout()
    plt.savefig(output_file, dpi=FIGURE_DPI, bbox_inches="tight")
    plt.close(fig)


def plot_ranking_heatmap(
    ranking_dist_df: pd.DataFrame,
    ranking_tests_df: pd.DataFrame,
    output_file: Path,
) -> None:
    """绘制 ranking 分布热力图。"""
    fig, axes = plt.subplots(1, 2, figsize=(16, 5), sharey=True)
    vmax = ranking_dist_df["user_count"].max() if not ranking_dist_df.empty else 1

    for axis, group_name in zip(axes, GROUP_ORDER):
        group_df = ranking_dist_df[ranking_dist_df["Group"] == group_name].copy()
        pivot_df = (
            group_df.pivot(index="round", columns="ranking_label", values="user_count")
            .reindex(columns=RANKING_LABELS)
            .fillna(0)
        )

        heatmap = axis.imshow(pivot_df.values, aspect="auto", cmap="Blues", vmin=0, vmax=vmax)
        axis.set_title(group_name)
        axis.set_xticks(range(len(pivot_df.columns)))
        axis.set_xticklabels(pivot_df.columns, rotation=45, ha="right")
        axis.set_yticks(range(len(pivot_df.index)))
        axis.set_yticklabels(pivot_df.index.tolist())
        axis.set_xlabel("Ranking label")
        if group_name == GROUP_ORDER[0]:
            axis.set_ylabel("Round")

    fig.suptitle("Ranking distribution by round and group", fontsize=14, fontweight="bold")
    fig.colorbar(heatmap, ax=axes, shrink=0.9, label="User count")
    ranking_label_map = _build_scope_label_map(ranking_tests_df, "round", "sig_label_ranking_chi2")
    fig.text(
        0.985,
        0.95,
        "\n".join(
            [
                f"Chi-square: {round_value}={ranking_label_map.get(round_value, 'NA')}"
                for round_value in sorted(ranking_label_map)
            ]
        ),
        ha="right",
        va="top",
        fontsize=9,
        bbox={
            "boxstyle": "round,pad=0.3",
            "facecolor": "white",
            "alpha": 0.82,
            "edgecolor": "gray",
        },
    )
    fig.subplots_adjust(top=0.84, bottom=0.22, wspace=0.15)
    plt.savefig(output_file, dpi=FIGURE_DPI, bbox_inches="tight")
    plt.close(fig)


def plot_cumulative_boxplot(
    cumulative_df: pd.DataFrame,
    cumulative_tests_df: pd.DataFrame,
    output_file: Path,
) -> None:
    """绘制累计得分箱线图。"""
    fig, ax = plt.subplots(figsize=(8, 6))
    values = [
        cumulative_df.loc[cumulative_df["Group"] == group_name, "cumulative_score"].dropna().to_numpy()
        for group_name in GROUP_ORDER
    ]

    boxplot = ax.boxplot(
        values,
        tick_labels=GROUP_ORDER,
        patch_artist=True,
        medianprops={"color": "black", "linewidth": 1.2},
    )
    for patch, group_name in zip(boxplot["boxes"], GROUP_ORDER):
        patch.set_facecolor(GROUP_COLORS[group_name])
        patch.set_alpha(0.75)

    ax.set_title("Cumulative score distribution by group", fontsize=14, fontweight="bold")
    ax.set_ylabel("Cumulative score")
    ax.grid(axis="y", alpha=0.3)
    sig_label = (
        cumulative_tests_df["sig_label_distribution_mw"].iloc[0]
        if not cumulative_tests_df.empty
        else "NA"
    )
    _add_pair_annotation(
        ax=ax,
        left_x=1,
        right_x=2,
        left_top=np.max(values[0]) if len(values) > 0 and len(values[0]) > 0 else np.nan,
        right_top=np.max(values[1]) if len(values) > 1 and len(values[1]) > 0 else np.nan,
        label=sig_label,
    )
    _add_summary_box(ax, [f"MW distribution: {sig_label}"])
    plt.tight_layout()
    plt.savefig(output_file, dpi=FIGURE_DPI, bbox_inches="tight")
    plt.close(fig)


def plot_cumulative_summary(
    cumulative_summary_df: pd.DataFrame,
    cumulative_tests_df: pd.DataFrame,
    output_file: Path,
) -> None:
    """绘制累计得分均值和方差柱状图。"""
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    sig_map = {
        "mean_cumulative_score": (
            cumulative_tests_df["sig_label_mean_welch"].iloc[0] if not cumulative_tests_df.empty else "NA"
        ),
        "variance_cumulative_score": (
            cumulative_tests_df["sig_label_variance_levene"].iloc[0] if not cumulative_tests_df.empty else "NA"
        ),
    }

    for axis, value_col, title in zip(
        axes,
        ["mean_cumulative_score", "variance_cumulative_score"],
        ["Mean cumulative score", "Variance of cumulative score"],
    ):
        plot_df = cumulative_summary_df.copy()
        x = np.arange(len(plot_df))
        axis.bar(
            x,
            plot_df[value_col],
            color=[GROUP_COLORS[group_name] for group_name in plot_df["Group"]],
            alpha=0.8,
        )
        axis.set_xticks(x)
        axis.set_xticklabels(plot_df["Group"])
        axis.set_title(title, fontsize=12, fontweight="bold")
        axis.grid(axis="y", alpha=0.3)
        _add_pair_annotation(
            ax=axis,
            left_x=0,
            right_x=1,
            left_top=plot_df.loc[plot_df["Group"] == "Control", value_col].iloc[0]
            if "Control" in plot_df["Group"].values
            else np.nan,
            right_top=plot_df.loc[plot_df["Group"] == "OnlyAI", value_col].iloc[0]
            if "OnlyAI" in plot_df["Group"].values
            else np.nan,
            label=sig_map.get(value_col, "NA"),
        )
        summary_name = "Welch mean" if value_col == "mean_cumulative_score" else "Levene variance"
        _add_summary_box(axis, [f"{summary_name}: {sig_map.get(value_col, 'NA')}"])

    plt.tight_layout()
    plt.savefig(output_file, dpi=FIGURE_DPI, bbox_inches="tight")
    plt.close(fig)


def create_visualizations(
    user_round_df: pd.DataFrame,
    round_summary_df: pd.DataFrame,
    round_tests_df: pd.DataFrame,
    ranking_dist_df: pd.DataFrame,
    ranking_tests_df: pd.DataFrame,
    cumulative_df: pd.DataFrame,
    cumulative_summary_df: pd.DataFrame,
    cumulative_tests_df: pd.DataFrame,
) -> None:
    """生成全部图表。"""
    ensure_output_dirs()
    plot_round_metric(
        round_summary_df,
        round_tests_df=round_tests_df,
        value_col="mean_score",
        ylabel="Mean score",
        title="Round mean score by group (Welch significance annotated)",
        output_file=FIGURES_DIR / "round_mean_score_by_group.png",
        sig_label_col="sig_label_mean_welch",
        summary_label="Welch",
    )
    plot_round_metric(
        round_summary_df,
        round_tests_df=round_tests_df,
        value_col="variance_score",
        ylabel="Variance",
        title="Round variance by group (Levene significance annotated)",
        output_file=FIGURES_DIR / "round_variance_score_by_group.png",
        sig_label_col="sig_label_variance_levene",
        summary_label="Levene",
    )
    plot_round_score_boxplot(
        user_round_df=user_round_df,
        round_tests_df=round_tests_df,
        output_file=FIGURES_DIR / "round_score_boxplot_by_group.png",
    )
    plot_ranking_heatmap(
        ranking_dist_df=ranking_dist_df,
        ranking_tests_df=ranking_tests_df,
        output_file=FIGURES_DIR / "round_ranking_heatmap_by_group.png",
    )
    plot_cumulative_boxplot(
        cumulative_df=cumulative_df,
        cumulative_tests_df=cumulative_tests_df,
        output_file=FIGURES_DIR / "cumulative_score_boxplot_by_group.png",
    )
    plot_cumulative_summary(
        cumulative_summary_df=cumulative_summary_df,
        cumulative_tests_df=cumulative_tests_df,
        output_file=FIGURES_DIR / "cumulative_mean_variance_by_group.png",
    )
