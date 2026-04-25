#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
============================================================
Participant Evaluation Pipeline
============================================================

功能：
- 整理导出目录为评估项目结构
- 执行 merge / filter / expand / match 四个阶段

作者：Elaine
日期：2026-04-21
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime
import importlib.util
import json
import re
import shutil
import sys
import tempfile
from pathlib import Path
from types import ModuleType

import pandas as pd

from shared import encoding_utils, username_mapping

try:
    import numpy as np
except Exception:  # pragma: no cover
    np = None


SCRIPT_DIR = Path(__file__).resolve().parent
CODE_DIR = SCRIPT_DIR.parent

if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))
if str(CODE_DIR) not in sys.path:
    sys.path.insert(0, str(CODE_DIR))

from internal.scoring_common import (  # noqa: E402
    configure_runtime as configure_score_runtime,
    create_visualizations,
    load_saved_tables_for_figures,
    run_performance_score_tables,
)


FINAL_OUTPUT_DIRNAME = "4_Final_Outputs"
RAW_SUBDIR = "0_Raw_Data"
MERGED_OUT_DIRNAME = "1_Merged_Data"
FILTERED_DIRNAME = "2_Filtered_Data"
FEEDBACK_DIRNAME = "3_DP_feedback"
DATA_MATCH_DIR = "0_Data_for_match"
INPUT_FILENAME = "users_feedback.csv"
EXPANDED_FILENAME = "1_feedback_expanded.csv"
DETAIL_FILENAME = "2_feedback_detail.csv"
MERGE_REPORT_FILENAME = "0_merge_summary.txt"
MANUAL_REVIEW_FILENAME = "manual_review_queue.csv"

EXCLUDE_RELATIVE_PATHS = {
    "clickhouse/article_visits.csv",
    "clickhouse/chat_like.csv",
    "clickhouse/chat_messages.csv",
    "clickhouse/chat_read_status.csv",
    "clickhouse/chat_sessions.csv",
    "clickhouse/filter_use.csv",
    "clickhouse/product_compare.csv",
    "clickhouse/product_publish.csv",
    "clickhouse/product_visits.csv",
    "mysql/users_config.csv",
    "mysql/users_questionnaire.csv",
    "mysql/users_shopitem.csv",
    "mysql/users_topproductdata.csv",
    "mysql/users_category.csv",
    "mysql/users_marketdata.csv",
    "mysql/users_instruction.csv",
    "mysql/users_product.csv",
    "mysql/users_article.csv",
    "mysql/users_categorylevelrelationship.csv",
    "mysql/users_productdata.csv",
    "mysql/users.csv",
}
SCORE_RULES = {
    "rule3": {
        "产生亏损": -8,
        "爆品": 20,
        "销量较高": 8,
        "销量一般": 1,
        "销量较差": -3,
    },
}
PRODUCT_FILENAME_PATTERNS = (
    "unique_*_merged.csv",
    "*merged*.csv",
    "*.csv",
)
AIVL_ROOT = CODE_DIR.parent.parent
OPENAI_EMBED_DIR = (
    AIVL_ROOT
    / "1_Prod_Pool"
    / "Prod_Pool_tk_thai"
    / "0_shared_data"
    / "5_embeddings_openai"
)
OPENAI_EMBED_INDEX_FILE = "product_ids_embed_order_openai.csv"
OPENAI_EMBED_META_FILE = "embedding_run_meta_openai.json"
OPENAI_EMBED_NPY_FILE = "product_embeddings_openai_3large.npy"
OPENAI_EMBED_ATTACH_VECTOR = False
DEFAULT_RULES = frozenset({1, 2, 3, 4, 5, 6})
DEFAULT_PRE_ROUNDS = frozenset({1, 2})

# export 与 output 为同一目录时，不得把这些顶层目录再拷进 Combined（否则会自我嵌套）
_PIPELINE_ROOT_DIR_NAMES = frozenset(
    {RAW_SUBDIR, MERGED_OUT_DIRNAME, FILTERED_DIRNAME, FEEDBACK_DIRNAME, FINAL_OUTPUT_DIRNAME, DATA_MATCH_DIR}
)
_EXPORT_TOP_MARKERS = ("mysql", "clickhouse", "operation", "credemo", "credamo")


@dataclass(frozen=True)
class ParticipantEvaluationConfig:
    """Participant evaluation 配置。"""

    config_path: Path
    export_folder: Path
    output_project_dir: Path
    product_match_file: Path
    combined_dirname: str
    enabled_rules: frozenset[int]
    exclude_feedback_consecutive_3_same: bool
    pre_rounds: frozenset[int]
    run_figures: bool
    credemo_check_post: bool


def resolve_relative_path(base_dir: Path, raw_path: str) -> Path:
    """将配置中的相对路径解析为绝对路径。"""
    candidate = Path(str(raw_path).strip()).expanduser()
    if candidate.is_absolute():
        return candidate.resolve()
    return (base_dir / candidate).resolve()


def replace_directory(target_dir: Path) -> None:
    """删除旧目录后重新创建。"""
    if target_dir.exists():
        shutil.rmtree(target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)


def copy_directory_contents(source_dir: Path, target_dir: Path) -> None:
    """复制目录下所有文件和子目录。"""
    target_dir.mkdir(parents=True, exist_ok=True)
    for child in source_dir.iterdir():
        destination = target_dir / child.name
        if child.is_dir():
            if destination.exists():
                shutil.rmtree(destination)
            shutil.copytree(child, destination)
        else:
            shutil.copy2(child, destination)


def _has_export_markers(base: Path) -> bool:
    return any((base / name).exists() for name in _EXPORT_TOP_MARKERS)


def copy_export_skipping_pipeline_roots(source_dir: Path, target_dir: Path) -> None:
    """复制顶层内容到 target，跳过工程根下已生成的目录名。"""
    target_dir.mkdir(parents=True, exist_ok=True)
    for child in source_dir.iterdir():
        if child.name in _PIPELINE_ROOT_DIR_NAMES:
            continue
        destination = target_dir / child.name
        if child.is_dir():
            if destination.exists():
                shutil.rmtree(destination)
            shutil.copytree(child, destination)
        else:
            shutil.copy2(child, destination)


def _deepest_combined_on_raw_chain(export_folder: Path, combined_dirname: str) -> Path:
    """沿 0_Raw_Data/<combined>/ 反复下钻到最深一层（用于修复曾被误拷进多层的导出目录）。"""
    cur = export_folder.resolve()
    for _ in range(128):
        nxt = cur / RAW_SUBDIR / combined_dirname
        if not nxt.is_dir():
            break
        cur = nxt
    return cur


def _find_shallowest_export_marker_dir(root: Path, max_depth: int = 36) -> Path | None:
    """在 root 下广度优先，找到深度最浅且含平台导出标记的目录。"""
    root = root.resolve()
    dq: deque[tuple[Path, int]] = deque([(root, 0)])
    seen: set[Path] = set()
    while dq:
        path, depth = dq.popleft()
        if path in seen or depth > max_depth:
            continue
        seen.add(path)
        try:
            if not path.is_dir():
                continue
        except OSError:
            continue
        if _has_export_markers(path):
            return path
        try:
            for child in path.iterdir():
                if child.is_dir():
                    dq.append((child, depth + 1))
        except OSError:
            continue
    return None


def _resolve_export_source_dir(export_folder: Path, combined_dirname: str) -> Path:
    """确定应复制到 Combined 的「平台导出根」：扁平根、或 0_Raw_Data 链最深处、或浅层 BFS 命中。"""
    root = export_folder.resolve()
    if _has_export_markers(root):
        return root
    deepest = _deepest_combined_on_raw_chain(root, combined_dirname)
    if _has_export_markers(deepest):
        return deepest
    found = _find_shallowest_export_marker_dir(root)
    if found is not None:
        return found
    raise FileNotFoundError(
        f"在导出目录中未找到平台数据（mysql、clickhouse、operation、credemo 等）: {root}。"
        "若该目录曾被错误嵌套拷贝，请删除其中的 0_Raw_Data 后换用干净导出，或改用新的 data 批次文件夹。"
    )


def _populate_combined_from_export(
    export_folder: Path,
    project_root: Path,
    combined_root: Path,
    combined_dirname: str,
) -> None:
    """将导出写入 combined；export 与 output 同目录时避免嵌套拷贝，并支持重复运行。"""
    if export_folder.resolve() != project_root.resolve():
        replace_directory(combined_root)
        source = _resolve_export_source_dir(export_folder, combined_dirname)
        copy_export_skipping_pipeline_roots(source, combined_root)
        if not any(combined_root.iterdir()):
            raise FileNotFoundError(
                f"复制导出后 Combined 为空: {combined_root}（来源: {source}）"
            )
        return

    if _has_export_markers(project_root):
        replace_directory(combined_root)
        copy_export_skipping_pipeline_roots(project_root, combined_root)
        return

    staging: Path | None = None
    if combined_root.exists() and _has_export_markers(combined_root):
        staging = Path(tempfile.mkdtemp(prefix="pe_combined_stage_"))
        copy_directory_contents(combined_root, staging)
    replace_directory(combined_root)
    if staging is not None:
        copy_directory_contents(staging, combined_root)
        shutil.rmtree(staging, ignore_errors=True)
        return

    replace_directory(combined_root)
    copy_export_skipping_pipeline_roots(project_root, combined_root)
    if not any(combined_root.iterdir()):
        raise FileNotFoundError(
            "export_folder 与 output_project_dir 为同一目录，但未找到可复制的导出内容 "
            f"（期望根目录或上一轮 {combined_root.name} 下有 mysql / clickhouse / operation 等）。"
        )


def load_module_from_file(module_path: Path, module_name: str) -> ModuleType:
    """从文件路径动态加载模块。"""
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"无法加载模块: {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def write_run_summary(summary_path: Path, lines: list[str]) -> None:
    """写出中文运行摘要。"""
    summary_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def copy_key_output_files(project_root: Path, final_output_dir: Path) -> dict[str, Path]:
    """复制最关键结果到最终交付目录。"""
    outputs = {
        "filtered_feedback": project_root / "2_Filtered_Data" / "users_feedback.csv",
        "feedback_detail": project_root / "3_DP_feedback" / "2_feedback_detail.csv",
        "user_round_score": project_root / "3_DP_feedback" / "dv_performance_score" / "derived" / "user_round_performance_score.csv",
        "user_cumulative_score": project_root / "3_DP_feedback" / "dv_performance_score" / "derived" / "user_cumulative_performance_summary.csv",
    }

    copied_paths: dict[str, Path] = {}
    for label, source_path in outputs.items():
        if not source_path.is_file():
            continue
        destination_path = final_output_dir / source_path.name
        shutil.copy2(source_path, destination_path)
        copied_paths[label] = destination_path
    return copied_paths


def _normalize_bool(value: object, default: bool) -> bool:
    """将常见文本值转为布尔值。"""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return default
    text = str(value).strip().lower()
    if not text:
        return default
    return text in {"1", "true", "yes", "y", "是"}


def _normalize_optional_text(value: object) -> str:
    """将单元格值清洗为普通字符串。"""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return ""
    return text


def _normalize_int_set(value: object, default: frozenset[int]) -> frozenset[int]:
    """将逗号分隔文本转为整数集合。"""
    text = _normalize_optional_text(value)
    if not text:
        return default
    parts = [part.strip() for part in text.split(",") if part.strip()]
    if not parts:
        return default
    return frozenset(int(part) for part in parts)


def _read_key_value_config(config_path: Path) -> dict[str, object]:
    """读取 key-value 形式的 Excel 或 CSV 配置。"""
    if config_path.suffix.lower() == ".csv":
        df = pd.read_csv(config_path)
    else:
        df = pd.read_excel(config_path)
    if not {"key", "value"}.issubset(df.columns):
        raise KeyError("配置文件缺少必要列: key, value")

    config_map: dict[str, object] = {}
    for _, row in df.iterrows():
        key = str(row.get("key", "")).strip()
        if key:
            config_map[key] = row.get("value")
    return config_map


def load_config(config_path: Path) -> ParticipantEvaluationConfig:
    """读取并校验配置。"""
    config_path = config_path.resolve()
    base_dir = config_path.parent
    raw = _read_key_value_config(config_path)

    export_folder = resolve_relative_path(base_dir, _normalize_optional_text(raw.get("export_folder")))
    output_project_dir = resolve_relative_path(base_dir, _normalize_optional_text(raw.get("output_project_dir")))
    product_match_file = resolve_relative_path(base_dir, _normalize_optional_text(raw.get("product_match_file")))

    if not export_folder.is_dir():
        raise FileNotFoundError(f"导出目录不存在: {export_folder}")
    if not product_match_file.is_file():
        raise FileNotFoundError(f"产品匹配文件不存在: {product_match_file}")

    combined_dirname = _normalize_optional_text(raw.get("combined_dirname")) or export_folder.name
    return ParticipantEvaluationConfig(
        config_path=config_path,
        export_folder=export_folder,
        output_project_dir=output_project_dir,
        product_match_file=product_match_file,
        combined_dirname=combined_dirname,
        enabled_rules=_normalize_int_set(raw.get("enabled_rules"), DEFAULT_RULES),
        exclude_feedback_consecutive_3_same=_normalize_bool(
            raw.get("exclude_feedback_consecutive_3_same"),
            default=False,
        ),
        pre_rounds=_normalize_int_set(raw.get("pre_rounds"), DEFAULT_PRE_ROUNDS),
        run_figures=_normalize_bool(raw.get("run_figures"), default=True),
        credemo_check_post=_normalize_bool(raw.get("credemo_check_post"), default=False),
    )


def prepare_project_input_data(config: ParticipantEvaluationConfig) -> dict[str, Path]:
    """将导出目录整理为旧流程可识别的项目结构。"""
    project_root = config.output_project_dir
    raw_root = project_root / RAW_SUBDIR
    combined_root = raw_root / config.combined_dirname
    match_dir = project_root / DATA_MATCH_DIR
    final_output_dir = project_root / FINAL_OUTPUT_DIRNAME

    project_root.mkdir(parents=True, exist_ok=True)
    raw_root.mkdir(parents=True, exist_ok=True)
    match_dir.mkdir(parents=True, exist_ok=True)
    final_output_dir.mkdir(parents=True, exist_ok=True)

    _populate_combined_from_export(
        config.export_folder,
        project_root,
        combined_root,
        config.combined_dirname,
    )

    for generated_dirname in (MERGED_OUT_DIRNAME, FILTERED_DIRNAME, FEEDBACK_DIRNAME, FINAL_OUTPUT_DIRNAME):
        generated_dir = project_root / generated_dirname
        if generated_dirname == FINAL_OUTPUT_DIRNAME:
            replace_directory(generated_dir)
        elif generated_dir.exists():
            shutil.rmtree(generated_dir)

    copied_product_file = match_dir / config.product_match_file.name
    shutil.copy2(config.product_match_file, copied_product_file)
    return {
        "project_root": project_root,
        "combined_root": combined_root,
        "product_file": copied_product_file,
        "final_output_dir": final_output_dir,
    }


def _relpaths_under(base: Path) -> set[str]:
    out = set()
    for file_path in base.rglob("*"):
        if file_path.is_file() and file_path.suffix.lower() == ".csv":
            out.add(str(file_path.relative_to(base)))
    return out


def _read_csv_any_encoding(path: Path, allow_skip_bad_lines: bool = False) -> pd.DataFrame:
    return encoding_utils.read_csv_any_encoding(
        path,
        allow_skip_bad_lines=allow_skip_bad_lines,
    )


def _detect_combined_group_dir(raw_root: Path) -> Path | None:
    if not raw_root.is_dir():
        return None
    candidates: list[Path] = []
    for child in raw_root.iterdir():
        if child.is_dir():
            lowered = child.name.lower()
            if "onlyai" in lowered and "control" in lowered:
                candidates.append(child)
    if len(candidates) == 1:
        return candidates[0]
    return None


def _log(records: list[str], message: str) -> None:
    print(message)
    records.append(message)


def _write_merge_report(out_dir: Path, report_lines: list[str]) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = out_dir / MERGE_REPORT_FILENAME
    with open(report_path, "w", encoding="utf-8") as file_obj:
        file_obj.write("Merge Summary\n")
        file_obj.write("=" * 60 + "\n")
        file_obj.write(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        for line in report_lines:
            file_obj.write(line + "\n")
    return report_path


def run_merge_stage(project_root: Path, combined_dirname: str) -> Path:
    """执行 merge 阶段。"""
    report_lines: list[str] = []
    raw_root = project_root / RAW_SUBDIR
    combined_dir = raw_root / combined_dirname if combined_dirname else None
    active_combined_dir = combined_dir if combined_dir and combined_dir.is_dir() else _detect_combined_group_dir(raw_root)
    if active_combined_dir is None:
        raise FileNotFoundError(f"未找到 Combined 导出目录: {raw_root}")

    out_dir = project_root / MERGED_OUT_DIRNAME
    mapping = username_mapping.load_temp_username_mapping(active_combined_dir)
    if mapping:
        mapping_path = username_mapping.resolve_temp_mapping_path(active_combined_dir)
        _log(report_lines, f"[临时账号映射] Combined: {len(mapping)} 条（{mapping_path.name if mapping_path else '?'}）")

    rel_all = _relpaths_under(active_combined_dir)
    to_copy = rel_all - EXCLUDE_RELATIVE_PATHS
    excluded_existing = sorted(rel_all & EXCLUDE_RELATIVE_PATHS)
    _log(report_lines, f"[模式] 单组输出（仅 Combined({active_combined_dir.name})）")
    _log(report_lines, f"源文件数: {len(rel_all)}，排除后待写出: {len(to_copy)}")
    if excluded_existing:
        _log(report_lines, f"未写出文件数: {len(excluded_existing)}")
        for rel_path in excluded_existing:
            _log(report_lines, f"  未写出(排除规则): {rel_path}")
    else:
        _log(report_lines, "未写出文件数: 0")

    for rel_path in sorted(to_copy):
        source_path = active_combined_dir / rel_path
        output_path = out_dir / Path(rel_path).name
        df = _read_csv_any_encoding(source_path, allow_skip_bad_lines=True)
        df = username_mapping.apply_username_mapping(df, mapping)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_path, index=False, encoding="utf-8-sig")
        _log(report_lines, f"  已写出: {rel_path} -> {output_path.name}")

    _log(report_lines, "是否有合并: 否（仅单组复制）")
    report_path = _write_merge_report(out_dir, report_lines)
    print(f"已写入: {report_path}")
    return report_path


def run_filter_stage(config: ParticipantEvaluationConfig, project_root: Path) -> None:
    """执行 filter 阶段。"""
    filter_module = load_module_from_file(
        SCRIPT_DIR / "internal" / "filter_engine.py",
        "participant_eval_filter",
    )
    filter_module.main(
        enabled_rules=config.enabled_rules,
        exclude_feedback_consecutive_3_same=config.exclude_feedback_consecutive_3_same,
        excluded_pre_rounds=config.pre_rounds,
        project_root=project_root,
        combined_dirname=config.combined_dirname,
        apply_exclusion_to_outputs=False,
        credemo_check_post_questionnaire=config.credemo_check_post,
    )


def load_feedback_input(path: Path) -> pd.DataFrame:
    """读取待展开的 feedback 数据。"""
    for encoding in ("utf-8", "utf-8-sig", "gb18030", "gbk", "latin-1", "cp1252"):
        try:
            return pd.read_csv(path, encoding=encoding)
        except UnicodeDecodeError:
            continue
    return pd.read_csv(path, encoding="latin-1")


def expand_feedback_rows(df: pd.DataFrame) -> pd.DataFrame:
    """将 feedback 文本拆为多行 feedback_item。"""
    rows = []
    for _, row in df.iterrows():
        feedback_text = str(row.get("feedback", "")).strip()
        if not feedback_text or feedback_text.lower() == "nan":
            continue
        for part in feedback_text.split("\n"):
            clean_part = part.strip()
            if clean_part and clean_part.lower() != "nan":
                new_row = row.copy()
                new_row["feedback_item"] = clean_part
                rows.append(new_row)

    expanded_df = pd.DataFrame(rows)
    if "feedback_item" in expanded_df.columns:
        expanded_df = expanded_df.dropna(subset=["feedback_item"])
        expanded_df = expanded_df[expanded_df["feedback_item"].astype(str).str.strip() != ""]
    return expanded_df


def run_expand_stage(project_root: Path) -> Path:
    """执行 expand 阶段。"""
    print("=" * 70)
    print("📊 Feedback expansion")
    print("=" * 70)
    input_file = project_root / FILTERED_DIRNAME / INPUT_FILENAME
    output_dir = project_root / FEEDBACK_DIRNAME
    output_file = output_dir / EXPANDED_FILENAME
    output_dir.mkdir(parents=True, exist_ok=True)
    if not input_file.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")

    print(f"📂 Loading: {input_file.name}")
    df = load_feedback_input(input_file)
    expanded_df = expand_feedback_rows(df)
    expanded_df.to_csv(output_file, index=False, encoding="utf-8")
    print(f"✅ Saved: {output_file.relative_to(project_root)}")
    print(f"📊 {len(expanded_df)} rows.")
    return output_file


def resolve_product_file(match_dir: Path, preferred_name: str) -> Path | None:
    """解析用于匹配的产品文件。"""
    preferred_path = match_dir / preferred_name
    if preferred_path.exists():
        return preferred_path
    candidates: list[Path] = []
    for pattern in PRODUCT_FILENAME_PATTERNS:
        for file_path in sorted(match_dir.glob(pattern)):
            if file_path.is_file() and file_path not in candidates:
                candidates.append(file_path)
    if not candidates:
        return None
    picked = candidates[0]
    print(f"ℹ️  默认产品文件不存在，自动匹配到: {picked.name}")
    if len(candidates) > 1:
        print(f"   共匹配 {len(candidates)} 个文件，当前使用排序后第一个。")
    return picked


def extract_product_name(text: str) -> str | None:
    if not isinstance(text, str):
        return None
    match = re.search(r'产品"?([^""]+)"?', text)
    return match.group(1).strip() if match else None


def extract_seller_count(text: str) -> int | None:
    if not isinstance(text, str):
        return None
    match = re.search(r"本轮有(\d+)个商家", text)
    return int(match.group(1)) if match else None


def extract_performance_keyword(text: str, all_keywords: list[str]) -> str | None:
    if not isinstance(text, str):
        return None
    for keyword in all_keywords:
        if keyword in text:
            return keyword
    return None


def load_openai_embedding_index(embed_dir: Path) -> tuple[pd.DataFrame | None, dict]:
    index_path = embed_dir / OPENAI_EMBED_INDEX_FILE
    meta_path = embed_dir / OPENAI_EMBED_META_FILE
    meta: dict = {}
    if meta_path.exists():
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except Exception:
            meta = {}
    if not index_path.exists():
        print(f"⚠️  OpenAI embedding index not found: {index_path}")
        return None, meta

    idx_df = pd.read_csv(index_path)
    if "product_id" not in idx_df.columns:
        print(f"⚠️  embedding index 缺少 product_id 列: {index_path.name}")
        return None, meta
    idx_df = idx_df.copy()
    idx_df["product_id"] = idx_df["product_id"].astype(str).str.strip()
    if "embed_text" in idx_df.columns:
        idx_df = idx_df.rename(columns={"embed_text": "openai_embed_text"})
    else:
        idx_df["openai_embed_text"] = None
    idx_df["openai_embed_index"] = idx_df.index.astype("Int64")
    keep_cols = ["product_id", "openai_embed_text", "openai_embed_index"]
    return idx_df[keep_cols].drop_duplicates(subset=["product_id"], keep="first"), meta


def attach_openai_embedding_info(df: pd.DataFrame, idx_df: pd.DataFrame | None, meta: dict) -> pd.DataFrame:
    if idx_df is None or "product_id" not in df.columns:
        return df
    out = df.copy()
    out["product_id"] = out["product_id"].astype(str).str.strip()
    out = out.merge(idx_df, on="product_id", how="left")
    model_name = meta.get("model_name")
    if model_name:
        out["openai_embedding_model"] = model_name
    return out


def attach_openai_embedding_vectors(df: pd.DataFrame, embed_dir: Path, meta: dict) -> pd.DataFrame:
    if not OPENAI_EMBED_ATTACH_VECTOR or np is None or "openai_embed_index" not in df.columns:
        return df
    npy_path = embed_dir / OPENAI_EMBED_NPY_FILE
    if not npy_path.exists():
        return df
    matrix = np.load(npy_path, mmap_mode="r")
    out = df.copy()
    idx_num = pd.to_numeric(out["openai_embed_index"], errors="coerce")
    out["openai_embedding_vector"] = None
    valid_mask = idx_num.notna() & (idx_num >= 0) & (idx_num < int(matrix.shape[0]))
    for row_index in out.index[valid_mask]:
        vector = np.asarray(matrix[int(idx_num.loc[row_index])], dtype=float)
        out.at[row_index, "openai_embedding_vector"] = json.dumps(vector.tolist(), ensure_ascii=False)
    model_name = meta.get("model_name")
    if model_name and "openai_embedding_model" not in out.columns:
        out["openai_embedding_model"] = model_name
    return out


def run_match_stage(project_root: Path, product_file: Path | None = None) -> Path:
    """执行 match 阶段。"""
    print("=" * 70)
    print("📊 Feedback match")
    print("=" * 70)
    feedback_dir = project_root / FEEDBACK_DIRNAME
    input_file = feedback_dir / EXPANDED_FILENAME
    output_file = feedback_dir / DETAIL_FILENAME
    match_dir = project_root / DATA_MATCH_DIR
    actual_product_file = product_file or resolve_product_file(match_dir, "unique_2024-31-35_merged.csv")
    if actual_product_file is None or not actual_product_file.exists():
        raise FileNotFoundError(f"Product file not found: {actual_product_file}")

    feedback_df = pd.read_csv(input_file, low_memory=False)
    if "id" in feedback_df.columns:
        feedback_df = feedback_df.drop(columns=["id"])
    feedback_df["prod_name"] = feedback_df["feedback_item"].apply(extract_product_name)
    feedback_df["本轮商家数"] = feedback_df["feedback_item"].apply(extract_seller_count)
    all_keywords = sorted(set().union(*(rule.keys() for rule in SCORE_RULES.values())), key=lambda x: -len(x))
    feedback_df["performance"] = feedback_df["feedback_item"].apply(lambda x: extract_performance_keyword(x, all_keywords))
    feedback_df["rule3_score"] = feedback_df["performance"].map(SCORE_RULES["rule3"])
    score = pd.to_numeric(feedback_df["rule3_score"], errors="coerce")
    seller_count = pd.to_numeric(feedback_df["本轮商家数"], errors="coerce")
    feedback_df["rule3_score_adjusted"] = (score / seller_count).where((score > 0) & seller_count.notna() & (seller_count > 0), score)

    product_df = pd.read_csv(actual_product_file, low_memory=False)
    dedup_col = "product_name_zh_short" if "product_name_zh_short" in product_df.columns else "product_name"
    if dedup_col in product_df.columns:
        product_df = product_df.drop_duplicates(subset=[dedup_col], keep="first").reset_index(drop=True)

    use_cols = [
        "product_id",
        "product_name",
        "product_name_zh",
        "product_name_zh_short",
        "level_1_label",
        "level_2_label",
        "level_3_label",
        "img_url",
        "product_tags",
        "real_price_cny",
        "rating_clean",
        "review_count_clean",
        "release_date",
    ]
    use_cols = [col for col in use_cols if col in product_df.columns]
    merged_df = feedback_df.merge(
        product_df[use_cols],
        left_on="prod_name",
        right_on="product_name_zh_short",
        how="left",
    )
    if "level_1_label" in merged_df.columns:
        merged_df = merged_df.rename(
            columns={
                "level_1_label": "level1_name",
                "level_2_label": "level2_name",
                "level_3_label": "level3_name",
            }
        )

    embed_idx_df, embed_meta = load_openai_embedding_index(OPENAI_EMBED_DIR)
    merged_df = attach_openai_embedding_info(merged_df, embed_idx_df, embed_meta)
    merged_df = attach_openai_embedding_vectors(merged_df, OPENAI_EMBED_DIR, embed_meta)
    merged_df.to_csv(output_file, index=False, encoding="utf-8")
    print(f"✅ Saved: {output_file.relative_to(project_root)} ({len(merged_df)} rows)")
    return output_file


def run_scoring(project_root: Path, run_figures: bool = True) -> None:
    """执行评分与可选作图。"""
    configure_score_runtime(project_root=project_root, input_file=None, output_root=None)
    run_performance_score_tables(project_root=project_root, input_file=None, output_root=None)
    if run_figures:
        create_visualizations(**load_saved_tables_for_figures())


def export_final_selected_performance(project_root: Path) -> dict[str, Path]:
    """按最终有效用户名名单导出最终版绩效结果。"""
    final_output_dir = project_root / FINAL_OUTPUT_DIRNAME
    final_output_dir.mkdir(parents=True, exist_ok=True)

    valid_user_file = project_root / FILTERED_DIRNAME / "user_lists" / "valid_usernames_final.csv"
    manual_review_file = project_root / FILTERED_DIRNAME / MANUAL_REVIEW_FILENAME
    user_round_file = (
        project_root
        / FEEDBACK_DIRNAME
        / "dv_performance_score"
        / "derived"
        / "user_round_performance_score.csv"
    )
    cumulative_file = (
        project_root
        / FEEDBACK_DIRNAME
        / "dv_performance_score"
        / "derived"
        / "user_cumulative_performance_summary.csv"
    )

    if not valid_user_file.is_file():
        raise FileNotFoundError(f"最终有效用户名名单不存在: {valid_user_file}")

    valid_df = pd.read_csv(valid_user_file, low_memory=False)
    valid_users = set(valid_df["username"].astype(str).str.strip().dropna().unique())
    manual_review_df = pd.DataFrame()
    if manual_review_file.is_file():
        manual_review_df = pd.read_csv(manual_review_file, low_memory=False)
        for _, row in manual_review_df.iterrows():
            username = str(row.get("username", "")).strip()
            decision = str(row.get("reviewer_decision", "")).strip().lower()
            if not username or not decision:
                continue
            if decision in {"保留", "keep", "include", "retain"}:
                valid_users.add(username)
            elif decision in {"剔除", "exclude", "remove", "drop"}:
                valid_users.discard(username)

    outputs: dict[str, Path] = {}
    final_valid_user_file = final_output_dir / "final_valid_usernames.csv"
    final_valid_df = pd.DataFrame({"username": sorted(valid_users)})
    final_valid_df.to_csv(final_valid_user_file, index=False, encoding="utf-8-sig")
    outputs["final_valid_usernames"] = final_valid_user_file
    if not manual_review_df.empty:
        manual_review_copy = final_output_dir / MANUAL_REVIEW_FILENAME
        manual_review_df.to_csv(manual_review_copy, index=False, encoding="utf-8-sig")
        outputs["manual_review_queue"] = manual_review_copy

    if user_round_file.is_file():
        user_round_df = pd.read_csv(user_round_file, low_memory=False)
        mask = user_round_df["username"].astype(str).str.strip().isin(valid_users)
        final_user_round_file = final_output_dir / "user_round_performance_score_final.csv"
        user_round_df.loc[mask].copy().to_csv(final_user_round_file, index=False, encoding="utf-8-sig")
        outputs["final_user_round_score"] = final_user_round_file

    if cumulative_file.is_file():
        cumulative_df = pd.read_csv(cumulative_file, low_memory=False)
        mask = cumulative_df["username"].astype(str).str.strip().isin(valid_users)
        final_cumulative_file = final_output_dir / "user_cumulative_performance_summary_final.csv"
        cumulative_df.loc[mask].copy().to_csv(final_cumulative_file, index=False, encoding="utf-8-sig")
        outputs["final_user_cumulative_score"] = final_cumulative_file

    return outputs
