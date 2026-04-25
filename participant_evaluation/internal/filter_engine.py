"""
对 1_Merged_Data 进行筛选与打标：统一资格逻辑（合并脚本只做映射与合并）。

规则（并集剔除 + 预警，写入 filter_summary）：
  1) operation/实验不合格样本记录.csv 中的 username（先经临时账号映射）
  2) users_feedback 中任意一条 feedback 为空
  3) user_operation_log：决策时间（轮次范围与有效轮数阈值由参数控制；仅预警，不剔除）
  4) questionnaire_results.csv：每用户每轮至少有一条非空有效 responses（轮次范围由参数控制）
  5) credemo：学号匹配检查（预警，不剔除）。默认仅检查 pre 问卷；配置 credemo_check_post=TRUE 时同时检查 post
  6) users_shop：店铺名/描述仍为默认（剔除）。仅检查在 users_feedback 中有记录、且已通过规则1+2（不含 feedback 不合格、不含「完全无 feedback 行」）的用户；与规则3/4/5 的「规则1+2 后样本」口径一致。

  规则2 附加（不占用规则编号）：从 feedback 提取「产品…」后，若同一用户存在连续 3 个 round（如 1,2,3）
  的产品名集合完全一致，写入 filter_summary 预警。是否据此额外剔除用户由
  EXCLUDE_ON_FEEDBACK_CONSECUTIVE_3_SAME_PRODUCT 或命令行 --exclude-feedback-consecutive-3-same 控制。

启用方式：
  - 改下方 DEFAULT_ENABLED_RULE_IDS（默认全开 {1,2,3,4,5}）
  - 或命令行：python 2_filter_and_label.py --rules 1,2,5
  - 连续3轮同品额外剔除：--exclude-feedback-consecutive-3-same

输出：2_Filtered_Data、Group/LetterGroup、filter_summary、质量报告、user_lists（含 valid_usernames_final.csv）。
Group：username 首字符 a/A→OnlyAI，b/B→Control；非 a/b 时回退 raw Control 臂 users_feedback 名单。
"""
from __future__ import annotations

import argparse
import ast
import importlib.util
import json
import re
from datetime import datetime
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# 项目根
# ---------------------------------------------------------------------------
_SCRIPT_DIR = Path(__file__).resolve().parent
_SHARED = _SCRIPT_DIR.parent.parent / "shared"
_CFG_PATH = _SHARED / "project_config.py"
_cfg_spec = importlib.util.spec_from_file_location("project_config", _CFG_PATH)
project_config = importlib.util.module_from_spec(_cfg_spec)
_cfg_spec.loader.exec_module(project_config)
PROJECT_ROOT = project_config.PROJECT_ROOT

_MAP_PATH = _SHARED / "username_mapping.py"
_map_spec = importlib.util.spec_from_file_location("username_mapping", _MAP_PATH)
username_mapping = importlib.util.module_from_spec(_map_spec)
_map_spec.loader.exec_module(username_mapping)

_ENC_PATH = _SHARED / "encoding_utils.py"
_enc_spec = importlib.util.spec_from_file_location("encoding_utils", _ENC_PATH)
encoding_utils = importlib.util.module_from_spec(_enc_spec)
_enc_spec.loader.exec_module(encoding_utils)

# ---------------------------------------------------------------------------
# 目录与文件名
# ---------------------------------------------------------------------------
INPUT_MERGED_DIR = "1_Merged_Data"
OUTPUT_FILTERED_DIR = "2_Filtered_Data"
RAW_SUBDIR = "0_Raw_Data"
CONTROL_DIRNAME = project_config.CONTROL_DIRNAME
ONLYAI_DIRNAME = project_config.ONLYAI_DIRNAME
COMBINED_DIRNAME = getattr(project_config, "COMBINED_DIRNAME", "")
USER_FEEDBACK_FILENAME = "users_feedback.csv"
RAW_MYSQL_SUBDIR = "mysql"
SUMMARY_FILENAME = "filter_summary.txt"
QUALITY_REPORT_FILENAME = "0_check_quality_empty_feedback_users.csv"
PARTICIPANT_INFO_XLSX = "电商运营实验被试信息收集表3.26.xlsx"
PARTICIPANT_MARKED_CSV = "电商运营实验被试信息收集表3.26_规则标注.csv"
SKIP_OUTPUT_FILENAMES = {
    "临时账号使用.csv",
    "实验不合格样本记录.csv",
}

OPERATION_SUBDIR = "operation"
DISQUALIFIED_CSV = "实验不合格样本记录.csv"
CREDEMO_SUBDIR = "credemo"
QUESTIONNAIRE_FILENAME = "questionnaire_results.csv"
USER_OPERATION_LOG = "user_operation_log.csv"

# credemo 导出表中学号列（pre/post 文案略有不同）
CREDEMO_COL_PRE = "请填写你的学号（用于数据匹配）"
CREDEMO_COL_POST = "请填写您的学号（用于数据匹配）"

# decision time（与 1_decision_time / 用户约定一致）
ROUND_MIN, ROUND_MAX = 1, 10
# pre 轮次（默认不参与规则2/3/4中的 round 判定）
DEFAULT_PRE_ROUNDS_EXCLUDED: frozenset[int] = frozenset({1, 2})
MIN_DECISION_MINUTES_PER_ROUND = 3.0
MIN_DECISION_AVG_MINUTES = 5.0
REQUIRED_DECISION_ROUNDS = 5
# 规则3（decision time）是否排除最后一轮（通常为 summary/final 阶段）
DECISION_TIME_EXCLUDE_LAST_ROUND = True
# 规则4（questionnaire）是否排除最后一轮（按 total round - 1 检查）
QUESTIONNAIRE_EXCLUDE_LAST_ROUND = True

# 筛选规则编号（按执行逻辑）：1=外部名单 2=feedback 空 3=decision time 4=问卷 5=credemo(预警) 6=店铺信息未修改
RULE_EXTERNAL = 1
RULE_FEEDBACK_EMPTY = 2
# 规则2 说明（用于控制台与报告）
RULE2_FEEDBACK_RULE_TEXT = (
    "users_feedback 中任意一行 feedback 为缺失（NaN）或 strip 后为空白字符串，则将该用户整体剔除（该用户在本表及其它表中的全部行均删除）"
)
# 与 code/1_DP_feedback/0_preDP_feedback/2_feedback_match.py 中 extract_product_name 一致：从每行提取「产品」后的名称（非整段 feedback 比对）
_FEEDBACK_PRODUCT_RE = re.compile(r'产品"?([^""\n]+)"?')
_TEMP_USERNAME_RE = re.compile(r"^[A-Za-z]\d{3}$")
RULE2_POSITIVE_PERFORMANCE_KEYWORDS: tuple[str, ...] = ("销量一般", "销量较高", "爆品", "高绩效")
RULE3_POSITIVE_PERFORMANCE_KEYWORDS: tuple[str, ...] = ("销量一般", "销量较高", "爆品", "高绩效")
RULE3_NEGATIVE_PERFORMANCE_KEYWORDS: tuple[str, ...] = ("销量较差", "产生亏损", "亏损")
RULE_DECISION_TIME = 3
RULE_QUESTIONNAIRE = 4
RULE_CREDEMO = 5
RULE_SHOP_PROFILE = 6
_ALL_RULE_IDS: frozenset[int] = frozenset({1, 2, 3, 4, 5, 6})

# 默认启用的规则（可只保留子集，例如 frozenset({1, 2})）
DEFAULT_ENABLED_RULE_IDS: frozenset[int] = frozenset({1, 2, 3, 4, 5, 6})

# 规则2 附加：连续 N 轮（整数 round 相邻）提取的产品集合完全一致时，在报告中预警
FEEDBACK_CONSECUTIVE_SAME_PRODUCT_LEN = 3
# 规则2 同品跨轮：仅统计同一产品集合在 >= N 个 round 中重复出现
SAME_PRODUCT_MIN_ROUNDS = 3
# 为 True 时，将命中上述预警的用户并入剔除名单；False 时仅写入 filter_summary / 控制台预警
EXCLUDE_ON_FEEDBACK_CONSECUTIVE_3_SAME_PRODUCT = False
# 规则3（decision time）固定仅预警，不执行自动剔除
EXCLUDE_ON_RULE3_DECISION_TIME = False

RULE_FLAG_COLS: dict[int, str] = {
    RULE_EXTERNAL: "规则1_外部名单不合格",
    RULE_FEEDBACK_EMPTY: "规则2_feedback为空",
    RULE_DECISION_TIME: "规则3_decision_time不合格",
    RULE_QUESTIONNAIRE: "规则4_问卷不完整",
    RULE_CREDEMO: "规则5_credemo预警",
    RULE_SHOP_PROFILE: "规则6_店铺信息未修改",
}
USERS_SHOP_FILENAME = "users_shop.csv"
DEFAULT_SHOP_DESCRIPTION = "这是你的商店描述"
MANUAL_REVIEW_FILENAME = "manual_review_queue.csv"


def configure_runtime(
    project_root: Path,
    control_dirname: str | None = None,
    onlyai_dirname: str | None = None,
    combined_dirname: str | None = None,
) -> None:
    """覆盖默认运行时目录，便于由统一入口调度。"""
    global PROJECT_ROOT
    global CONTROL_DIRNAME
    global ONLYAI_DIRNAME
    global COMBINED_DIRNAME

    PROJECT_ROOT = Path(project_root).resolve()
    if control_dirname is not None:
        CONTROL_DIRNAME = str(control_dirname).strip()
    if onlyai_dirname is not None:
        ONLYAI_DIRNAME = str(onlyai_dirname).strip()
    if combined_dirname is not None:
        COMBINED_DIRNAME = str(combined_dirname).strip()


def _normalize_enabled_rules(enabled: set[int] | frozenset[int] | None) -> frozenset[int]:
    """返回校验后的规则集合；None 表示使用 DEFAULT_ENABLED_RULE_IDS。"""
    if enabled is None:
        return frozenset(DEFAULT_ENABLED_RULE_IDS)
    s = frozenset(enabled)
    bad = s - _ALL_RULE_IDS
    if bad:
        raise ValueError(f"未知规则编号 {sorted(bad)}，允许范围为 1–6")
    if not s:
        raise ValueError("至少启用一条规则（1–6），否则请直接跳过本脚本")
    return s


def _parse_cli_args() -> tuple[
    frozenset[int] | None,
    bool,
    bool,
    frozenset[int] | None,
    bool,
]:
    """返回 (启用规则子集或None用默认, 规则2连续同品剔除, 规则3剔除, pre轮次或None用默认, 规则5是否检查post)。"""
    parser = argparse.ArgumentParser(
        description="1_Merged_Data → 2_Filtered_Data，可按规则编号子集筛选。",
    )
    parser.add_argument(
        "--rules",
        type=str,
        default=None,
        metavar="N,N,...",
        help="启用规则编号，逗号分隔，如 1,2,5。省略则使用脚本内 DEFAULT_ENABLED_RULE_IDS。",
    )
    parser.add_argument(
        "--exclude-feedback-consecutive-3-same",
        action="store_true",
        help=(
            "连续 3 个整数 round 上，从 feedback 提取的产品集合完全一致时，将该用户并入剔除名单；"
            "默认仅写入 filter_summary 预警，不剔除。"
        ),
    )
    parser.add_argument(
        "--exclude-rule3-decision-time",
        action="store_true",
        help="已停用；规则3（decision time）现在固定仅预警，不会自动剔除用户。",
    )
    parser.add_argument(
        "--pre-rounds",
        type=str,
        default=None,
        metavar="N,N,...",
        help="预实验轮次（不参与规则2/3/4按round判定，且会从输出数据中移除），如 1,2。省略则使用脚本默认。",
    )
    parser.add_argument(
        "--credemo-check-post",
        action="store_true",
        help="规则5 credemo：同时检查 post 问卷（默认仅检查 pre）。",
    )
    args, _unknown = parser.parse_known_args()
    exclude_c3 = bool(args.exclude_feedback_consecutive_3_same)
    exclude_rule3 = False
    parsed_pre_rounds: frozenset[int] | None = None
    if args.pre_rounds is not None:
        parts_pre = [p.strip() for p in args.pre_rounds.split(",") if p.strip()]
        out_pre: set[int] = set()
        for p in parts_pre:
            try:
                n = int(p)
            except ValueError as e:
                raise SystemExit(f"--pre-rounds 无效片段 {p!r}，须为整数") from e
            out_pre.add(n)
        parsed_pre_rounds = frozenset(out_pre)
    credemo_check_post = bool(args.credemo_check_post)
    if args.rules is None:
        return None, exclude_c3, exclude_rule3, parsed_pre_rounds, credemo_check_post
    parts = [p.strip() for p in args.rules.split(",") if p.strip()]
    if not parts:
        return None, exclude_c3, exclude_rule3, parsed_pre_rounds, credemo_check_post
    out: set[int] = set()
    for p in parts:
        try:
            n = int(p)
        except ValueError as e:
            raise SystemExit(f"--rules 无效片段 {p!r}，须为 1–6 的整数") from e
        if n not in _ALL_RULE_IDS:
            raise SystemExit(f"规则编号须在 1–6 之间: {n}")
        out.add(n)
    return frozenset(out), exclude_c3, exclude_rule3, parsed_pre_rounds, credemo_check_post


def _normalize_pre_rounds(pre_rounds: set[int] | frozenset[int] | None) -> frozenset[int]:
    """None 时使用 DEFAULT_PRE_ROUNDS_EXCLUDED，并过滤到 ROUND_MIN~ROUND_MAX。"""
    if pre_rounds is None:
        raw = set(DEFAULT_PRE_ROUNDS_EXCLUDED)
    else:
        raw = {int(x) for x in pre_rounds}
    return frozenset(r for r in raw if ROUND_MIN <= r <= ROUND_MAX)


def _read_csv_any_encoding(path: Path) -> pd.DataFrame:
    """
    读取 CSV（utf-8-sig / utf-8 / gb18030 / gbk），并对合并阶段 latin-1 误读导致的 GB18030 乱码做修复。
    """
    return encoding_utils.read_csv_any_encoding_maybe_repair(
        path, allow_skip_bad_lines=False, repair_mojibake=True
    )


def _drop_pre_round_rows(df: pd.DataFrame, excluded_pre_rounds: frozenset[int]) -> tuple[pd.DataFrame, int]:
    """
    从数据中移除 pre round 行（若存在 round 列）。
    返回 (处理后df, 被移除行数)。
    """
    if not excluded_pre_rounds or "round" not in df.columns:
        return df, 0
    out = df.copy()
    round_int = pd.to_numeric(out["round"], errors="coerce")
    is_pre = round_int.isin(list(excluded_pre_rounds))
    removed = int(is_pre.sum())
    if removed <= 0:
        return out, 0
    out = out.loc[~is_pre].copy()
    return out, removed


def _get_decision_time_check_rounds(excluded_pre_rounds: frozenset[int]) -> list[int]:
    """规则3实际检查的轮次：总轮次去掉 pre rounds，按配置可再去掉最后一轮。"""
    rounds = [r for r in range(ROUND_MIN, ROUND_MAX + 1) if r not in excluded_pre_rounds]
    if DECISION_TIME_EXCLUDE_LAST_ROUND and ROUND_MAX in rounds:
        rounds = [r for r in rounds if r != ROUND_MAX]
    return rounds


def _get_questionnaire_check_rounds(excluded_pre_rounds: frozenset[int]) -> list[int]:
    """规则4实际检查的轮次：总轮次去掉 pre rounds，按配置可再去掉最后一轮。"""
    rounds = [r for r in range(ROUND_MIN, ROUND_MAX + 1) if r not in excluded_pre_rounds]
    if QUESTIONNAIRE_EXCLUDE_LAST_ROUND and ROUND_MAX in rounds:
        rounds = [r for r in rounds if r != ROUND_MAX]
    return rounds


def _combined_temp_mapping(project_root: Path) -> dict[str, str]:
    """两组临时账号映射合并（后者覆盖同名键，极少见）。"""
    cdir = project_root / RAW_SUBDIR / CONTROL_DIRNAME
    odir = project_root / RAW_SUBDIR / ONLYAI_DIRNAME
    xdir = project_root / RAW_SUBDIR / COMBINED_DIRNAME if COMBINED_DIRNAME else None
    m: dict[str, str] = {}
    if cdir.is_dir():
        m.update(username_mapping.load_temp_username_mapping(cdir))
    if odir.is_dir():
        m.update(username_mapping.load_temp_username_mapping(odir))
    if xdir and xdir.is_dir():
        m.update(username_mapping.load_temp_username_mapping(xdir))
    return m


def _map_username(u: str, mapping: dict[str, str]) -> str:
    s = str(u).strip()
    return mapping.get(s, s)


def _map_or_drop_temp_username(u: object, mapping: dict[str, str]) -> str | None:
    """
    用户名映射策略：
    - 若在临时账号映射表中：替换为正式账号
    - 若不在映射表且形似临时账号（如 E001/F123）：返回 None（后续清除）
    - 其它值：保持原样
    """
    s = str(u).strip()
    if not s or s.lower() in {"nan", "none"}:
        return None
    mapped = mapping.get(s)
    if mapped:
        return mapped
    if _TEMP_USERNAME_RE.fullmatch(s):
        return None
    return s


def _apply_username_mapping_with_temp_cleanup(df: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:
    """
    对 username/seller_username 应用“可映射则替换，不可映射的临时账号清除”策略。
    对 username 列：无法映射且形似临时账号的整行剔除。
    对 seller_username 列：无法映射且形似临时账号时置空（保留行）。
    """
    out = username_mapping.apply_username_mapping(df, mapping)
    if "username" in out.columns:
        u = out["username"].apply(lambda v: _map_or_drop_temp_username(v, mapping))
        out = out.loc[u.notna()].copy()
        out.loc[:, "username"] = u.loc[u.notna()].astype(str)
    if "seller_username" in out.columns:
        s = out["seller_username"].apply(lambda v: _map_or_drop_temp_username(v, mapping))
        out.loc[:, "seller_username"] = s.where(s.notna(), "")
    return out


def _collect_unmapped_temp_usernames_in_merged(merged_dir: Path, mapping: dict[str, str]) -> set[str]:
    """扫描 merged 中 username 列，收集“未映射成功的临时账号用户名”（如 E001）。"""
    out: set[str] = set()
    for p in sorted(merged_dir.glob("*.csv")):
        df = _read_csv_any_encoding(p)
        if "username" not in df.columns:
            continue
        for v in df["username"].dropna():
            s = str(v).strip()
            if not s:
                continue
            mapped = _map_or_drop_temp_username(s, mapping)
            if mapped is None and _TEMP_USERNAME_RE.fullmatch(s):
                out.add(s)
    return out


def _read_merged_csv_for_rules(merged_dir: Path, filename: str, mapping: dict[str, str]) -> pd.DataFrame | None:
    """规则前统一入口：读取 merged CSV，并执行“可映射则替换、未映射临时账号清理”."""
    path = merged_dir / filename
    if not path.is_file():
        return None
    df = _read_csv_any_encoding(path)
    return _apply_username_mapping_with_temp_cleanup(df, mapping)


def _exclude_usernames_from_df(df: pd.DataFrame, excluded_usernames: set[str] | frozenset[str] | None) -> pd.DataFrame:
    """若存在 username 列，则先剔除指定用户名（用于“规则前剔除”）。"""
    if not excluded_usernames or "username" not in df.columns:
        return df
    out = df.copy()
    u = out["username"].astype(str).str.strip()
    return out.loc[~u.isin(set(excluded_usernames))].copy()


def _exclude_usernames_from_col(
    df: pd.DataFrame,
    column_name: str,
    excluded_usernames: set[str] | frozenset[str] | None,
) -> pd.DataFrame:
    """按指定用户名列剔除用户。"""
    if not excluded_usernames or column_name not in df.columns:
        return df
    out = df.copy()
    u = out[column_name].astype(str).str.strip()
    return out.loc[~u.isin(set(excluded_usernames))].copy()


def filter_usernames_default_shop_profile(
    merged_dir: Path,
    mapping: dict[str, str],
    pre_excluded_usernames: set[str] | frozenset[str] | None = None,
) -> tuple[set[str], dict[str, list[str]]]:
    """识别未修改默认店铺名/描述的用户。"""
    df = _read_merged_csv_for_rules(merged_dir, USERS_SHOP_FILENAME, mapping)
    if df is None:
        return set(), {}

    user_col = "seller_username" if "seller_username" in df.columns else ("username" if "username" in df.columns else None)
    if user_col is None or "name" not in df.columns or "description" not in df.columns:
        return set(), {}

    df = _exclude_usernames_from_col(df, user_col, pre_excluded_usernames)
    flagged: set[str] = set()
    details: dict[str, list[str]] = {}
    for _, row in df.iterrows():
        username = str(row.get(user_col, "")).strip()
        if not username:
            continue
        reasons: list[str] = []
        shop_name = str(row.get("name", "")).strip()
        description = str(row.get("description", "")).strip()
        default_name = f"{username}的商店"
        if shop_name == default_name:
            reasons.append(f"name 仍为默认值「{default_name}」")
        if description == DEFAULT_SHOP_DESCRIPTION:
            reasons.append(f"description 仍为默认值「{DEFAULT_SHOP_DESCRIPTION}」")
        if reasons:
            flagged.add(username)
            details[username] = reasons
    return flagged, details


def _build_manual_review_rows(
    rule_usernames: dict[int, set[str]],
    decision_time_details: dict[str, str],
    credemo_missing_pre: list[str],
    credemo_missing_post: list[str],
    credemo_missing_both: list[str],
    shop_profile_details: dict[str, list[str]],
) -> list[dict[str, str]]:
    """构建人工复核候选表。"""
    rows: list[dict[str, str]] = []
    for username in sorted(rule_usernames.get(RULE_DECISION_TIME, set())):
        rows.append(
            {
                "username": username,
                "warning_rule": "规则3_decision_time",
                "warning_reason": decision_time_details.get(username, "命中 decision time 预警"),
                "suggested_action": "人工复核后决定是否剔除",
            }
        )

    for username in sorted(set(credemo_missing_pre)):
        rows.append(
            {
                "username": username,
                "warning_rule": "规则5_credemo",
                "warning_reason": "credemo 缺 pre",
                "suggested_action": "人工复核后决定是否剔除",
            }
        )
    for username in sorted(set(credemo_missing_post)):
        rows.append(
            {
                "username": username,
                "warning_rule": "规则5_credemo",
                "warning_reason": "credemo 缺 post",
                "suggested_action": "人工复核后决定是否剔除",
            }
        )
    for username in sorted(set(credemo_missing_both)):
        rows.append(
            {
                "username": username,
                "warning_rule": "规则5_credemo",
                "warning_reason": "credemo pre/post 都缺",
                "suggested_action": "人工复核后决定是否剔除",
            }
        )

    for username in sorted(rule_usernames.get(RULE_SHOP_PROFILE, set())):
        detail_text = "；".join(shop_profile_details.get(username, [])) or "店铺名称/描述未修改"
        rows.append(
            {
                "username": username,
                "warning_rule": "规则6_店铺信息未修改",
                "warning_reason": detail_text,
                "suggested_action": "默认建议剔除；若人工确认例外可填保留",
            }
        )
    return rows


def write_manual_review_queue(out_dir: Path, rows: list[dict[str, str]]) -> Path:
    """写出人工复核表，并保留已填写的处理意见。"""
    review_path = out_dir / MANUAL_REVIEW_FILENAME
    base_cols = [
        "username",
        "warning_rule",
        "warning_reason",
        "suggested_action",
        "reviewer_decision",
        "reviewer_note",
    ]
    existing_map: dict[tuple[str, str], dict[str, str]] = {}
    if review_path.is_file():
        existing_df = pd.read_csv(review_path, low_memory=False)
        for _, row in existing_df.iterrows():
            username = str(row.get("username", "")).strip()
            warning_rule = str(row.get("warning_rule", "")).strip()
            if username and warning_rule:
                existing_map[(username, warning_rule)] = {
                    "reviewer_decision": str(row.get("reviewer_decision", "")).strip(),
                    "reviewer_note": str(row.get("reviewer_note", "")).strip(),
                }

    output_rows: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for row in rows:
        username = str(row.get("username", "")).strip()
        warning_rule = str(row.get("warning_rule", "")).strip()
        if not username or not warning_rule:
            continue
        key = (username, warning_rule)
        if key in seen:
            continue
        seen.add(key)
        existing = existing_map.get(key, {})
        output_rows.append(
            {
                "username": username,
                "warning_rule": warning_rule,
                "warning_reason": str(row.get("warning_reason", "")).strip(),
                "suggested_action": str(row.get("suggested_action", "")).strip(),
                "reviewer_decision": existing.get("reviewer_decision", ""),
                "reviewer_note": existing.get("reviewer_note", ""),
            }
        )

    pd.DataFrame(output_rows, columns=base_cols).to_csv(review_path, index=False, encoding="utf-8-sig")
    return review_path


def _get_control_onlyai_username_sets() -> tuple[set[str], set[str]]:
    """raw users_feedback 中的 username 经各组临时账号映射后，与 1_Merged_Data 一致，用于打 Group。"""
    control_path = PROJECT_ROOT / RAW_SUBDIR / CONTROL_DIRNAME / RAW_MYSQL_SUBDIR / USER_FEEDBACK_FILENAME
    onlyai_path = PROJECT_ROOT / RAW_SUBDIR / ONLYAI_DIRNAME / RAW_MYSQL_SUBDIR / USER_FEEDBACK_FILENAME
    map_c = username_mapping.load_temp_username_mapping(PROJECT_ROOT / RAW_SUBDIR / CONTROL_DIRNAME)
    map_o = username_mapping.load_temp_username_mapping(PROJECT_ROOT / RAW_SUBDIR / ONLYAI_DIRNAME)
    control_usernames: set[str] = set()
    onlyai_usernames: set[str] = set()
    if control_path.is_file():
        df = _read_csv_any_encoding(control_path)
        if "username" in df.columns:
            for u in df["username"].astype(str).str.strip().dropna().unique():
                control_usernames.add(_map_username(u, map_c))
    if onlyai_path.is_file():
        df = _read_csv_any_encoding(onlyai_path)
        if "username" in df.columns:
            for u in df["username"].astype(str).str.strip().dropna().unique():
                onlyai_usernames.add(_map_username(u, map_o))
    return control_usernames, onlyai_usernames


def _core_study_id(u: str) -> str:
    """a202300630129 → 202300630129，便于与 credemo 学号列对齐。"""
    u = str(u).strip()
    if len(u) > 1 and u[0].isalpha() and u[1:].isdigit():
        return u[1:]
    return u


def _credemo_pick_id_column(df: pd.DataFrame) -> str | None:
    if CREDEMO_COL_PRE in df.columns:
        return CREDEMO_COL_PRE
    if CREDEMO_COL_POST in df.columns:
        return CREDEMO_COL_POST
    for c in df.columns:
        if "学号" in str(c) and "数据匹配" in str(c):
            return str(c)
    return None


def _credemo_filename_kind(name: str) -> str | None:
    stem = Path(name).stem.lower()
    stem_norm = stem.replace("_", "-")
    if (
        "pre-task" in stem_norm
        or "pretask" in stem_norm
        or stem_norm.startswith("pre-")
        or stem_norm.startswith("pre")
    ):
        return "pre"
    if (
        ("post" in stem_norm and "task" in stem_norm)
        or "posttask" in stem_norm
        or stem_norm.startswith("post-")
        or stem_norm.startswith("post")
    ):
        return "post"
    return None


def _collect_credemo_id_sets(
    project_root: Path,
    *,
    require_post_for_active: bool = False,
) -> tuple[set[str], set[str], bool]:
    """
    返回 (pre 核心学号集合, post 核心学号集合, 是否启用 credemo 规则)。

    - require_post_for_active=False（默认）：至少 1 个 pre 类 csv 即启用（仅对 pre 做缺失预警）。
    - require_post_for_active=True：须同时至少 1 个 pre 与 1 个 post 类 csv 才启用（与旧版一致）。
    """
    pre_ids: set[str] = set()
    post_ids: set[str] = set()
    n_pre_files = 0
    n_post_files = 0
    group_dirnames = [CONTROL_DIRNAME, ONLYAI_DIRNAME]
    if COMBINED_DIRNAME:
        group_dirnames.append(COMBINED_DIRNAME)
    for group_dirname in group_dirnames:
        credemo_dir = project_root / RAW_SUBDIR / group_dirname / CREDEMO_SUBDIR
        if not credemo_dir.is_dir():
            continue
        for p in sorted(credemo_dir.glob("*.csv")):
            kind = _credemo_filename_kind(p.name)
            if kind is None:
                continue
            if kind == "pre":
                n_pre_files += 1
            else:
                n_post_files += 1
            df = _read_csv_any_encoding(p)
            col = _credemo_pick_id_column(df)
            if col is None or col not in df.columns:
                continue
            target = pre_ids if kind == "pre" else post_ids
            for v in df[col].dropna():
                s = username_mapping._cell_to_canonical_str(v)
                if s:
                    target.add(_core_study_id(s))
    if require_post_for_active:
        active = n_pre_files > 0 and n_post_files > 0
    else:
        active = n_pre_files > 0
    return pre_ids, post_ids, active


def filter_usernames_external_disqualified(project_root: Path) -> set[str]:
    """operation/实验不合格样本记录.csv，列 username；值经合并后的临时账号映射。"""
    mapping = _combined_temp_mapping(project_root)
    out: set[str] = set()
    group_dirnames = [CONTROL_DIRNAME, ONLYAI_DIRNAME]
    if COMBINED_DIRNAME:
        group_dirnames.append(COMBINED_DIRNAME)
    for group_dirname in group_dirnames:
        path = project_root / RAW_SUBDIR / group_dirname / OPERATION_SUBDIR / DISQUALIFIED_CSV
        if not path.is_file():
            continue
        df = _read_csv_any_encoding(path)
        if "username" not in df.columns:
            raise ValueError(f"{path} 需包含列 'username'，当前: {list(df.columns)}")
        for v in df["username"].dropna():
            s = str(v).strip()
            if not s:
                continue
            out.add(_map_username(s, mapping))
    return out


def _sort_round_like_values(vals: list) -> list:
    """用于 round 等可数值排序的展示顺序。"""

    def _key(x: object) -> tuple:
        try:
            return (0, float(x))
        except (TypeError, ValueError):
            return (1, str(x))

    uniq = list(pd.Series(vals).drop_duplicates())
    return sorted(uniq, key=_key)


def _empty_feedback_locations_per_user(df: pd.DataFrame, empty: pd.Series) -> dict[str, str]:
    """
    每个被剔除用户名一条中文说明（简洁版）：缺 feedback 的 round 概览。
    若某用户所有 round 都缺，输出“每一round缺的都为空”。
    若无 round 列，则回退为行号说明。
    """
    has_round = "round" in df.columns
    # username -> list of (round, line_no) 或仅 line_no
    buckets: dict[str, list] = {}
    for pos in range(len(df)):
        if not bool(empty.iloc[pos]):
            continue
        uname = str(df.iloc[pos]["username"]).strip()
        line_no = pos + 2
        if has_round:
            buckets.setdefault(uname, []).append((df.iloc[pos]["round"], line_no))
        else:
            buckets.setdefault(uname, []).append(line_no)

    out: dict[str, str] = {}
    for uname, locs in buckets.items():
        if has_round:
            # locs: list[(round, line_no)]；按 round 汇总，避免输出过长
            by_round: dict[object, list[int]] = {}
            for r, ln in locs:
                by_round.setdefault(r, []).append(int(ln))
            miss_rounds = _sort_round_like_values(list(by_round.keys()))
            miss_round_text = "、".join(str(r) for r in miss_rounds)
            all_rounds = (
                _sort_round_like_values(list(pd.Series(df["round"]).dropna().unique()))
                if "round" in df.columns
                else []
            )
            if all_rounds and miss_rounds == all_rounds:
                out[uname] = f"每一round缺的都为空（round {miss_round_text}）"
            else:
                out[uname] = f"缺 feedback 的 round：{miss_round_text}"
        else:
            line_nos = sorted({int(x) for x in locs})
            joined = "、".join(str(n) for n in line_nos)
            out[uname] = f"缺 feedback 的数据行（第 1 行为表头）：第 {joined} 行"
    return out


def _parse_round_for_feedback_row(r: object) -> int | None:
    try:
        return int(float(r))
    except (TypeError, ValueError):
        return None


def _extract_product_signature_from_feedback(feedback: object) -> tuple[str, ...]:
    """
    从一条 feedback 文本中按行提取「产品xxx」片段中的产品名，去重后排序为元组，用于跨轮次比对。

    多条产品线时，以「产品名集合」相同视为同一选择；与整段字符串比对无关。
    """
    if pd.isna(feedback):
        return ()
    text = str(feedback).strip()
    if not text or text.lower() == "nan":
        return ()
    names: list[str] = []
    for line in text.split("\n"):
        line = line.strip()
        if not line:
            continue
        m = _FEEDBACK_PRODUCT_RE.search(line)
        if m:
            n = m.group(1).strip()
            if n:
                names.append(n)
    if not names:
        return ()
    return tuple(sorted(set(names)))


def _format_product_signature_display(sig: tuple[str, ...]) -> str:
    if len(sig) == 1:
        return sig[0]
    return " + ".join(sig)


def _user_round_product_signatures(
    df: pd.DataFrame,
    excluded_pre_rounds: set[int] | frozenset[int] | None = None,
) -> dict[str, dict[int, tuple[str, ...]]]:
    """username -> round(int) -> 该产品轮次从 feedback 提取的产品名有序元组（空则该 round 不入表）。"""
    if "round" not in df.columns or "username" not in df.columns or "feedback" not in df.columns:
        return {}
    work = df.copy()
    work["_u"] = work["username"].astype(str).str.strip()
    excluded_pre = _normalize_pre_rounds(excluded_pre_rounds)
    user_round_sig: dict[str, dict[int, tuple[str, ...]]] = {}
    for (uname, r), g in work.groupby(["_u", "round"], sort=False):
        r_int = _parse_round_for_feedback_row(r)
        if r_int is None:
            continue
        if r_int in excluded_pre:
            continue
        chunks: list[str] = []
        for _, row in g.iterrows():
            fb = row["feedback"]
            if pd.isna(fb):
                continue
            s = str(fb).strip()
            if s and s.lower() != "nan":
                chunks.append(s)
        if not chunks:
            continue
        sig = _extract_product_signature_from_feedback("\n".join(chunks))
        if not sig:
            continue
        user_round_sig.setdefault(uname, {})[r_int] = sig
    return user_round_sig


def _user_round_feedback_texts(
    df: pd.DataFrame,
    excluded_pre_rounds: set[int] | frozenset[int] | None = None,
) -> dict[str, dict[int, str]]:
    """username -> round(int) -> 该轮次原始 feedback 文本（同轮多行用分隔符拼接）。"""
    if "round" not in df.columns or "username" not in df.columns or "feedback" not in df.columns:
        return {}
    work = df.copy()
    work["_u"] = work["username"].astype(str).str.strip()
    excluded_pre = _normalize_pre_rounds(excluded_pre_rounds)
    out: dict[str, dict[int, str]] = {}
    for (uname, r), g in work.groupby(["_u", "round"], sort=False):
        r_int = _parse_round_for_feedback_row(r)
        if r_int is None:
            continue
        if r_int in excluded_pre:
            continue
        chunks: list[str] = []
        for _, row in g.iterrows():
            fb = row["feedback"]
            if pd.isna(fb):
                continue
            s = str(fb).strip()
            if s and s.lower() != "nan":
                chunks.append(s)
        if not chunks:
            continue
        out.setdefault(uname, {})[r_int] = "\n---\n".join(chunks)
    return out


def _feedback_has_any_keyword(text: str, keywords: tuple[str, ...]) -> bool:
    s = str(text or "").strip()
    if not s:
        return False
    return any(k in s for k in keywords)


def _same_product_across_rounds_analysis(
    df: pd.DataFrame,
    excluded_pre_rounds: set[int] | frozenset[int] | None = None,
) -> dict[str, object]:
    """
    同一 username 下，若产品名集合（来自「产品…」片段）在 >= SAME_PRODUCT_MIN_ROUNDS 个 round 中完全相同，则记入结果。

    返回 {"status": "no_round" | "ok", "items": list[str]}，items 为可直接打印/写入摘要的短句。
    """
    if "round" not in df.columns:
        return {"status": "no_round", "items": []}
    user_round_sig = _user_round_product_signatures(df, excluded_pre_rounds=excluded_pre_rounds)
    items: list[str] = []
    for uname in sorted(user_round_sig):
        rmap = user_round_sig[uname]
        inv: dict[tuple[str, ...], list[int]] = {}
        for r_int, sig in rmap.items():
            inv.setdefault(sig, []).append(r_int)
        for sig, rounds in inv.items():
            if len(rounds) < SAME_PRODUCT_MIN_ROUNDS:
                continue
            rounds_s = "、".join(str(x) for x in sorted(rounds))
            label = _format_product_signature_display(sig)
            items.append(f"{uname} — 「{label}」在 round {rounds_s} 中相同")

    return {"status": "ok", "items": items}


def _consecutive_same_product_analysis(
    df: pd.DataFrame,
    window: int = FEEDBACK_CONSECUTIVE_SAME_PRODUCT_LEN,
    excluded_pre_rounds: set[int] | frozenset[int] | None = None,
) -> dict[str, object]:
    """
    报告“最长连续区间”：同一用户在相邻整数 round 上产品集合一致，且连续长度 >= window。

    示例：若 4-7 都一致，仅报一条 4-7；不再重复报 4-6、5-7。
    返回 {
      "status": "no_round"|"ok",
      "items": list[str],               # 保留预警
      "usernames": set[str],            # 保留预警用户名
      "waived_items": list[str],        # 解除预警
      "waived_usernames": set[str],     # 解除预警用户名
      "details": dict
    }。
    """
    if "round" not in df.columns:
        return {
            "status": "no_round",
            "items": [],
            "usernames": set(),
            "waived_items": [],
            "waived_usernames": set(),
            "details": {},
        }
    user_round_sig = _user_round_product_signatures(df, excluded_pre_rounds=excluded_pre_rounds)
    user_round_feedback = _user_round_feedback_texts(df, excluded_pre_rounds=excluded_pre_rounds)
    items: list[str] = []
    flagged: set[str] = set()
    waived_items: list[str] = []
    waived_users: set[str] = set()
    details: dict[str, list[dict[str, object]]] = {}
    k = max(int(window), 2)

    for uname in sorted(user_round_sig):
        rmap = user_round_sig[uname]
        rounds_sorted = sorted(rmap.keys())
        if not rounds_sorted:
            continue
        i = 0
        while i < len(rounds_sorted):
            start_r = rounds_sorted[i]
            sig = rmap[start_r]
            j = i + 1
            # 同签名且 round 连续，扩展到最长区间
            while (
                j < len(rounds_sorted)
                and rounds_sorted[j] == rounds_sorted[j - 1] + 1
                and rmap[rounds_sorted[j]] == sig
            ):
                j += 1
            run_rounds = rounds_sorted[i:j]
            if len(run_rounds) >= k:
                label = _format_product_signature_display(sig)
                if len(run_rounds) == 2:
                    rs = f"{run_rounds[0]}、{run_rounds[1]}"
                else:
                    rs = f"{run_rounds[0]}-{run_rounds[-1]}"
                fb_map = user_round_feedback.get(uname, {})
                feedback_by_round = {r: fb_map.get(r, "") for r in run_rounds}
                transitions_positive = True
                transition_notes: list[str] = []
                for idx_t in range(1, len(run_rounds)):
                    prev_r = run_rounds[idx_t - 1]
                    cur_r = run_rounds[idx_t]
                    prev_fb = feedback_by_round.get(prev_r, "")
                    is_pos = _feedback_has_any_keyword(prev_fb, RULE2_POSITIVE_PERFORMANCE_KEYWORDS)
                    transitions_positive = transitions_positive and is_pos
                    transition_notes.append(
                        f"{prev_r}->{cur_r}:{'上一轮正向表现' if is_pos else '上一轮非正向/未识别'}"
                    )
                notes_text = "；".join(transition_notes) if transition_notes else "（无可判定轮次）"
                if transitions_positive:
                    waived_items.append(
                        f"{uname} — 「{label}」在连续 round {rs}（共 {len(run_rounds)} 轮）产品集合一致，"
                        f"且每次继续持有均对应上一轮正向表现，解除预警。[{notes_text}]"
                    )
                    waived_users.add(uname)
                    alert_state = "waived"
                else:
                    items.append(
                        f"{uname} — 「{label}」在连续 round {rs}（共 {len(run_rounds)} 轮）产品集合一致【预警】"
                        f"[{notes_text}]"
                    )
                    flagged.add(uname)
                    alert_state = "alert"
                details.setdefault(uname, []).append(
                    {
                        "rounds": list(run_rounds),
                        "product_label": label,
                        "feedback_by_round": feedback_by_round,
                        "alert_state": alert_state,
                        "transition_notes": transition_notes,
                    }
                )
            i = j

    return {
        "status": "ok",
        "items": items,
        "usernames": flagged,
        "waived_items": waived_items,
        "waived_usernames": waived_users,
        "details": details,
    }


def _extract_product_ids_from_details(details_obj: object) -> set[str]:
    """
    从 operation.details 里提取产品ID集合。
    兼容：
    - {"products":[{"product_id":...}, ...]}
    - list[{"id"/"product_id":...}, ...]
    """
    products: list[object] = []
    if isinstance(details_obj, dict):
        p = details_obj.get("products")
        if isinstance(p, list):
            products = p
    elif isinstance(details_obj, list):
        products = details_obj
    out: set[str] = set()
    for x in products:
        if not isinstance(x, dict):
            continue
        pid = x.get("product_id", x.get("id"))
        if pid is None:
            continue
        s = str(pid).strip()
        if s:
            out.add(s)
    return out


def _load_user_round_product_sets_from_operation_log(
    merged_dir: Path,
    mapping: dict[str, str] | None = None,
    pre_excluded_usernames: set[str] | frozenset[str] | None = None,
) -> dict[str, dict[int, set[str]]]:
    """
    从 user_operation_log 的 end_round 事件提取每个用户每轮最终产品集合。
    返回 username -> round -> set(product_id)。
    """
    path = merged_dir / USER_OPERATION_LOG
    if not path.is_file():
        return {}
    map_used = mapping or {}
    df = _read_merged_csv_for_rules(merged_dir, USER_OPERATION_LOG, map_used)
    if df is None:
        return {}
    df = _exclude_usernames_from_df(df, pre_excluded_usernames)
    if not {"username", "operation", "round", "details"}.issubset(df.columns):
        return {}
    work = df[df["operation"].astype(str) == "end_round"].copy()
    if work.empty:
        return {}
    out: dict[str, dict[int, set[str]]] = {}
    for _, row in work.iterrows():
        uname = str(row["username"]).strip()
        try:
            r = int(float(row["round"]))
        except (TypeError, ValueError):
            continue
        raw = row["details"]
        parsed: object
        if isinstance(raw, (dict, list)):
            parsed = raw
        else:
            s = str(raw).strip()
            if not s:
                continue
            try:
                parsed = json.loads(s)
            except Exception:
                try:
                    parsed = ast.literal_eval(s)
                except Exception:
                    continue
        pid_set = _extract_product_ids_from_details(parsed)
        if not pid_set:
            continue
        out.setdefault(uname, {})[r] = pid_set
    return out


def _load_user_round_feedback_text_map(
    merged_dir: Path,
    mapping: dict[str, str] | None = None,
    pre_excluded_usernames: set[str] | frozenset[str] | None = None,
) -> dict[str, dict[int, str]]:
    """从 users_feedback 聚合 username+round 的 feedback 文本。"""
    fb_path = merged_dir / USER_FEEDBACK_FILENAME
    if not fb_path.is_file():
        return {}
    map_used = mapping or {}
    df = _read_merged_csv_for_rules(merged_dir, USER_FEEDBACK_FILENAME, map_used)
    if df is None:
        return {}
    df = _exclude_usernames_from_df(df, pre_excluded_usernames)
    if not {"username", "round", "feedback"}.issubset(df.columns):
        return {}
    out: dict[str, dict[int, str]] = {}
    work = df.copy()
    work["_u"] = work["username"].astype(str).str.strip()
    for (uname, r), g in work.groupby(["_u", "round"], sort=False):
        try:
            r_int = int(float(r))
        except (TypeError, ValueError):
            continue
        chunks: list[str] = []
        for fb in g["feedback"].tolist():
            if pd.isna(fb):
                continue
            s = str(fb).strip()
            if s and s.lower() != "nan":
                chunks.append(s)
        if not chunks:
            continue
        out.setdefault(uname, {})[r_int] = "\n---\n".join(chunks)
    return out


def filter_usernames_empty_feedback(
    merged_dir: Path,
    mapping: dict[str, str] | None = None,
    excluded_pre_rounds: set[int] | frozenset[int] | None = None,
    pre_excluded_usernames: set[str] | frozenset[str] | None = None,
) -> tuple[set[str], int, dict[str, int], dict[str, object]]:
    """
    返回：(需剔除用户名集合, feedback 为空的行数, 每用户空行数, 统计字典供打印)。

    统计字典 success 时含 available=True 及 total_users, total_rows, n_distinct_rounds(可选) 等；
    失败时 available=False 及 reason。
    """
    fb_path = merged_dir / USER_FEEDBACK_FILENAME
    if not fb_path.is_file():
        return set(), 0, {}, {"available": False, "reason": "no_file", "path": str(fb_path.name)}
    map_used = mapping or {}
    df = _read_merged_csv_for_rules(merged_dir, USER_FEEDBACK_FILENAME, map_used)
    if df is None:
        return set(), 0, {}, {"available": False, "reason": "no_file", "path": str(fb_path.name)}
    df = _exclude_usernames_from_df(df, pre_excluded_usernames)
    if "feedback" not in df.columns or "username" not in df.columns:
        return set(), 0, {}, {"available": False, "reason": "no_columns"}
    u = df["username"].astype(str).str.strip()
    total_users = int(u.nunique())
    total_rows = len(df)
    excluded_pre = _normalize_pre_rounds(excluded_pre_rounds)
    if "round" in df.columns and excluded_pre:
        round_int = pd.to_numeric(df["round"], errors="coerce")
        is_pre_round = round_int.isin(list(excluded_pre))
    else:
        is_pre_round = pd.Series(False, index=df.index)
    empty = (df["feedback"].isna() | (df["feedback"].astype(str).str.strip() == "")) & (~is_pre_round)
    empty_df = df.loc[empty].copy()
    empty_df["username"] = empty_df["username"].astype(str).str.strip()
    per_user_count = empty_df.groupby("username", sort=False).size().to_dict()
    excluded_set = set(per_user_count.keys())
    n_empty_rows = int(empty.sum())
    rows_for_excluded_users = int(u.isin(excluded_set).sum()) if excluded_set else 0
    remaining_users = total_users - len(excluded_set)

    stats: dict[str, object] = {
        "available": True,
        "total_users": total_users,
        "total_rows": total_rows,
        "n_excluded_users": len(excluded_set),
        "rows_removed_for_excluded_users": rows_for_excluded_users,
        "n_empty_feedback_rows": n_empty_rows,
        "remaining_users": remaining_users,
    }
    if "round" in df.columns:
        stats["n_distinct_rounds"] = int(df["round"].nunique())
        stats["excluded_pre_rounds"] = sorted(excluded_pre)

    stats["empty_feedback_locations_by_user"] = _empty_feedback_locations_per_user(df, empty)
    stats["same_product_across_rounds"] = _same_product_across_rounds_analysis(df, excluded_pre_rounds=excluded_pre)
    stats["consecutive_3_same_product"] = _consecutive_same_product_analysis(
        df, excluded_pre_rounds=excluded_pre
    )

    return excluded_set, n_empty_rows, per_user_count, stats


def _print_rule2_feedback_summary(fb_stats: dict[str, object]) -> None:
    """规则2 专用控制台汇总。"""
    if not fb_stats:
        return
    if fb_stats.get("available") is False:
        reason = fb_stats.get("reason")
        if reason == "no_file":
            print(f"[规则2 feedback] 未找到 {USER_FEEDBACK_FILENAME}，无法统计。")
        elif reason == "no_columns":
            print("[规则2 feedback] 表中缺少 username 或 feedback 列，无法统计。")
        return

    total_users = fb_stats["total_users"]
    total_rows = fb_stats["total_rows"]
    n_rounds = fb_stats.get("n_distinct_rounds")
    round_hint = f"，数据中 round 列共有 {n_rounds} 个不同取值" if n_rounds is not None else ""
    pre_rounds = fb_stats.get("excluded_pre_rounds") or []
    pre_hint = f"；不参与判定的 pre round: {pre_rounds}" if pre_rounds else ""

    print(
        f"[规则2 feedback] 输入 users_feedback：共 {total_users} 名用户，{total_rows} 行记录{round_hint}{pre_hint}。"
    )
    print(f"  剔除依据：{RULE2_FEEDBACK_RULE_TEXT}")
    print(
        f"  剔除：{fb_stats['n_excluded_users']} 名用户，"
        f"{fb_stats['rows_removed_for_excluded_users']} 行（被剔除用户在本表中的全部行）；"
        f"其中 feedback 字段为空的记录共 {fb_stats['n_empty_feedback_rows']} 条。"
    )
    locs = fb_stats.get("empty_feedback_locations_by_user") or {}
    if locs:
        print("  缺 feedback 的具体位置（按用户）：")
        for un in sorted(locs):
            print(f"    {un} — {locs[un]}")
    print(f"  按本规则保留用户：{fb_stats['remaining_users']} 名。")

    c3 = fb_stats.get("consecutive_3_same_product")
    if isinstance(c3, dict):
        k = FEEDBACK_CONSECUTIVE_SAME_PRODUCT_LEN
        print(
            f"  --- 【预警】连续 >= {k} 轮整数 round 上产品集合一致（去重后最长区间展示；非剔除条件，除非开启额外剔除开关）---"
        )
        if c3.get("status") == "no_round":
            print("    （表中无 round 列，跳过此项。）")
        elif c3.get("status") == "ok":
            items3 = c3.get("items") or []
            nu = c3.get("usernames") or set()
            if not items3:
                print(f"    （未发现连续 {k} 轮同品集合的用户。）")
            else:
                print(f"    涉及用户约 {len(nu)} 人，明细：")
                for line in items3:
                    print(f"    {line}")


def analyze_credemo_missing_after_prior_rules(
    merged_dir: Path,
    pre_ids: set[str],
    post_ids: set[str],
    active: bool,
    users_after_rule2: set[str],
    *,
    check_post_questionnaire: bool = False,
) -> dict[str, object]:
    """
    规则5（credemo）改为预警：仅在前序规则之后的用户上检查 credemo 学号是否出现在导出问卷中，不剔除。

    check_post_questionnaire=False 时只判定是否出现在 pre_ids 中（缺 pre 即预警；不检查 post）。
    check_post_questionnaire=True 时与旧版一致：缺 pre / 缺 post / 两者都缺分别列出。

    返回：
      {
        "active": bool,
        "checked_users": int,
        "missing_pre": list[str],
        "missing_post": list[str],
        "missing_both": list[str],
      }
    """
    if not active:
        return {
            "active": False,
            "checked_users": 0,
            "missing_pre": [],
            "missing_post": [],
            "missing_both": [],
        }
    if not users_after_rule2:
        return {
            "active": True,
            "checked_users": 0,
            "missing_pre": [],
            "missing_post": [],
            "missing_both": [],
        }
    missing_pre: list[str] = []
    missing_post: list[str] = []
    missing_both: list[str] = []
    for u in sorted(users_after_rule2):
        core = _core_study_id(u)
        in_pre = core in pre_ids
        in_post = core in post_ids
        if check_post_questionnaire:
            if (not in_pre) and (not in_post):
                missing_both.append(u)
            elif not in_pre:
                missing_pre.append(u)
            elif not in_post:
                missing_post.append(u)
        else:
            if not in_pre:
                missing_pre.append(u)
    return {
        "active": True,
        "checked_users": len(users_after_rule2),
        "missing_pre": missing_pre,
        "missing_post": missing_post,
        "missing_both": missing_both,
    }


def _pick_participant_id_column(df: pd.DataFrame) -> str | None:
    for c in ("请填写你的学号（用于数据匹配）", "请填写您的学号（用于数据匹配）", "学号", "username"):
        if c in df.columns:
            return c
    for c in df.columns:
        cs = str(c)
        if "学号" in cs:
            return cs
    return None


def _find_participant_info_xlsx(project_root: Path) -> Path | None:
    """
    动态定位被试信息表：
    1) 优先按固定文件名在 Combined/OnlyAI/Control 的 operation 下查找
    2) 若未命中，回退到 operation 下按“被试信息收集表*.xlsx”模式匹配
    """
    raw_root = project_root / RAW_SUBDIR
    group_order: list[str] = []
    if COMBINED_DIRNAME:
        group_order.append(COMBINED_DIRNAME)
    group_order.extend([ONLYAI_DIRNAME, CONTROL_DIRNAME])

    # 先查固定文件名
    for g in group_order:
        op_dir = raw_root / g / OPERATION_SUBDIR
        p = op_dir / PARTICIPANT_INFO_XLSX
        if p.is_file():
            return p

    # 再按名称模式回退（兼容“电商运营实验被试信息收集表_第二批次.xlsx”等）
    candidates: list[Path] = []
    for g in group_order:
        op_dir = raw_root / g / OPERATION_SUBDIR
        if not op_dir.is_dir():
            continue
        for p in op_dir.glob("*.xlsx"):
            name = p.name
            if "被试信息收集表" in name:
                candidates.append(p)
    if candidates:
        # group_order 已按优先级遍历；同组按文件名排序保证稳定性
        candidates = sorted(candidates, key=lambda x: x.name)
        return candidates[0]
    return None


def export_rule_flags_to_participant_sheet(
    rule_usernames: dict[int, set[str]],
    pre_ids: set[str],
    post_ids: set[str],
    feedback_users_after_rule2: set[str] | None = None,
    *,
    credemo_check_post: bool = False,
) -> tuple[Path | None, list[str]]:
    """
    将每条规则命中的用户标注到被试信息表（每规则一列 X），并输出汇总列。

    返回：(输出路径或 None, 可写入 summary 的说明行)。
    """
    info_path = _find_participant_info_xlsx(PROJECT_ROOT)
    if info_path is None:
        expected = PROJECT_ROOT / RAW_SUBDIR / ONLYAI_DIRNAME / OPERATION_SUBDIR / PARTICIPANT_INFO_XLSX
        return None, [f"【规则标注表】未找到被试信息表: {expected}（已尝试 Combined/OnlyAI/Control 的 operation）"]
    df = pd.read_excel(info_path)
    id_col = _pick_participant_id_column(df)
    if id_col is None:
        return None, ["【规则标注表】被试信息表未找到学号列（如“请填写你的学号（用于数据匹配）”）。"]

    # 学号列做“格式规范化”：仅修正科学计数法/xxx.0/空值，不做临时账号替换
    id_col_canon = df[id_col].apply(lambda v: username_mapping._cell_to_canonical_str(v) or "")
    df[id_col] = id_col_canon
    core_ids = id_col_canon.apply(lambda v: _core_study_id(v) if str(v).strip() else "")
    df["_core_id_tmp"] = core_ids

    # 规则命中（用户名）先做临时账号映射，再统一转核心学号（用于匹配“学号列”的核心学号）
    temp_mapping = _combined_temp_mapping(PROJECT_ROOT)
    rule_core_ids: dict[int, set[str]] = {}
    for rid, users in rule_usernames.items():
        mapped_users = {_map_username(str(u).strip(), temp_mapping) for u in users if str(u).strip()}
        rule_core_ids[rid] = {_core_study_id(u) for u in mapped_users if str(u).strip()}

    # 不再向原始被试信息表“补行”（避免把替换后的学号写进最终表）

    hit_cols: list[str] = []
    for rid in (RULE_EXTERNAL, RULE_FEEDBACK_EMPTY, RULE_DECISION_TIME, RULE_QUESTIONNAIRE, RULE_CREDEMO, RULE_SHOP_PROFILE):
        col = RULE_FLAG_COLS[rid]
        hit_cols.append(col)
        ids = rule_core_ids.get(rid, set())
        df[col] = df["_core_id_tmp"].apply(lambda x: "X" if x in ids else "")

    # 汇总列：列出命中的规则编号
    def _summary_for_row(row: pd.Series) -> str:
        tags: list[str] = []
        for rid in (RULE_EXTERNAL, RULE_FEEDBACK_EMPTY, RULE_DECISION_TIME, RULE_QUESTIONNAIRE, RULE_CREDEMO, RULE_SHOP_PROFILE):
            col = RULE_FLAG_COLS[rid]
            if str(row.get(col, "")).strip() == "X":
                tags.append(f"规则{rid}")
        return "；".join(tags)

    df["不合格汇总"] = df.apply(_summary_for_row, axis=1)
    df = df.drop(columns=["_core_id_tmp"], errors="ignore")

    out_filename = f"{info_path.stem}_规则标注.csv"
    out_path = PROJECT_ROOT / RAW_SUBDIR / out_filename
    df.to_csv(out_path, index=False, encoding="utf-8-sig")

    # credemo 两类关键预警
    # “名单有”口径：优先使用规则2后的 feedback 用户名单；若未提供则回退到被试信息表名单
    if feedback_users_after_rule2 is not None:
        list_ids = {
            _core_study_id(_map_username(str(u).strip(), temp_mapping))
            for u in feedback_users_after_rule2
            if str(u).strip() and str(u).strip().lower() not in {"none", "nan"}
        }
    else:
        list_ids = {
            s for s in core_ids
            if isinstance(s, str) and s and str(s).strip().lower() not in {"none", "nan"}
        }
    if credemo_check_post:
        both_ok = set(pre_ids) & set(post_ids)
        no_in_list_but_both = sorted(both_ok - list_ids)
        in_list_but_missing_any = sorted(
            {sid for sid in list_ids if sid and (sid not in pre_ids or sid not in post_ids)}
        )
        lines = [
            f"【规则标注表】已输出: {out_path}",
            f"【规则5 关键预警】名单没有，但 pre/post 都有：{len(no_in_list_but_both)} 人。名单: {no_in_list_but_both}",
            f"【规则5 关键预警】名单有，但 pre/post 任一缺：{len(in_list_but_missing_any)} 人。名单: {in_list_but_missing_any}",
        ]
    else:
        no_in_list_but_pre = sorted(set(pre_ids) - list_ids)
        in_list_but_missing_pre = sorted({sid for sid in list_ids if sid and sid not in pre_ids})
        lines = [
            f"【规则标注表】已输出: {out_path}",
            f"【规则5 关键预警·仅pre】名单没有，但 pre 数据中有学号：{len(no_in_list_but_pre)} 人。名单: {no_in_list_but_pre}",
            f"【规则5 关键预警·仅pre】名单有，但 pre 缺：{len(in_list_but_missing_pre)} 人。名单: {in_list_but_missing_pre}",
        ]
    return out_path, lines


def _responses_effectively_empty(s: object) -> bool:
    if pd.isna(s):
        return True
    st = str(s).strip()
    if st == "" or st == "{}":
        return True
    try:
        d = ast.literal_eval(st)
        if not isinstance(d, dict) or len(d) == 0:
            return True
        if all(v == -1 for v in d.values()):
            return True
    except (ValueError, SyntaxError, TypeError):
        return True
    return False


def filter_usernames_questionnaire_incomplete(
    merged_dir: Path,
    mapping: dict[str, str] | None = None,
    excluded_pre_rounds: set[int] | frozenset[int] | None = None,
    pre_excluded_usernames: set[str] | frozenset[str] | None = None,
) -> tuple[set[str], dict[str, list[int]], int]:
    path = merged_dir / QUESTIONNAIRE_FILENAME
    if not path.is_file():
        return set(), {}, 0
    map_used = mapping or {}
    df = _read_merged_csv_for_rules(merged_dir, QUESTIONNAIRE_FILENAME, map_used)
    if df is None:
        return set(), {}, 0
    df = _exclude_usernames_from_df(df, pre_excluded_usernames)
    if "username" not in df.columns or "round" not in df.columns or "responses" not in df.columns:
        return set(), {}, 0
    # 先整表剔除无效 responses 行，再进行用户轮次完整性判断
    invalid_mask = df["responses"].apply(_responses_effectively_empty)
    df_valid = df.loc[~invalid_mask].copy()
    need = set(_get_questionnaire_check_rounds(_normalize_pre_rounds(excluded_pre_rounds)))
    excluded: set[str] = set()
    missing_rounds_by_user: dict[str, list[int]] = {}
    for username, g in df_valid.groupby(df_valid["username"].astype(str).str.strip()):
        valid_rounds: set[int] = set()
        for _, row in g.iterrows():
            try:
                r = int(row["round"])
            except (ValueError, TypeError):
                continue
            valid_rounds.add(r)
        if not need.issubset(valid_rounds):
            excluded.add(username)
            missing = sorted(need - valid_rounds)
            missing_rounds_by_user[username] = missing
    users_checked = int(df_valid["username"].astype(str).str.strip().nunique()) if not df_valid.empty else 0
    return excluded, missing_rounds_by_user, users_checked


def _compute_decision_time_from_operation_log(
    merged_dir: Path,
    mapping: dict[str, str] | None = None,
    pre_excluded_usernames: set[str] | frozenset[str] | None = None,
) -> pd.DataFrame | None:
    path = merged_dir / USER_OPERATION_LOG
    if not path.is_file():
        return None
    map_used = mapping or {}
    df = _read_merged_csv_for_rules(merged_dir, USER_OPERATION_LOG, map_used)
    if df is None:
        return None
    df = _exclude_usernames_from_df(df, pre_excluded_usernames)
    if not {"username", "operation", "round", "time_stamp"}.issubset(df.columns):
        return None
    df = df.copy()
    try:
        df["time_stamp"] = pd.to_datetime(df["time_stamp"], utc=True, format="mixed")
    except (ValueError, TypeError):
        df["time_stamp"] = pd.to_datetime(df["time_stamp"], utc=True, errors="coerce")
    df["round"] = pd.to_numeric(df["round"], errors="coerce")
    df = df[(df["round"] >= ROUND_MIN) & (df["round"] <= ROUND_MAX)].copy()
    start = df[df["operation"] == "start_round"][["username", "round", "time_stamp"]].rename(
        columns={"time_stamp": "start_round_ts"}
    )
    end = df[df["operation"] == "end_round"][["username", "round", "time_stamp"]].rename(
        columns={"time_stamp": "end_round_ts"}
    )
    if start.empty or end.empty:
        return None
    merged = start.merge(end, on=["username", "round"], how="inner")
    if merged.empty:
        return None
    merged["decision_time_minutes"] = (
        (merged["end_round_ts"] - merged["start_round_ts"]).dt.total_seconds() / 60.0
    ).round(2)
    return merged[["username", "round", "decision_time_minutes"]]


def filter_usernames_decision_time(
    merged_dir: Path,
    mapping: dict[str, str] | None = None,
    excluded_pre_rounds: set[int] | frozenset[int] | None = None,
    pre_excluded_usernames: set[str] | frozenset[str] | None = None,
) -> tuple[set[str], dict[str, str], dict[str, str]]:
    dt = _compute_decision_time_from_operation_log(
        merged_dir,
        mapping=mapping,
        pre_excluded_usernames=pre_excluded_usernames,
    )
    if dt is None or dt.empty:
        return set(), {}, {}
    excluded_pre = _normalize_pre_rounds(excluded_pre_rounds)
    if excluded_pre:
        dt = dt[~dt["round"].astype(int).isin(excluded_pre)].copy()
    if dt.empty:
        return set(), {}, {}
    need_rounds = _get_decision_time_check_rounds(excluded_pre)
    if need_rounds:
        dt = dt[dt["round"].astype(int).isin(need_rounds)].copy()
    if dt.empty:
        return set(), {}, {}
    required_rounds = min(REQUIRED_DECISION_ROUNDS, len(need_rounds))
    excluded: set[str] = set()
    details: dict[str, str] = {}
    waiver_log: dict[str, str] = {}
    user_round_products = _load_user_round_product_sets_from_operation_log(
        merged_dir,
        mapping=mapping,
        pre_excluded_usernames=pre_excluded_usernames,
    )
    user_round_feedback = _load_user_round_feedback_text_map(
        merged_dir,
        mapping=mapping,
        pre_excluded_usernames=pre_excluded_usernames,
    )
    for username, g in dt.groupby(dt["username"].astype(str).str.strip()):
        rounds_have = sorted(set(int(r) for r in g["round"].tolist()))
        miss_rounds = [r for r in need_rounds if r not in rounds_have]
        if g["round"].nunique() < required_rounds:
            excluded.add(username)
            details[username] = (
                "【规则3·不合格原因·有效决策轮次不足】"
                f"有效轮数 {g['round'].nunique()} < {required_rounds}；"
                f"缺失 round: {miss_rounds}"
            )
            continue
        low_round_rows = g[g["decision_time_minutes"] < MIN_DECISION_MINUTES_PER_ROUND][
            ["round", "decision_time_minutes"]
        ]
        any_unreasonable_low_round = False
        if not low_round_rows.empty:
            per_round_notes: list[str] = []
            has_changed_low_round = False
            has_unchanged_negative = False
            for _, row in low_round_rows.sort_values("round").iterrows():
                r_cur = int(row["round"])
                m_cur = float(row["decision_time_minutes"])
                prev_r = r_cur - 1
                cur_set = user_round_products.get(username, {}).get(r_cur, set())
                prev_set = user_round_products.get(username, {}).get(prev_r, set())
                changed_state = "unknown"
                if cur_set and prev_set:
                    changed_state = "changed" if cur_set != prev_set else "unchanged"

                prev_fb = user_round_feedback.get(username, {}).get(prev_r, "")
                prev_good = _feedback_has_any_keyword(prev_fb, RULE3_POSITIVE_PERFORMANCE_KEYWORDS)
                prev_bad = _feedback_has_any_keyword(prev_fb, RULE3_NEGATIVE_PERFORMANCE_KEYWORDS)

                if changed_state == "changed":
                    has_changed_low_round = True
                    any_unreasonable_low_round = True
                    if prev_bad:
                        removed_ids = sorted(prev_set - cur_set)
                        added_ids = sorted(cur_set - prev_set)
                        reduced_holdings = bool(removed_ids)
                        per_round_notes.append(
                            f"【换仓+上轮表现差】r{r_cur}={m_cur:.2f}m（低于单轮阈值）："
                            f"本轮与 r{prev_r} 所选产品集合不一致（已换仓）；r{prev_r} 的 feedback 已命中「表现差」类关键词。"
                            f"替换核对：相对上一轮剔除产品 id {removed_ids}，新增 id {added_ids}；"
                            f"是否减少对差表现持仓的延续：{'是（已剔除部分上轮产品）' if reduced_holdings else '否（未剔除上轮产品，可能仅为扩仓）'}。"
                            f"结论：不因换仓而豁免短时，本模式仍判不合格。"
                        )
                    else:
                        per_round_notes.append(
                            f"【换仓+上轮未识别差】r{r_cur}={m_cur:.2f}m（低于单轮阈值）："
                            f"本轮与 r{prev_r} 所选产品集合不一致；r{prev_r} 的 feedback 未命中「表现差」关键词，"
                            f"不足以将极短决策解释为「针对已知劣仓的调整」；仍判不合格。"
                        )
                elif changed_state == "unchanged":
                    if prev_good:
                        per_round_notes.append(
                            f"【未换仓+上轮正向】r{r_cur}={m_cur:.2f}m（低于单轮阈值）："
                            f"本轮与 r{prev_r} 产品集合一致（未换仓）；r{prev_r} feedback 命中「正向表现」类关键词。"
                            f"结论：该轮短时决策视为合理，本条不因此轮判不合格。"
                        )
                    elif prev_bad:
                        has_unchanged_negative = True
                        any_unreasonable_low_round = True
                        per_round_notes.append(
                            f"【未换仓+上轮负向】r{r_cur}={m_cur:.2f}m（低于单轮阈值）："
                            f"本轮与 r{prev_r} 产品集合一致；r{prev_r} feedback 已命中「表现差」类关键词。"
                            f"结论：在已知表现不佳下仍快速维持原仓，本模式判不合格。"
                        )
                    else:
                        any_unreasonable_low_round = True
                        per_round_notes.append(
                            f"【未换仓+上轮表现未识别】r{r_cur}={m_cur:.2f}m（低于单轮阈值）："
                            f"未换仓，但 r{prev_r} feedback 未命中正向表现词，亦未命中负向表现词。"
                            f"结论：无法归类为「正向持有下的合理短决策」；该轮记为风险项，可与其它轮次及平均时长规则一并作用。"
                        )
                else:
                    any_unreasonable_low_round = True
                    per_round_notes.append(
                        f"【明细不足】r{r_cur}={m_cur:.2f}m（低于单轮阈值）："
                        f"缺少 r{prev_r} 或 r{r_cur} 在 operation 中的产品集合，无法判断是否换仓。"
                        f"结论：不适用换仓/未换仓下的豁免叙事；该轮记为风险项，可与其它轮次及平均时长规则一并作用。"
                    )

            if has_changed_low_round or has_unchanged_negative:
                excluded.add(username)
                details[username] = (
                    "【规则3·不合格原因·单轮决策时长过短且行为不合理】"
                    "（逐条前缀【换仓+…】【未换仓+…】等与汇总中「行为模式与判定口径」对应。）"
                    f"存在轮次低于 {MIN_DECISION_MINUTES_PER_ROUND} 分钟；"
                    + "；".join(per_round_notes)
                )
                continue
            if per_round_notes:
                waiver_log[username] = (
                    f"存在轮次低于 {MIN_DECISION_MINUTES_PER_ROUND} 分钟但未判不合格（"
                    f"均为【未换仓+上轮正向】等口径（3）所述合理情形）；"
                    + "；".join(per_round_notes)
                )
        # 若所有「低于单轮阈值」的轮次均为「未换仓且上一轮正向」等合理情形，不因平均时长再剔除
        skip_avg_due_to_reasonable_short_rounds = (
            not low_round_rows.empty and not any_unreasonable_low_round
        )
        avg_m = float(g["decision_time_minutes"].mean())
        if avg_m < MIN_DECISION_AVG_MINUTES and not skip_avg_due_to_reasonable_short_rounds:
            excluded.add(username)
            details[username] = (
                "【规则3·不合格原因·平均决策时长不足】"
                f"平均决策时长 {avg_m:.2f} < {MIN_DECISION_AVG_MINUTES:.2f} 分钟"
            )
        elif avg_m < MIN_DECISION_AVG_MINUTES and skip_avg_due_to_reasonable_short_rounds:
            note = (
                f"平均决策时长 {avg_m:.2f} < {MIN_DECISION_AVG_MINUTES:.2f} 分钟，"
                f"但各低于单轮阈值的轮次均已按口径（3）【未换仓+上轮正向】等处理，不因均值再判不合格"
            )
            if username in waiver_log:
                waiver_log[username] = waiver_log[username] + "；" + note
            else:
                waiver_log[username] = note
    return excluded, details, waiver_log


def collect_excluded_usernames_and_reasons(
    merged_dir: Path,
    enabled_rules: set[int] | frozenset[int] | None = None,
    exclude_feedback_consecutive_3_same: bool | None = None,
    exclude_rule3_decision_time: bool | None = None,
    excluded_pre_rounds: set[int] | frozenset[int] | None = None,
    credemo_check_post_questionnaire: bool = False,
) -> tuple[
    set[str],
    list[str],
    frozenset[int],
    dict[str, int],
    dict[int, set[str]],
    set[str],
    set[str],
    set[str],
    list[dict[str, str]],
]:
    """
    返回 (
      剔除用户名并集,
      说明行列表,
      本次实际启用的规则编号,
      规则2每人空feedback条数,
      每规则命中用户名集合(含预警规则),
      credemo_pre核心学号集合,
      credemo_post核心学号集合,
      规则2后feedback用户名集合,
      人工复核表行
    )。

    未启用规则2 时第四项为 {}。
    enabled_rules 为 None 时使用 DEFAULT_ENABLED_RULE_IDS。
    exclude_feedback_consecutive_3_same 为 None 时使用 EXCLUDE_ON_FEEDBACK_CONSECUTIVE_3_SAME_PRODUCT。
    """
    rules = _normalize_enabled_rules(enabled_rules)
    eff_exclude_c3 = (
        EXCLUDE_ON_FEEDBACK_CONSECUTIVE_3_SAME_PRODUCT
        if exclude_feedback_consecutive_3_same is None
        else exclude_feedback_consecutive_3_same
    )
    eff_exclude_rule3 = False
    eff_pre_rounds = _normalize_pre_rounds(excluded_pre_rounds)
    excluded: set[str] = set()
    per_user_empty_feedback: dict[str, int] = {}
    rule_usernames: dict[int, set[str]] = {rid: set() for rid in _ALL_RULE_IDS}
    users_after_rule2: set[str] = set()
    all_fb_users: set[str] = set()
    manual_review_rows: list[dict[str, str]] = []
    decision_time_details: dict[str, str] = {}
    credemo_missing_pre: list[str] = []
    credemo_missing_post: list[str] = []
    credemo_missing_both: list[str] = []
    shop_profile_details: dict[str, list[str]] = {}
    credemo_mode = (
        "规则5 credemo=检查pre+post问卷"
        if credemo_check_post_questionnaire
        else "规则5 credemo=仅检查pre问卷（配置 credemo_check_post=TRUE 可开启post）"
    )
    reasons: list[str] = [
        f"【本次启用的规则】{sorted(rules)} "
        f"(1=外部名单 2=feedback空 3=decision time 4=问卷 5=credemo预警 6=店铺信息未修改)；"
        f"连续{FEEDBACK_CONSECUTIVE_SAME_PRODUCT_LEN}轮同品额外剔除={'是' if eff_exclude_c3 else '否（仅预警）'}；"
        "规则3决策时长剔除=否（固定仅预警）；"
        f"pre round 不参与判定且从输出移除={sorted(eff_pre_rounds)}；"
        f"{credemo_mode}"
    ]
    reasons.append("")

    pre_ids: set[str] = set()
    post_ids: set[str] = set()
    credemo_active = False
    if RULE_CREDEMO in rules:
        pre_ids, post_ids, credemo_active = _collect_credemo_id_sets(
            PROJECT_ROOT,
            require_post_for_active=credemo_check_post_questionnaire,
        )

    temp_mapping = _combined_temp_mapping(PROJECT_ROOT)
    unmapped_temp_users = _collect_unmapped_temp_usernames_in_merged(merged_dir, temp_mapping)
    if unmapped_temp_users:
        excluded |= set(unmapped_temp_users)
        reasons.append(
            f"【临时账号清理】未映射成功的临时账号已直接剔除：{len(unmapped_temp_users)} 人。"
            f"【不合格原因】username 形如临时账号但未出现在 临时账号使用.csv 映射表中。名单: {sorted(unmapped_temp_users)}"
        )
        reasons.append("")

    if RULE_EXTERNAL in rules:
        ext = filter_usernames_external_disqualified(PROJECT_ROOT)
        if ext:
            excluded |= ext
            rule_usernames[RULE_EXTERNAL] |= set(ext)
            reasons.append(
                f"【规则1 外部不合格名单】共 {len(ext)} 个用户。"
                f"【不合格原因】命中各组 raw/operation/实验不合格样本记录.csv。名单: {sorted(ext)}"
            )
        reasons.append("")

    if RULE_FEEDBACK_EMPTY in rules:
        empty_fb_usernames, _n_empty_feedback, per_user_empty_feedback, fb_stats = filter_usernames_empty_feedback(
            merged_dir,
            mapping=temp_mapping,
            excluded_pre_rounds=eff_pre_rounds,
            pre_excluded_usernames=excluded,
        )
        fb_df = _read_merged_csv_for_rules(merged_dir, USER_FEEDBACK_FILENAME, temp_mapping)
        if fb_df is not None:
            fb_df = _exclude_usernames_from_df(fb_df, excluded)
        if fb_df is not None and "username" in fb_df.columns:
            all_fb_users = set(fb_df["username"].astype(str).str.strip().dropna().unique())
            users_after_rule2 = all_fb_users - excluded - set(empty_fb_usernames)
        _print_rule2_feedback_summary(fb_stats)
        c3 = fb_stats.get("consecutive_3_same_product")
        if isinstance(c3, dict):
            k = FEEDBACK_CONSECUTIVE_SAME_PRODUCT_LEN
            if c3.get("status") == "no_round":
                reasons.append(f"【规则2 预警 连续{k}轮同品】表中无 round 列，跳过。")
            elif c3.get("status") == "ok":
                items3 = c3.get("items") or []
                flagged3 = set(c3.get("usernames") or set())
                waived_items3 = c3.get("waived_items") or []
                waived_users3 = set(c3.get("waived_usernames") or set())
                details3 = c3.get("details") or {}
                if items3:
                    reasons.append(
                        f"【规则2 预警 连续{k}轮同品】以下用户在相邻整数 round 上提取的产品集合连续 {k} 轮一致（请人工复核）："
                    )
                    for it in items3:
                        reasons.append(f"  {it}")
                    if waived_items3:
                        reasons.append("【规则2 解除预警 连续同品】以下情况判断为合理继续持有：")
                        for it in waived_items3:
                            reasons.append(f"  {it}")
                    for uname in sorted(details3):
                        entries = details3.get(uname) or []
                        for entry in entries:
                            rounds = entry.get("rounds") or []
                            label = str(entry.get("product_label", ""))
                            rounds_text = "、".join(str(r) for r in rounds)
                            reasons.append(
                                f"  【规则2 反馈明细】{uname} | 产品集合「{label}」| rounds: {rounds_text}"
                            )
                            fb_by_round = entry.get("feedback_by_round") or {}
                            for r in rounds:
                                fb_text = str(fb_by_round.get(r, "")).strip()
                                if not fb_text:
                                    fb_text = "（该轮无可用 feedback 文本）"
                                reasons.append(f"    round {r} feedback: {fb_text}")
                else:
                    reasons.append(
                        f"【规则2 预警 连续{k}轮同品】未发现连续 {k} 轮产品集合一致的情况。"
                    )
                    if waived_items3:
                        reasons.append("【规则2 解除预警 连续同品】以下情况判断为合理继续持有：")
                        for it in waived_items3:
                            reasons.append(f"  {it}")
                if eff_exclude_c3 and flagged3:
                    excluded |= flagged3
                    reasons.append(
                        f"【规则2 连续{k}轮同品 · 已剔除】开关已开启，已将 {len(flagged3)} 个用户并入剔除名单："
                        f"{sorted(flagged3)}"
                    )
                    reasons.append(
                        f"  【不合格原因·连续同品】相邻整数 round 上从 feedback 提取的产品集合连续 {k} 轮一致，且已开启额外剔除开关。"
                    )
                elif flagged3 and not eff_exclude_c3:
                    reasons.append(
                        f"【规则2 连续{k}轮同品 · 未剔除】上述预警用户未剔除；"
                        f"若需剔除请设 EXCLUDE_ON_FEEDBACK_CONSECUTIVE_3_SAME_PRODUCT=True 或使用 "
                        f"--exclude-feedback-consecutive-3-same。"
                    )
                if waived_users3:
                    reasons.append(
                        f"【规则2 连续{k}轮同品 · 解除预警】共 {len(waived_users3)} 人：{sorted(waived_users3)}"
                    )
        if empty_fb_usernames:
            excluded |= empty_fb_usernames
            rule_usernames[RULE_FEEDBACK_EMPTY] |= set(empty_fb_usernames)
            loc_lines = fb_stats.get("empty_feedback_locations_by_user") or {}
            reasons.append(
                f"【规则2 feedback 为空】共 {len(empty_fb_usernames)} 个用户。"
                f"【不合格原因】users_feedback 中该用户至少一行 feedback 为空或缺失（判定已排除 pre round 行）。"
            )
            count_buckets: dict[int, list[str]] = {}
            for u, c in per_user_empty_feedback.items():
                count_buckets.setdefault(int(c), []).append(u)
            for miss_cnt in sorted(count_buckets.keys(), reverse=True):
                users = sorted(count_buckets[miss_cnt])
                reasons.append(
                    f"  缺 {miss_cnt} 条：{len(users)} 人。名单: {users}"
                )
            all_round_empty_users: list[str] = []
            partial_empty_users: list[str] = []
            for u in sorted(empty_fb_usernames):
                hint = str(loc_lines.get(u, "（未能标注具体行/round）"))
                if hint.startswith("每一round缺的都为空"):
                    all_round_empty_users.append(u)
                else:
                    partial_empty_users.append(u)

            # “每一round都为空”的汇总不再单独输出，避免与总览重复
            for u in partial_empty_users:
                hint = loc_lines.get(u, "（未能标注具体行/round）")
                reasons.append(f"  【规则2 明细】{u}：【不合格原因·feedback】{hint}")
        reasons.append("")

    # 规则3：decision time（基于规则1+2后用户）
    users_after_rule12 = users_after_rule2 if users_after_rule2 else (all_fb_users - excluded if all_fb_users else set())
    if RULE_DECISION_TIME in rules:
        dt_need_rounds = _get_decision_time_check_rounds(eff_pre_rounds)
        reasons.append("【规则3 decision time · 行为模式与判定口径】")
        reasons.append(
            "  本规则只看「检查轮次」内由 operation 日志配对的单轮决策时长（分钟），并与「相对上一轮是否更换产品集合」、"
            "「上一轮 feedback 是否命中配置中的正向/负向表现关键词」交叉分类。阈值："
            f"每轮 ≥{MIN_DECISION_MINUTES_PER_ROUND} 分钟、平均 ≥{MIN_DECISION_AVG_MINUTES} 分钟（且有效轮数满足要求）。"
        )
        reasons.append(
            "  （1）换仓 + 上一轮已识别表现差：反馈已表明上轮持仓表现不佳，若本轮仍远低于单轮时长阈值，"
            "不因「已换仓」而豁免；明细中附「替换核对」（相对上一轮剔除/新增的产品 id，以及是否仍延续部分差表现持仓）。"
        )
        reasons.append(
            "  （2）换仓 + 上一轮未识别表现差：未见「因已知劣仓而调整」的文本依据，短时完成仍视为行为不合理。"
        )
        reasons.append(
            "  （3）未换仓 + 上一轮正向表现：继续持有且反馈偏正向时，该轮短时决策可接受；"
            "若所有「低于单轮阈值」的轮次均落入此类，则即使平均时长不足也不再据此追加剔除。"
        )
        reasons.append(
            "  （4）未换仓 + 上一轮负向表现：已知表现差仍快速维持原仓，视为不合理，单轮即倾向不合格。"
        )
        reasons.append(
            "  （5）无法判断是否换仓（缺上一轮或本轮产品明细），或未换仓但上一轮表现既未命中正向也未命中负向："
            "不足以支撑「合理短决策」叙事；该轮仍记为「信息不足下的风险项」，可与其它轮次及平均时长规则一并作用。"
        )
        reasons.append("")
        dt_ex, dt_detail, dt_waiver_log = filter_usernames_decision_time(
            merged_dir,
            mapping=temp_mapping,
            excluded_pre_rounds=eff_pre_rounds,
            pre_excluded_usernames=excluded,
        )
        if users_after_rule12:
            dt_ex = set(dt_ex) & users_after_rule12
            dt_waiver_log = {
                u: t for u, t in dt_waiver_log.items() if u in users_after_rule12
            }
        if dt_waiver_log:
            reasons.append(
                "【规则3 decision time · 合理情形说明】以下用户存在低于单轮阈值的轮次，但按上文口径（3）"
                "或「仅平均时长不足、且各过短轮均已按（3）处理」而未计入不合格；详见逐条。"
            )
            for u in sorted(dt_waiver_log):
                reasons.append(f"  {u}：{dt_waiver_log[u]}")
        if dt_ex:
            rule_usernames[RULE_DECISION_TIME] |= set(dt_ex)
            decision_time_details = dict(dt_detail)
            reasons.append(
                f"【规则3 decision time 预警】共 {len(dt_ex)} 个用户（检查轮次 {dt_need_rounds}；要求每轮 ≥{MIN_DECISION_MINUTES_PER_ROUND} 分钟、"
                f"平均 ≥{MIN_DECISION_AVG_MINUTES} 分钟、有效轮数 ≥{min(REQUIRED_DECISION_ROUNDS, len(dt_need_rounds))}），"
                f"当前固定仅预警不剔除。名单: {sorted(dt_ex)}"
            )
            reasons.append(
                "  【不合格原因说明】逐条见下（仅作预警与复核，不据此自动剔除）；"
                "类别含义：（a）轮次不足、（b）单轮过短且模式见行为口径、（c）平均时长不足。"
            )
            for u in sorted(dt_ex):
                d = dt_detail.get(u)
                if d:
                    reasons.append(f"  【规则3 明细】{u}：{d}")
        elif _compute_decision_time_from_operation_log(
            merged_dir,
            mapping=temp_mapping,
            pre_excluded_usernames=excluded,
        ) is None:
            reasons.append(
                "【规则3 decision time】未启用（无 user_operation_log 或缺少 start_round/end_round 可配对）"
            )
        reasons.append("")

    if RULE_QUESTIONNAIRE in rules:
        q_ex, q_missing_by_user, q_users_checked = filter_usernames_questionnaire_incomplete(
            merged_dir,
            mapping=temp_mapping,
            excluded_pre_rounds=eff_pre_rounds,
            pre_excluded_usernames=excluded,
        )
        checked_rounds = _get_questionnaire_check_rounds(eff_pre_rounds)
        checked_rounds_text = "、".join(str(r) for r in checked_rounds) if checked_rounds else "（空）"
        users_after_rule123 = users_after_rule12 - excluded if users_after_rule12 else set()
        if users_after_rule123:
            q_ex = set(q_ex) & users_after_rule123
        reasons.append(f"【规则4 问卷完整性检查】去除无效 responses 行后，参与检查用户 {q_users_checked} 人。")
        if q_ex:
            excluded |= q_ex
            rule_usernames[RULE_QUESTIONNAIRE] |= set(q_ex)
            reasons.append(
                f"【规则4 问卷不完整或 responses 无效】共 {len(q_ex)} 个用户（要求检查轮次 {checked_rounds_text} 各有至少一条有效 responses）。"
                f"【不合格原因】见下行「【不合格原因·问卷】」。名单: {sorted(q_ex)}"
            )
            for u in sorted(q_ex):
                miss = q_missing_by_user.get(u, [])
                if miss:
                    rounds_text = "、".join(str(r) for r in miss)
                    reasons.append(
                        f"  【规则4 明细】{u}：【不合格原因·问卷】检查轮次 {checked_rounds_text} 中 round {rounds_text} 无有效 responses。"
                    )
        else:
            reasons.append(
                f"【规则4 结果】未发现缺失轮次用户（0 人，检查轮次 {checked_rounds_text} 均有有效 responses）。"
            )
        reasons.append("")

    # 规则5：credemo 预警（基于规则1+2+3之后，且规则4应用后的样本）
    if RULE_CREDEMO in rules:
        if not credemo_active:
            if credemo_check_post_questionnaire:
                reasons.append("【规则5 credemo】未启用（须同时存在 pre 与 post 类 csv，或 credemo 目录不存在）")
            else:
                reasons.append("【规则5 credemo】未启用（未找到 pre 类 csv，或 credemo 目录不存在）")
        elif RULE_FEEDBACK_EMPTY not in rules:
            reasons.append("【规则5 预警】当前未启用规则2，按你的要求跳过规则5（仅在规则2后检查）。")
        else:
            users_after_rule1234 = users_after_rule12 - excluded if users_after_rule12 else set()
            cstat = analyze_credemo_missing_after_prior_rules(
                merged_dir=merged_dir,
                pre_ids=pre_ids,
                post_ids=post_ids,
                active=credemo_active,
                users_after_rule2=users_after_rule1234,
                check_post_questionnaire=credemo_check_post_questionnaire,
            )
            post_hint = (
                "检查 pre+post"
                if credemo_check_post_questionnaire
                else "仅检查 pre（未检查 post；配置 credemo_check_post=TRUE 可开启）"
            )
            reasons.append(
                f"【规则5 预警（{post_hint}；基于规则1/2/3后样本）】"
                f"检查用户 {cstat['checked_users']} 人；规则5仅预警，不参与剔除。"
            )
            miss_pre = cstat["missing_pre"]
            miss_post = cstat["missing_post"]
            miss_both = cstat["missing_both"]
            credemo_missing_pre = list(miss_pre)
            credemo_missing_post = list(miss_post)
            credemo_missing_both = list(miss_both)
            rule_usernames[RULE_CREDEMO] |= set(miss_pre) | set(miss_post) | set(miss_both)
            reasons.append(f"  缺 pre：{len(miss_pre)} 人。名单: {miss_pre}")
            if credemo_check_post_questionnaire:
                reasons.append(f"  缺 post：{len(miss_post)} 人。名单: {miss_post}")
                reasons.append(f"  pre/post 都缺：{len(miss_both)} 人。名单: {miss_both}")
        reasons.append("")

    if RULE_SHOP_PROFILE in rules:
        shop_ex, shop_profile_details = filter_usernames_default_shop_profile(
            merged_dir,
            mapping=temp_mapping,
            pre_excluded_usernames=excluded,
        )
        # 与规则3/4/5 一致：仅对「规则1+2 后仍在 feedback 样本中」的用户判规则6（剔除无 feedback / feedback 不合格者）
        users_for_rule6 = (users_after_rule12 - excluded) if users_after_rule12 else set()
        if users_for_rule6:
            shop_ex = set(shop_ex) & users_for_rule6
            shop_profile_details = {u: shop_profile_details[u] for u in shop_ex if u in shop_profile_details}
        else:
            shop_ex = set()
            shop_profile_details = {}
        if shop_ex:
            excluded |= shop_ex
            rule_usernames[RULE_SHOP_PROFILE] |= set(shop_ex)
            reasons.append(
                f"【规则6 店铺名称/描述未修改】共 {len(shop_ex)} 个用户（仅统计 users_feedback 中有记录且已通过规则1+2 者）。"
                "【不合格原因】users_shop.csv 中店铺名称仍为“<seller_username>的商店”或 description 仍为默认文案。"
            )
            for username in sorted(shop_ex):
                detail_text = "；".join(shop_profile_details.get(username, []))
                reasons.append(f"  【规则6 明细】{username}：【不合格原因·店铺信息】{detail_text}")
        else:
            reasons.append("【规则6 店铺名称/描述未修改】未发现命中用户（0 人）。")
        reasons.append("")

    while reasons and reasons[-1] == "":
        reasons.pop()

    manual_review_rows = _build_manual_review_rows(
        rule_usernames=rule_usernames,
        decision_time_details=decision_time_details,
        credemo_missing_pre=credemo_missing_pre,
        credemo_missing_post=credemo_missing_post,
        credemo_missing_both=credemo_missing_both,
        shop_profile_details=shop_profile_details,
    )

    return (
        excluded,
        reasons,
        rules,
        per_user_empty_feedback,
        rule_usernames,
        pre_ids,
        post_ids,
        users_after_rule2,
        manual_review_rows,
    )


def write_quality_report(out_dir: Path, per_user_empty_count: dict[str, int]) -> Path:
    report_path = out_dir / QUALITY_REPORT_FILENAME
    rows = [{"username": u, "empty_feedback_count": c} for u, c in sorted(per_user_empty_count.items())]
    pd.DataFrame(rows).to_csv(report_path, index=False, encoding="utf-8-sig")
    return report_path


def add_group_column(df: pd.DataFrame, control_usernames: set[str], onlyai_usernames: set[str]) -> pd.DataFrame:
    """
    Group 判定（优先级从高到低）：
    1) 用户名 strip 后首字符 a/A → OnlyAI，b/B → Control（学号/账号命名约定）
    2) 非 a/b 开头：若 username ∈ control_usernames（raw Control 臂 users_feedback）→ Control，否则 OnlyAI

    onlyai_usernames 保留参数供调用方兼容，当前未参与赋值。
    """
    if "username" not in df.columns:
        return df
    out = df.copy()
    u = out["username"].astype(str).str.strip()
    first = u.str.slice(0, 1).str.lower()

    out["Group"] = "OnlyAI"
    out.loc[first == "b", "Group"] = "Control"
    out.loc[first == "a", "Group"] = "OnlyAI"
    # 非 a/b 开头：回退到 raw Control 名单（历史合并口径）
    non_ab = ~first.isin(["a", "b"])
    out.loc[non_ab & u.isin(control_usernames), "Group"] = "Control"

    out["LetterGroup"] = u.str.slice(0, 1).str.upper().replace("", pd.NA)
    return out


def main(
    enabled_rules: set[int] | frozenset[int] | None = None,
    exclude_feedback_consecutive_3_same: bool | None = None,
    exclude_rule3_decision_time: bool | None = None,
    excluded_pre_rounds: set[int] | frozenset[int] | None = None,
    project_root: Path | None = None,
    control_dirname: str | None = None,
    onlyai_dirname: str | None = None,
    combined_dirname: str | None = None,
    apply_exclusion_to_outputs: bool = False,
    credemo_check_post_questionnaire: bool = False,
) -> None:
    if project_root is not None:
        configure_runtime(
            project_root=project_root,
            control_dirname=control_dirname,
            onlyai_dirname=onlyai_dirname,
            combined_dirname=combined_dirname,
        )
    merged_dir = PROJECT_ROOT / INPUT_MERGED_DIR
    out_dir = PROJECT_ROOT / OUTPUT_FILTERED_DIR
    if not merged_dir.is_dir():
        raise FileNotFoundError(f"合并数据目录不存在: {merged_dir}")
    temp_mapping = _combined_temp_mapping(PROJECT_ROOT)

    (
        excluded,
        reason_lines,
        rules,
        per_user_empty_count,
        rule_usernames,
        pre_ids,
        post_ids,
        feedback_users_after_rule2,
        manual_review_rows,
    ) = collect_excluded_usernames_and_reasons(
        merged_dir,
        enabled_rules,
        exclude_feedback_consecutive_3_same=exclude_feedback_consecutive_3_same,
        exclude_rule3_decision_time=exclude_rule3_decision_time,
        excluded_pre_rounds=excluded_pre_rounds,
        credemo_check_post_questionnaire=credemo_check_post_questionnaire,
    )
    control_usernames, onlyai_usernames = _get_control_onlyai_username_sets()
    print(
        "[Group] 首字符 a/A→OnlyAI，b/B→Control；非 a/b 时回退 raw Control users_feedback 名单→Control，否则 OnlyAI。"
    )

    out_dir.mkdir(parents=True, exist_ok=True)
    _eff_c3 = (
        EXCLUDE_ON_FEEDBACK_CONSECUTIVE_3_SAME_PRODUCT
        if exclude_feedback_consecutive_3_same is None
        else exclude_feedback_consecutive_3_same
    )
    _eff_rule3 = False
    _eff_pre_rounds = _normalize_pre_rounds(excluded_pre_rounds)
    print(f"[筛选] 启用规则: {sorted(rules)}")
    print(
        f"[筛选] 连续{FEEDBACK_CONSECUTIVE_SAME_PRODUCT_LEN}轮同品（feedback 提取）额外剔除: "
        f"{'是' if _eff_c3 else '否（仅写入 filter_summary 预警）'}"
    )
    print(
        f"[筛选] 规则3 decision time: "
        "预警模式（固定不剔除）"
    )
    print(f"[筛选] pre round 不参与判定且从输出移除: {sorted(_eff_pre_rounds)}")
    print(
        f"[筛选] 规则5 credemo post问卷检查: "
        f"{'开启' if credemo_check_post_questionnaire else '关闭（仅 pre）'}"
    )
    quality_report_path = write_quality_report(out_dir, per_user_empty_count)
    manual_review_path = write_manual_review_queue(out_dir, manual_review_rows)
    mark_out_path, mark_lines = export_rule_flags_to_participant_sheet(
        rule_usernames,
        pre_ids,
        post_ids,
        feedback_users_after_rule2=feedback_users_after_rule2,
        credemo_check_post=credemo_check_post_questionnaire,
    )
    if mark_lines:
        reason_lines.extend(mark_lines)
    if RULE_DECISION_TIME in rules:
        r3_names = sorted(rule_usernames.get(RULE_DECISION_TIME, set()))
        reason_lines.append("")
        reason_lines.append("【规则3 建议删除名单（汇总）】")
        if r3_names:
            reason_lines.append(
                f"  共 {len(r3_names)} 人：在规则1+2 后的检查样本上，因决策轮次、单轮或平均决策时长、"
                f"或与换仓/feedback 交叉的行为模式命中规则3，建议在后续分析中删除。"
                "（规则3现固定仅预警，合并输出 csv 不会因该规则自动删人；如需删除，请按本名单人工处理。）"
            )
            reason_lines.append(f"  名单: {r3_names}")
        else:
            reason_lines.append(
                "  共 0 人：在规则1+2 后的检查样本上，无人命中规则3 剔除条件。"
            )
    summary_path = out_dir / SUMMARY_FILENAME
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write("筛选说明（Filter Summary）\n")
        f.write("=" * 60 + "\n")
        f.write(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write(f"quality 检查明细: {quality_report_path.name}\n\n")
        f.write(f"因不符合条件被整体剔除的 username：共 {len(excluded)} 个\n")
        f.write(f"名单: {sorted(excluded)}\n\n")
        f.write("剔除原因说明（含【不合格原因】标签，便于检索）:\n")
        for line in reason_lines:
            if line == "":
                f.write("\n")
            else:
                f.write("  " + line + "\n")
        if not reason_lines:
            f.write("  （无）\n")
        f.write("\n")
        f.write("执行顺序小注:\n")
        f.write("  - 规则1与规则2：并行独立检查，剔除并集作为后续基础样本。\n")
        f.write(
            "  - 规则3（decision time）：在规则1+2之后的样本上执行；"
            "「换仓/未换仓 × 上一轮正负面反馈 × 单轮时长」的判定口径见上文「【规则3 decision time · 行为模式与判定口径】」。\n"
        )
        f.write("  - 规则4（questionnaire）：在规则1+2 后样本上执行；规则3仅预警，不参与自动剔除。\n")
        f.write(
            "  - 规则5（credemo）：在规则1+2(+4)之后样本上仅做预警，不参与剔除；"
            "默认仅核对 pre 问卷学号，配置 credemo_check_post=TRUE 或命令行 --credemo-check-post 时同时核对 post。\n"
        )
        if RULE_DECISION_TIME in rules:
            f.write(
                "  - 文末「【规则3 建议删除名单（汇总）】」：可复制 username 列表（与规则3 命中集一致）。\n"
            )
    print(f"已写入: {summary_path}")
    print(f"已写入: {quality_report_path}")
    print(f"已写入: {manual_review_path}")
    if mark_out_path is not None:
        print(f"已写入: {mark_out_path}")

    csv_files = sorted(merged_dir.glob("*.csv"))
    valid_final: set[str] = set()
    for p in csv_files:
        if p.name in SKIP_OUTPUT_FILENAMES:
            print(f"  跳过不输出: {p.name}")
            continue
        df = _read_csv_any_encoding(p)
        df = _apply_username_mapping_with_temp_cleanup(df, temp_mapping)
        if apply_exclusion_to_outputs and "username" in df.columns and excluded:
            before = len(df)
            df = df[~df["username"].astype(str).str.strip().isin(excluded)].copy()
            after = len(df)
            if before != after:
                print(f"  {p.name}: 剔除 {before - after} 行（username 在排除名单）")
        if _eff_pre_rounds:
            df, n_pre_removed = _drop_pre_round_rows(df, _eff_pre_rounds)
            if n_pre_removed > 0:
                print(f"  {p.name}: 移除 {n_pre_removed} 行（pre round={sorted(_eff_pre_rounds)}）")
        df = add_group_column(df, control_usernames, onlyai_usernames)
        if "username" in df.columns:
            valid_final |= set(df["username"].astype(str).str.strip().dropna().unique())
        out_path = out_dir / p.name
        # 统一为 UTF-8 BOM，便于 Excel 直接打开中文不乱码。
        df.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"  已写出: {out_path.name}")

    # 最终名单始终以 rules 计算结果为准，而不是由当前输出文件反推。
    fb_source = merged_dir / USER_FEEDBACK_FILENAME
    if fb_source.is_file():
        dff = _read_csv_any_encoding(fb_source)
        dff = _apply_username_mapping_with_temp_cleanup(dff, temp_mapping)
        if "username" in dff.columns and excluded:
            dff = dff[~dff["username"].astype(str).str.strip().isin(excluded)].copy()
        if _eff_pre_rounds:
            dff, _ = _drop_pre_round_rows(dff, _eff_pre_rounds)
        if "username" in dff.columns:
            valid_final = set(dff["username"].astype(str).str.strip().dropna().unique())

    user_lists_dir = out_dir / "user_lists"
    user_lists_dir.mkdir(parents=True, exist_ok=True)
    if valid_final:
        sorted_users = sorted(valid_final)
        pd.DataFrame({"username": sorted_users}).to_csv(
            user_lists_dir / "valid_usernames_final.csv", index=False, encoding="utf-8-sig"
        )
        pd.DataFrame({"username": sorted_users}).to_csv(
            user_lists_dir / "valid_usernames_feedback.csv", index=False, encoding="utf-8-sig"
        )
        print(
            f"  已写出: valid_usernames_final.csv / valid_usernames_feedback.csv "
            f"（各 {len(sorted_users)} 人）"
        )
    print("完成。")


if __name__ == "__main__":
    cli_rules, cli_exclude_c3, cli_exclude_rule3, cli_pre_rounds, cli_credemo_check_post = _parse_cli_args()
    main(
        enabled_rules=cli_rules,
        exclude_feedback_consecutive_3_same=True if cli_exclude_c3 else None,
        exclude_rule3_decision_time=True if cli_exclude_rule3 else None,
        excluded_pre_rounds=cli_pre_rounds,
        credemo_check_post_questionnaire=cli_credemo_check_post,
    )
