#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""从 key-value 配置表解析 output_project_dir，打印 4_Final_Outputs 路径；可加 --open 在资源管理器/Finder 中打开。"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pandas as pd


def main() -> int:
    args = [a for a in sys.argv[1:] if a != "--open"]
    if not args:
        print("用法: print_final_outputs_dir.py <配置.xlsx|配置.csv> [--open]", file=sys.stderr)
        return 2
    cfg = Path(args[0]).resolve()
    if not cfg.is_file():
        print(f"找不到配置: {cfg}", file=sys.stderr)
        return 1
    base = cfg.parent
    if cfg.suffix.lower() in {".xlsx", ".xls"}:
        df = pd.read_excel(cfg)
    else:
        df = pd.read_csv(cfg)
    kv = {str(r["key"]).strip(): r["value"] for _, r in df.iterrows()}
    rel = Path(str(kv.get("output_project_dir", "")).strip())
    out_dir = (base / rel).resolve() / "4_Final_Outputs"
    print(out_dir)
    if "--open" in sys.argv:
        if os.name == "nt":
            os.startfile(str(out_dir))  # type: ignore[attr-defined]
        else:
            subprocess.run(["open", str(out_dir)], check=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
