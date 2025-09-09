#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, re, glob, sys, math, time, traceback
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pymysql

DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "lajifenshao")
DB_NAME = os.getenv("DB_NAME", "data")
TABLE_NAME = "wuxihuilian"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
VARIABLES_ROOT = os.path.join(SCRIPT_DIR, "variables_out")
POINTS_CSV = os.path.join(SCRIPT_DIR, "无锡惠联垃圾dcs点表3.21.csv")
DATA_ENCODINGS = ["gbk", "utf-8", "utf-8-sig"]
BATCH_SIZE = 10_000

def log(msg: str):
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

def read_points_mapping(points_csv_path: str) -> Tuple[Dict[str, str], Dict[str, str]]:
    if not os.path.exists(points_csv_path):
        raise FileNotFoundError(points_csv_path)
    df = pd.read_csv(points_csv_path, encoding="gbk")
    def norm(s: str) -> str:
        return str(s).strip().replace("\ufeff", "")
    cols = {norm(c): c for c in df.columns}
    name_col = cols.get("名称"); desc_col = cols.get("描述")
    if not name_col or not desc_col:
        raise ValueError(f"点表缺列，需包含‘名称’和‘描述’，实际={list(df.columns)}")
    name_to_desc, desc_to_name = {}, {}
    mre = re.compile(r"\[(.*?)\]")
    for _, r in df.iterrows():
        raw_name = str(r[name_col]).strip(); desc = str(r[desc_col]).strip()
        if not raw_name or not desc: continue
        m = mre.search(raw_name)
        if not m: continue
        name = m.group(1).strip()
        name_to_desc.setdefault(name, desc)
        desc_to_name[desc] = name
    if not desc_to_name:
        raise ValueError("点表未得到任何 映射(描述→名称)")
    log(f"点表加载：{len(desc_to_name)} 条映射(描述→名称)")
    return name_to_desc, desc_to_name

def try_read_dataset_csv(path: str) -> Optional[pd.DataFrame]:
    """
    尝试以多编码 + 分隔符自动嗅探读取 CSV。
    目标：
      - 至少两列（time + 值列）
      - 第一列重命名为 'time'
    """
    import io
    import codecs
    import csv

    # 快速空文件/太短文件检查
    if os.path.getsize(path) == 0:
        log(f"[跳过] 空文件：{path}")
        return None

    # 先读前几行做“肉眼”嗅探，并在失败时打日志辅助定位
    def read_head_bytes(fp, n=4096):
        with open(fp, "rb") as f:
            return f.read(n)

    head_bytes = read_head_bytes(path)
    # 编码尝试顺序：更可能正确的放前面
    encodings = ["utf-8-sig", "utf-8", "gbk", "gb18030"]

    # 备选分隔符集合（含全角逗号）
    candidate_seps = [None, ",", "\t", ";", "|", "，"]  # None = 让 pandas 自动推断（engine='python'）
    # pandas 的 engine='c' 不支持 sep=None 推断，这里统一用 python 引擎
    # on_bad_lines='skip' 兼容脏行

    for enc in encodings:
        try:
            # 先用该编码解码头部做启发式分隔符判断
            head_text = head_bytes.decode(enc, errors="replace")
            first_line = head_text.splitlines()[0] if head_text.splitlines() else ""
            # 快速估计哪个分隔符出现频次更像表头
            freq = {sep: (first_line.count(sep) if sep is not None else -1) for sep in [",", "\t", ";", "|", "，"]}
            # 选择出现次数最多的（None 让位给最大频次的 sep）
            likely_sep = max(freq, key=freq.get) if max(freq.values()) > 0 else None

            # 优先尝试 likely_sep，再尝试其它
            seps = [likely_sep] + [s for s in candidate_seps if s != likely_sep]

            for sep in seps:
                try:
                    df = pd.read_csv(
                        path,
                        encoding=enc,
                        sep=sep,
                        engine="python",
                        on_bad_lines="skip",
                        dtype=str,             # 先以字符串读入，后续再转
                        keep_default_na=False, # 防止 'NULL' 被误当 NaN
                    )
                    # 规范列名
                    df.columns = [str(c).strip().replace("\ufeff", "") for c in df.columns]

                    # 跳过只有 0/1 行或 1 列的
                    if df.shape[1] < 2:
                        continue

                    # 处理表头不是 'time' 的情况：重命名第一列
                    first_col = df.columns[0]
                    if str(first_col).strip().lower() != "time":
                        df.rename(columns={first_col: "time"}, inplace=True)

                    # 清理空行
                    df = df[~df["time"].astype(str).str.strip().eq("")]

                    # 解析时间（去掉废弃参数 infer_datetime_format）
                    df["time"] = pd.to_datetime(df["time"], errors="coerce")
                    df = df.dropna(subset=["time"])

                    # 取第二列作为值列
                    value_col = df.columns[1]
                    # 尝试把值列转为数值（失败用 NaN，再用 None 传 DB）
                    df[value_col] = pd.to_numeric(df[value_col].str.replace(",", "").str.strip(), errors="coerce")

                    # 成功条件：至少两列+至少一行有效数据
                    if df.shape[1] >= 2 and len(df) > 0:
                        # 关键调试日志（只在 sep 推断有意义时打印一次）
                        log(f"[解析] {os.path.basename(path)} | 编码={enc} | 分隔符={'AUTO' if sep is None else repr(sep)} | 列数={df.shape[1]}")
                        return df

                except Exception:
                    # 该 sep 下失败，换 sep
                    continue

            # 该编码失败，换编码
        except Exception:
            continue

    # 兜底：输出头两行帮助排查
    try:
        sample_text = head_bytes.decode("utf-8", errors="replace")
    except Exception:
        sample_text = str(head_bytes[:200])
    log(f"[跳过] 无法正确解析：{path}\n首行预览：{sample_text.splitlines()[0:2]}")
    return None



def iter_all_dataset_files(root_dir: str) -> List[str]:
    if not os.path.isdir(root_dir): return []
    return [p for p in glob.glob(os.path.join(root_dir, "**", "*.csv"), recursive=True)]

def sanitize_column_name(name: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_]", "_", name)
    if re.match(r"^\d", safe): safe = "_" + safe
    return safe

def ensure_ts_and_columns(conn, table: str, needed_cols: List[str]):
    """确保存在 ts 列( DATETIME(6) ) 与唯一索引；并添加缺失变量列(DOUBLE)。不修改 id 主键。"""
    with conn.cursor() as cur:
        # 表必须已存在
        cur.execute(f"SHOW COLUMNS FROM `{table}`;")
        cols = {row[0]: row for row in cur.fetchall()}
        if "ts" not in cols:
            cur.execute(f"ALTER TABLE `{table}` ADD COLUMN `ts` DATETIME(6) NULL;")
            # 填完列后再建唯一索引
        # 确保 ts 唯一索引（命名 idx_ts_unique）
        cur.execute(f"SHOW INDEX FROM `{table}`;")
        idx_rows = cur.fetchall()
        has_ts_unique = False
        for r in idx_rows:
            # rows: Table, Non_unique, Key_name, Seq_in_index, Column_name, ...
            non_unique = r[1]; key_name = r[2]; col_name = r[4]
            if col_name == "ts" and non_unique == 0:
                has_ts_unique = True; break
        if not has_ts_unique:
            # 若已有重复 ts，需要你先清洗；这里直接尝试创建唯一索引
            cur.execute(f"ALTER TABLE `{table}` ADD UNIQUE KEY `idx_ts_unique` (`ts`);")

        # 添加缺失变量列
        cur.execute(f"SHOW COLUMNS FROM `{table}`;")
        existing = {row[0] for row in cur.fetchall()}
        for col in needed_cols:
            if col not in existing:
                cur.execute(f"ALTER TABLE `{table}` ADD COLUMN `{col}` DOUBLE NULL;")
    conn.commit()

def upsert_batch(conn, table: str, ts_vals: List[datetime], col: str, values: List[Optional[float]]):
    if not ts_vals: return
    params = []
    for ts, v in zip(ts_vals, values):
        if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
            params.append((ts, None))
        else:
            params.append((ts, float(v)))
    sql = f"""
        INSERT INTO `{table}` (`ts`, `{col}`)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE `{col}` = VALUES(`{col}`);
    """
    with conn.cursor() as cur:
        cur.executemany(sql, params)
    conn.commit()

def main():
    log("=== 开始 ===")
    # 点表
    try:
        _, desc_to_name = read_points_mapping(POINTS_CSV)
    except Exception as e:
        log(f"点表解析失败：{e}"); sys.exit(1)

    files = iter_all_dataset_files(VARIABLES_ROOT)
    if not files:
        log(f"未找到数据集：{VARIABLES_ROOT}"); sys.exit(0)
    log(f"发现 {len(files)} 个 CSV")

    try:
        conn = pymysql.connect(
            host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD,
            database=DB_NAME, charset="utf8mb4", autocommit=False
        )
    except Exception as e:
        log(f"数据库连接失败：{e}"); sys.exit(1)

    processed = 0
    prepared_cols: set = set()

    flag = False

    try:
        for fpath in files:
            if '8#炉燃烧炉排_PV_20250701T123000-20250701T124000.csv' in fpath and not flag:
                flag = True

            if not flag:
                continue

            try:
                df = try_read_dataset_csv(fpath)
                if df is None or len(df.columns) < 2:
                    log(f"[跳过] 无法读取或列不足两列：{fpath}")
                    continue
                value_col = df.columns[1]
                desc = str(value_col).strip()
                if desc not in desc_to_name:
                    log(f"[跳过] 点表未找到描述：{desc} | 文件：{fpath}")
                    continue
                raw_name = desc_to_name[desc]
                col_name = sanitize_column_name(raw_name)

                # 解析时间 & 数值
                df.rename(columns={df.columns[0]: "time"}, inplace=True)
                df["time"] = pd.to_datetime(df["time"], errors="coerce")
                df = df.dropna(subset=["time"])
                df[value_col] = pd.to_numeric(df[value_col], errors="coerce")

                ts_vals = df["time"].tolist()

                values = df[value_col].where(pd.notnull(df[value_col]), None).tolist()

                # 确保 ts/索引 与 变量列（仅做一次）
                if col_name not in prepared_cols:
                    ensure_ts_and_columns(conn, TABLE_NAME, [col_name])
                    prepared_cols.add(col_name)

                # 分批 UPSERT
                n = len(ts_vals)
                for i in range(0, n, BATCH_SIZE):
                    upsert_batch(conn, TABLE_NAME, ts_vals[i:i+BATCH_SIZE], col_name, values[i:i+BATCH_SIZE])

                processed += 1
                log(f"[OK] 写入：{desc} -> {col_name} | 行数={len(ts_vals)} | 文件：{fpath}")

            except Exception:
                log(f"[文件失败] {fpath}\n{traceback.format_exc()}")

        log(f"=== 完成：成功 {processed}/{len(files)} 个文件 ===")
    finally:
        try: conn.close()
        except: pass

if __name__ == "__main__":
    main()
