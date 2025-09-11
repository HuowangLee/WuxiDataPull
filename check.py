#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import pymysql
from decimal import Decimal
from datetime import datetime, timezone

DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "lajifenshao")
DB_NAME = os.getenv("DB_NAME", "data")
TABLE_NAME = os.getenv("TABLE_NAME", "wuxihuilian")  # 也可直接改为你的表名

TS_COL = "ts"  # 时间戳列名

def to_iso_from_ts(x):
    """把 MySQL 取出的 ts 值转换为 ISO8601 字符串。
       支持 datetime、int/float/Decimal（秒或毫秒级 Unix 时间戳）、以及字符串数字。"""
    if x is None:
        return None
    # datetime 直接格式化
    if isinstance(x, datetime):
        # 若没有时区，按本地或 UTC 自行调整；这里统一输出为 ISO（不强制加时区）
        return x.isoformat(sep=' ', timespec='seconds')
    # 处理数字/字符串
    if isinstance(x, (int, float, Decimal)) or (isinstance(x, str) and x.isdigit()):
        v = int(x)
        # 粗略判断毫秒/秒（>= 10^12 多为毫秒）
        if v >= 10**12:
            v = v / 1000.0
        dt = datetime.fromtimestamp(v)
        return dt.isoformat(sep=' ', timespec='seconds')
    # 兜底：转字符串
    return str(x)

def main():
    conn = pymysql.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD,
        database=DB_NAME, charset="utf8mb4", cursorclass=pymysql.cursors.DictCursor
    )
    try:
        with conn.cursor() as cur:
            # 1) 拿到所有列名，排除 ts 列
            cur.execute("""
                SELECT COLUMN_NAME
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s
                ORDER BY ORDINAL_POSITION
            """, (DB_NAME, TABLE_NAME))
            cols = [r["COLUMN_NAME"] for r in cur.fetchall()]
            if TS_COL not in cols:
                raise RuntimeError(f"列 `{TS_COL}` 不存在，请确认表结构。")
            target_cols = [c for c in cols if c != TS_COL]

            if not target_cols:
                print("表中只有 ts 列，无需统计。")
                return

            # 2) 构造一次性聚合 SQL：
            #    对每一列生成 COUNT(col) 和 MAX(CASE WHEN col IS NOT NULL THEN ts END)
            select_parts = []
            for c in target_cols:
                select_parts.append(f"COUNT(`{c}`) AS `count__{c}`")
                select_parts.append(
                    f"MAX(CASE WHEN `{c}` IS NOT NULL THEN `{TS_COL}` END) AS `latest__{c}`"
                )
            sql = f"SELECT {', '.join(select_parts)} FROM `{TABLE_NAME}`"

            cur.execute(sql)
            row = cur.fetchone() or {}

            # 3) 整理输出
            results = []
            for c in target_cols:
                count_key = f"count__{c}"
                latest_key = f"latest__{c}"
                cnt = row.get(count_key, 0) or 0
                latest_ts = row.get(latest_key, None)
                results.append({
                    "column": c,
                    "non_null_count": int(cnt),
                    "latest_non_null_ts": to_iso_from_ts(latest_ts)
                })

            # 可按 ts 排序理解为“更关注最近有值的列”，这里按 latest_non_null_ts 降序输出；
            # 若你只想自然顺序或按列名，请调整排序键。
            def sort_key(item):
                t = item["latest_non_null_ts"]
                if t is None:
                    return (0, "")  # None 放最后
                try:
                    # 尝试解析回时间用于排序
                    return (1, datetime.fromisoformat(t))
                except Exception:
                    return (1, t)

            results.sort(key=sort_key, reverse=True)

            # 4) 打印结果
            print(f"Table: {TABLE_NAME}  (DB: {DB_NAME})")
            print(f"统计说明：每列非空数量，以及该列最近一次非空记录对应的 {TS_COL} 时间")
            print("-" * 80)
            colw1, colw2 = 32, 16
            print(f"{'column'.ljust(colw1)} {'non_null_count'.rjust(colw2)}    latest_non_null_ts")
            print("-" * 80)
            for r in results:
                print(f"{r['column'].ljust(colw1)} {str(r['non_null_count']).rjust(colw2)}    {r['latest_non_null_ts']}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
