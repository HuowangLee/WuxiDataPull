# -*- coding: utf-8 -*-
import os
import sys
import mysql.connector

DB_NAME = "data"
TABLE_NAME = "wuxihuilian"
USER = "root"
PASSWORD = "lajifenshao"
HOST = "127.0.0.1"
PORT = 3306

INPUT_FILE = "newcolumns.txt"  # 每行：注释,列名（列名可能以 ljDCS. 开头）
ENCODING = "utf-8-sig"         # 兼容 UTF-8 和带 BOM 的 UTF-8

def connect_db(db=None):
    return mysql.connector.connect(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        database=db,
        charset="utf8mb4",
        use_pure=True,
    )

def ensure_database():
    try:
        conn = connect_db(DB_NAME)
        conn.close()
        return
    except mysql.connector.Error as e:
        if e.errno == 1049:  # Unknown database
            root_conn = connect_db(None)
            cur = root_conn.cursor()
            cur.execute(f"CREATE DATABASE IF NOT EXISTS `{DB_NAME}` DEFAULT CHARACTER SET utf8mb4;")
            root_conn.commit()
            cur.close()
            root_conn.close()
        else:
            raise

def ensure_table(conn):
    cur = conn.cursor()
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS `{TABLE_NAME}` (
            `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
            `ts` DATETIME(6) NULL COMMENT '时间戳',
            `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
            PRIMARY KEY (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)
    conn.commit()
    cur.close()

def fetch_existing_columns(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT COLUMN_NAME
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s
    """, (DB_NAME, TABLE_NAME))
    cols = {r[0] for r in cur.fetchall()}
    cur.close()
    return cols

def add_column(conn, col_name, comment):
    # 安全处理：注释中的单引号转义
    comment_safe = (comment or "").replace("'", "''")
    sql = f"ALTER TABLE `{TABLE_NAME}` ADD COLUMN `{col_name}` DOUBLE NULL COMMENT '{comment_safe}';"
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()

    cur.close()

def strip_prefix(name, prefix="ljDCS."):
    name = name.strip()
    if name.startswith(prefix):
        return name[len(prefix):]
    return name

def main():
    if not os.path.exists(INPUT_FILE):
        print(f"未找到输入文件：{INPUT_FILE}")
        sys.exit(1)

    ensure_database()
    conn = connect_db(DB_NAME)

    try:
        ensure_table(conn)
        existing = fetch_existing_columns(conn)

        added, skipped, existed = 0, 0, 0

        with open(INPUT_FILE, "r", encoding=ENCODING) as f:
            for ln, line in enumerate(f, start=1):
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                # 只按第一个逗号切分：注释,列名
                parts = line.split(",", 1)
                if len(parts) != 2:
                    skipped += 1
                    print(f"[第{ln}行] 格式不符合 '注释,列名'：{line}")
                    continue

                comment = parts[0].strip()
                raw_name = parts[1].strip()
                col_name = strip_prefix(raw_name, "ljDCS.")

                if not col_name:
                    skipped += 1
                    print(f"[第{ln}行] 列名为空，原始：{line}")
                    continue

                if len(col_name) > 64:
                    skipped += 1
                    print(f"[第{ln}行] 列名超过 64 字符，已跳过：{col_name}")
                    continue

                if col_name in existing:
                    existed += 1
                    continue

                try:
                    add_column(conn, col_name, comment)
                    existing.add(col_name)
                    added += 1
                except mysql.connector.Error as e:
                    skipped += 1
                    print(f"[第{ln}行] 添加列 {col_name} 失败：{e}")

        print(f"\n✅ 完成：新增列 {added} 个，已存在 {existed} 个，跳过 {skipped} 行。")
        print(f"表 `{DB_NAME}.{TABLE_NAME}` 已创建/更新。基础列：id/ts/updated_at。")

    finally:
        conn.close()

if __name__ == "__main__":
    main()
