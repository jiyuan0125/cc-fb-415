#!/usr/bin/env python3
"""ingest_ord.py — 按小时订单增量入库（幂等重跑）。

用法:
    python3 -I ingest_ord.py [feed_dir] [db_path]
默认: feed_dir=./feed  db_path=./orders.db

产物（均在当前工作目录，每次运行整体重建，故可重复执行且结果不变）:
    orders.db          sqlite3 库，orders(order_id, amount, status, updated_at)，一单一行
    quarantine.csv     越界行，表头 file line raw，一行一个越界行
    ingest_summary.txt 四行: landed/updated/skipped/quarantined + 数值
    notes.txt          重推行处置规则说明
"""

import csv
import re
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

FEED_GLOB = "orders_20*"
VALID_STATUS = {"已支付", "已发货", "已完成", "已关闭"}
TS_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")
INT_RE = re.compile(r"^\d+$")

NOTES = """重推行处置规则（ingest_ord.py）
================================

1. 处理顺序：按文件名顺序处理 24 个小时文件（orders_YYYYMMDDHH），
   文件内按行号从小到大处理。

2. 每行先用规约校验，不合规的行不进表，原样记入 quarantine.csv
   （列：file 文件名、line 文件内行号（从 1 起）、raw 原始整行）。
   校验全部通过才允许参与入库判断。校验项：
   - 必须恰好四列，逗号分隔，无空字段；
   - 金额：纯数字整数（以分记），带小数点即越界；
   - 状态：仅允许 已支付 / 已发货 / 已完成 / 已关闭；
   - 更新时间：严格形如 "YYYY-MM-DD HH:MM:SS"，
     带 T 的 ISO 写法越界，且日期时间必须是真实存在的时刻；
   - 订单号：非空纯数字。

3. 合规行按更新时间（updated_at）决定算数与否，与来自哪个小时文件、
   行的先后无关——重推的行不一定比库里已有的新：
   - 表中无此订单：首落（landed），插入该行；
   - 行内 updated_at 严格晚于表中该行：更新（updated），整行覆盖；
   - 行内 updated_at 等于或早于表中该行：跳过（skipped），表不动。
   时间戳为定长规范文本，按字符串比较与按时刻比较等价。

4. 同一文件内同一订单出现多行时逐行依次套用第 3 条；
   updated_at 相同的重推行按“先来的算数”处理，后来的记跳过。

5. 只推来过越界行、从未推来过合规行的订单不会出现在 orders 表中；
   先越界后被合规重推的订单，其合规行正常参与第 3 条判断。

6. 幂等：每次运行都删除并整体重建 orders.db、quarantine.csv、
   ingest_summary.txt，结论只取决于 feed 内容，重复执行表数据与
   四个计数均不变。
"""


def parse_row(raw):
    """返回 (order_id, amount:int, status, updated_at)；不合规返回 None。"""
    parts = raw.split(",")
    if len(parts) != 4:
        return None
    order_id, amount_s, status, updated_at = (p.strip() for p in parts)
    if not order_id or not INT_RE.match(order_id):
        return None
    if not INT_RE.match(amount_s):          # 带小数点/负数/空 一律越界
        return None
    if status not in VALID_STATUS:
        return None
    if not TS_RE.match(updated_at):
        return None
    try:
        datetime.strptime(updated_at, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None
    return order_id, int(amount_s), status, updated_at


def main():
    feed_dir = Path(sys.argv[1] if len(sys.argv) > 1 else "feed")
    db_path = Path(sys.argv[2] if len(sys.argv) > 2 else "orders.db")
    q_path = Path("quarantine.csv")
    summary_path = Path("ingest_summary.txt")
    notes_path = Path("notes.txt")

    files = sorted(p for p in feed_dir.glob(FEED_GLOB) if p.is_file())

    # 整体重建，保证重跑幂等
    db_path.unlink(missing_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE orders ("
        "order_id TEXT PRIMARY KEY, amount INTEGER, status TEXT, updated_at TEXT)"
    )

    landed = updated = skipped = quarantined = 0
    current = {}  # order_id -> (amount, status, updated_at)，工作状态即最终表内容

    with q_path.open("w", newline="", encoding="utf-8") as qf:
        qw = csv.writer(qf)
        qw.writerow(["file", "line", "raw"])
        for path in files:
            with path.open("r", encoding="utf-8", newline="") as f:
                for line_no, line in enumerate(f, start=1):
                    raw = line.rstrip("\r\n")
                    rec = parse_row(raw)
                    if rec is None:
                        qw.writerow([path.name, line_no, raw])
                        quarantined += 1
                        continue
                    order_id, amount, status, updated_at = rec
                    if order_id not in current:
                        current[order_id] = (amount, status, updated_at)
                        landed += 1
                    elif updated_at > current[order_id][2]:
                        current[order_id] = (amount, status, updated_at)
                        updated += 1
                    else:  # 等于或早于已有行：跳过
                        skipped += 1

    conn.executemany(
        "INSERT INTO orders (order_id, amount, status, updated_at) VALUES (?, ?, ?, ?)",
        [(oid, *row) for oid, row in sorted(current.items())],
    )
    conn.commit()
    conn.close()

    summary_path.write_text(
        f"landed {landed}\n"
        f"updated {updated}\n"
        f"skipped {skipped}\n"
        f"quarantined {quarantined}\n",
        encoding="utf-8",
    )
    notes_path.write_text(NOTES, encoding="utf-8")


if __name__ == "__main__":
    main()
