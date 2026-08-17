#!/usr/bin/env python3
"""Consumer đọc topic `ai-events` và ghi xuống bảng stream — NHIỆM VỤ 5 (đã sửa).

Chạy tay:
    python ingest/consumer.py --db data/crash/crash.duckdb \
        --topic data/crash/topic.jsonl --offset data/crash/offsets.json

ĐÃ SỬA — hai hạng mục:

  (a) Thứ tự thao tác: ghi TRƯỚC, commit offset SAU.
      Bản gốc commit trước rồi mới ghi → at-most-once: crash ở giữa làm
      offset đã dịch nhưng dữ liệu chưa xuống kho, lần restart bỏ qua lô đó
      và message MẤT vĩnh viễn, không có cơ chế nào phát hiện được.
      Sau khi đảo → at-least-once: crash chỉ khiến offset chưa kịp dịch, lần
      restart đọc lại lô đó. Ta đổi "mất dữ liệu" thành "trùng dữ liệu" —
      một đánh đổi có lợi, vì trùng thì sửa được ở tầng ghi, mất thì không.

  (b) Phép ghi idempotent: `event_id` là primary key, INSERT dùng
      ON CONFLICT ... DO UPDATE. Nhờ vậy lô được phát lại ghi đè chính nó
      thay vì nhân bản. At-least-once + ghi idempotent = hiệu ứng
      exactly-once, dù tầng giao vận không hề bảo đảm điều đó.

  Chọn DO UPDATE thay vì DO NOTHING: khi một message được phát lại với nội
  dung ĐÃ ĐỔI (ví dụ producer sửa latency_ms rồi gửi lại cùng event_id),
  DO NOTHING giữ nguyên bản cũ và âm thầm bỏ mất bản cập nhật, còn DO UPDATE
  hội tụ về trạng thái mới nhất. DO NOTHING chỉ đủ khi message bất biến.
"""

from __future__ import annotations

import argparse
import os
import pathlib
import sys

import duckdb

# Windows: stdout của subprocess mặc định cp1252, không in được tiếng Việt.
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from ingest.log_client import LogConsumer  # noqa: E402

TABLE = "bronze_events_stream"

# `primary key` trên event_id là điều kiện để DuckDB chấp nhận mệnh đề
# ON CONFLICT — không có ràng buộc này thì không có phép ghi idempotent nào.
DDL = f"""
create table if not exists {TABLE} (
    event_id      varchar primary key,
    ticket_id     varchar,
    customer_id   varchar,
    customer_name varchar,
    event_type    varchar,
    latency_ms    integer,
    event_time    timestamp,
    _ingested_at  timestamp
);
"""


def write_batch(con: duckdb.DuckDBPyConnection, batch: list[dict]) -> None:
    """Ghi một lô message xuống kho — idempotent theo event_id.

    Ghi lại cùng một event_id không tạo hàng mới: nó ghi đè hàng cũ. Đây là
    thứ biến at-least-once thành an toàn.
    """
    con.executemany(
        f"""
        insert into {TABLE} values (?, ?, ?, ?, ?, ?, ?, ?)
        on conflict (event_id) do update set
            ticket_id     = excluded.ticket_id,
            customer_id   = excluded.customer_id,
            customer_name = excluded.customer_name,
            event_type    = excluded.event_type,
            latency_ms    = excluded.latency_ms,
            event_time    = excluded.event_time,
            _ingested_at  = excluded._ingested_at
        """,
        [
            (
                r["event_id"], r["ticket_id"], r["customer_id"], r["customer_name"],
                r["event_type"], r["latency_ms"], r["event_time"], r["_ingested_at"],
            )
            for r in batch
        ],
    )


def maybe_crash(batch_no: int, crash_at: int | None) -> None:
    """Mô phỏng `kill -9`: chết ngay, không rollback, không flush."""
    if crash_at is not None and batch_no == crash_at:
        print(f"  [consumer] 💥 tiến trình bị giết ở lô {batch_no}", flush=True)
        os._exit(137)


def consume(
    db: str,
    topic: str,
    offset_file: str,
    batch_size: int = 500,
    crash_at: int | None = None,
) -> int:
    con = duckdb.connect(db)
    con.execute(DDL)

    written = 0
    with LogConsumer(topic, offset_file) as consumer:
        batch_no = 0
        while True:
            batch = consumer.poll(batch_size)
            if not batch:
                break
            batch_no += 1

            # ── ĐÃ SỬA — nhiệm vụ 5, hạng mục (a) ────────────────────────
            # GHI trước, COMMIT sau. Crash ở maybe_crash() nghĩa là dữ liệu
            # đã xuống kho nhưng offset chưa dịch → restart đọc lại lô này →
            # write_batch() idempotent hấp thụ phần trùng.
            write_batch(con, batch)           # ghi dữ liệu
            maybe_crash(batch_no, crash_at)   # sự cố xảy ra tại đây
            consumer.commit()                 # ghi nhận offset
            # ─────────────────────────────────────────────────────────────

            written += len(batch)

    con.close()
    return written


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--topic", required=True)
    ap.add_argument("--offset", required=True)
    ap.add_argument("--batch-size", type=int, default=500)
    ap.add_argument("--crash-at-batch", type=int, default=None)
    a = ap.parse_args()
    n = consume(a.db, a.topic, a.offset, a.batch_size, a.crash_at_batch)
    print(f"  [consumer] đã ghi {n:,} message")
    return 0


if __name__ == "__main__":
    sys.exit(main())