-- ---------------------------------------------------------------------------
-- gold_feature_daily — đặc trưng theo ngày cho agent định tuyến.
-- Grain: 1 hàng / 1 cặp (event_date, customer_id).
-- ---------------------------------------------------------------------------
-- NHIỆM VỤ 2 — đã sửa:
--   Phần 1: lookback window 3 ngày (P99 độ trễ đo được = 2,73 ngày,
--           max = 2,94 ngày → 3 ngày phủ trọn phân bố; lùi thêm chỉ tốn
--           chi phí tính lại ở MỌI lượt chạy sau này mà không bắt thêm hàng).
--   Phần 2: unique_key composite + delete+insert, để một cặp
--           (event_date, customer_id) được tính lại thì THAY THẾ chứ không
--           cộng dồn — nếu thiếu, mở rộng window sẽ tái tạo đúng lỗi NV1.
-- ---------------------------------------------------------------------------

{{ config(
    materialized         = 'incremental',
    unique_key           = ['event_date', 'customer_id'],
    incremental_strategy = 'delete+insert',
    on_schema_change     = 'fail'
) }}

select
    event_date,
    customer_id,
    customer_name,
    segment,
    count(*)                                                  as n_events,
    count(distinct ticket_id)                                 as n_tickets,
    sum(case when is_escalated then 1 else 0 end)             as n_escalated,
    round(avg(latency_ms), 2)                                 as avg_latency_ms,
    quantile_cont(latency_ms, 0.95)::int                      as p95_latency_ms,
    sum(tokens_in)                                            as tokens_in,
    sum(tokens_out)                                           as tokens_out
from {{ ref('silver_events') }}

{% if is_incremental() %}
where event_date >= (
        select (max(event_date) - interval 3 day)::date from {{ this }}
      )
{% endif %}

group by 1, 2, 3, 4