-- ---------------------------------------------------------------------------
-- quarantine_tickets — bản ghi CDC không thoả data contract.
-- Grain: 1 hàng / 1 BẢN GHI CDC bị loại. Kỳ vọng 312 hàng.
-- Dùng đúng macro normalize_priority mà silver_tickets dùng → hai model
-- không thể lệch nhau.
-- ---------------------------------------------------------------------------

{{ config(materialized = 'table') }}

select
    ticket_id,
    cdc_seq,
    op,
    event_time,
    _ingested_at,
    priority_raw,
    {{ priority_reject_reason('priority_raw') }}             as reject_reason,
    customer_id,
    customer_name,
    category,
    status
from {{ source('bronze', 'bronze_tickets_cdc') }}

where {{ normalize_priority('priority_raw') }} is null