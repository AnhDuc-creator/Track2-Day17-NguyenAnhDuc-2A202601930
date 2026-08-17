-- ---------------------------------------------------------------------------
-- silver_tickets — trạng thái mới nhất của mỗi ticket, dựng lại từ luồng CDC.
-- ---------------------------------------------------------------------------
-- NHIỆM VỤ 3 — phần 2/3 (đã sửa):
--   Thứ tự đúng là LỌC bản ghi hỏng TRƯỚC → xếp hạng SAU.
--   Ta cách ly BẢN GHI, không cách ly TICKET: ticket có bản ghi mới nhất bị
--   hỏng vẫn còn trạng thái hợp lệ từ lần cập nhật trước, nên tổng số ticket
--   giữ nguyên 12.480.
--   Các cột số/thời gian được cast tường minh để khớp data_type mà contract
--   khai báo trong schema.yml.
-- ---------------------------------------------------------------------------

{{ config(materialized = 'table') }}

with normalized as (

    select
        *,
        {{ normalize_priority('priority_raw') }}             as priority_clean
    from {{ source('bronze', 'bronze_tickets_cdc') }}

),

-- LỌC TRƯỚC: bỏ những bản ghi CDC không chuẩn hoá được (chúng đi sang
-- quarantine_tickets bằng chính điều kiện ngược lại).
valid as (select * from normalized where priority_clean is not null),

-- XẾP HẠNG SAU: trên tập bản ghi đã hợp lệ.
ranked as (

    select
        *,
        row_number() over (
            partition by ticket_id
            order by event_time desc, cdc_seq desc
        ) as _rn
    from valid

),

latest as (select * from ranked where _rn = 1)

select
    ticket_id::varchar                                       as ticket_id,
    customer_id::varchar                                     as customer_id,
    customer_name::varchar                                   as customer_name,
    segment::varchar                                         as segment,
    priority_clean::integer                                  as priority,
    category::varchar                                        as category,
    channel::varchar                                         as channel,
    status::varchar                                          as status,
    csat::integer                                            as csat,
    first_response_sec::integer                              as first_response_sec,
    subject::varchar                                         as subject,
    body::varchar                                            as body,
    event_time::timestamp                                    as updated_at,
    _ingested_at::timestamp                                  as _ingested_at
from latest
where op <> 'd'