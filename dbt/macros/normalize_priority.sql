{#
    NHIỆM VỤ 3 — phần 1/3.
    Nhóm 1 '1'..'4'                        -> giữ nguyên
    Nhóm 2 urgent/high/medium/low          -> quy về 1/2/3/4 (schema evolution)
    Nhóm 3 P1 P2 unknown 0 5 -1 '' NULL    -> NULL (tín hiệu quarantine)
#}

{% macro normalize_priority(col) %}
    case
        when try_cast({{ col }} as integer) between 1 and 4
            then try_cast({{ col }} as integer)
        when lower(trim({{ col }})) = 'urgent' then 1
        when lower(trim({{ col }})) = 'high'   then 2
        when lower(trim({{ col }})) = 'medium' then 3
        when lower(trim({{ col }})) = 'low'    then 4
        else null
    end
{% endmacro %}


{% macro priority_reject_reason(col) %}
    case
        when {{ col }} is null                       then 'priority NULL'
        when trim({{ col }}) = ''                    then 'priority rỗng'
        when try_cast({{ col }} as integer) is not null
            then 'priority là số nhưng ngoài miền 1..4'
        else 'priority là chuỗi không thuộc từ điển nhãn (urgent/high/medium/low)'
    end
{% endmacro %}