-- Purpose: derive sales call events (call 1 and call 2) per deal from activities
-- Note:
--   - Map activity_type -> call_number (1/2)
--   - Then deduplicate per deal and call_number
--   - Reporting aggregation handled downstream

{{ config(materialized='view') }}

with base as (

    select
        activity_sk,              
        activity_id,
        deal_id,
        activity_type,
        due_at,
        due_date::date as activity_date
    from {{ ref('stg_activity') }}
    where deal_id is not null
      and due_date is not null
      and activity_type in ('meeting', 'sc_2')

),

mapped as (

    select
        activity_sk,
        activity_id,
        deal_id,
        activity_date,
        due_at,
        case
            when activity_type = 'meeting' then 1
            when activity_type = 'sc_2' then 2
        end as call_number
    from base

),

deduplicated as (

    select *
    from (
        select
            *,
            row_number() over (
                partition by deal_id, call_number
                order by activity_date, due_at nulls last, activity_sk
            ) as rn_deal_call
        from mapped
    ) x
    where rn_deal_call = 1

)

select
    date_trunc('month', activity_date)::date as month,
    deal_id,
    activity_date,
    call_number
from deduplicated
order by deal_id, call_number
