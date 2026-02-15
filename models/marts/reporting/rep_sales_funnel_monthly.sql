
-- Purpose: monthly sales funnel report (deals entering each funnel step)
-- Note: stage events are mapped to funnel steps via explicit mapping; call steps derived from activities
-- Assumption: Pipedrive stages (stage_id 1–9) directly correspond to the required funnel steps.
-- If stage definitions were custom or non-sequential, an explicit mapping layer would be introduced.


{{ config(materialized='table') }}

with stage_kpis as (

    select
        month::date as month,
        stage_name as kpi_name,
        stage_id::text as funnel_step,
        count(distinct deal_id) as deals_count,
        stage_id::numeric as funnel_step_sort
    from {{ ref('int_deal_stage_events') }}
    group by 1,2,3,5

),

call_kpis as (

    select
        month::date as month,
        case
            when call_number = 1 then 'Sales Call 1'
            when call_number = 2 then 'Sales Call 2'
        end as kpi_name,
        case
            when call_number = 1 then '2.1'
            when call_number = 2 then '3.1'
        end as funnel_step,
        count(distinct deal_id) as deals_count,
        case
            when call_number = 1 then 2.1::numeric
            when call_number = 2 then 3.1::numeric
        end as funnel_step_sort
    from {{ ref('int_sales_call_events') }}
    where call_number in (1, 2)
    group by 1,2,3,5

),

final as (

    select month, kpi_name, funnel_step, deals_count, funnel_step_sort
    from stage_kpis

    union all

    select month, kpi_name, funnel_step, deals_count, funnel_step_sort
    from call_kpis

)

select
    month,
    kpi_name,
    funnel_step,
    deals_count
from final
order by month, funnel_step_sort
