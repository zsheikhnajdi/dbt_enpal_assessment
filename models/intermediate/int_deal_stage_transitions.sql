
-- Purpose: derive deal stage entry events from deal_changes (stage_id updates)
-- Note: no funnel mapping; enrichment and KPI logic handled downstream

with deal_changes as (

    select
        deal_id,
        changed_field_key,
        new_value,
        change_timestamp
    from {{ ref('stg_deal_changes') }}

),

stage_events as (

    select
        deal_id,
        new_value::int as stage_id,
        change_timestamp as entered_at
    from deal_changes
    where changed_field_key = 'stage_id'
      and new_value is not null
      and new_value ~ '^\d+$'

),

dedup_stage_events as (

    select
        deal_id,
        stage_id,
        entered_at,
        row_number() over (
            partition by deal_id, stage_id, entered_at
            order by entered_at
        ) as rn
    from stage_events

)

select
    deal_id,
    stage_id,
    entered_at,
    date_trunc('month', entered_at)::date as month
from dedup_stage_events
where rn = 1
