
-- Purpose: enrich deal stage entry events with stage names (lookup join)
-- Note: no funnel mapping; KPI logic handled downstream (marts/reporting)

with transitions as (
    select
        deal_id,
        entered_at,
        month,
        stage_id
    from {{ ref('int_deal_stage_transitions') }}
),

stages as (
    select
        stage_id,
        stage_name
    from {{ ref('stg_stages') }}
)

select
    t.deal_id,
    t.entered_at,
    t.month,
    t.stage_id,
    s.stage_name
from transitions t
left join stages s
    on t.stage_id = s.stage_id
