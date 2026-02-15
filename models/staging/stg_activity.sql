
-- Purpose: Activities from raw Pipedrive "activity" table (rename + type casting).
-- Note: keep staging minimal; any dedup/canonical logic is handled downstream (intermediate).

{{ config(materialized='view') }}

with source as (
  select *
  from {{ source('pipedrive','activity') }}
),

typed as (
  select
    activity_id::bigint as activity_id,
    deal_id::bigint as deal_id,
    type::text as activity_type,
    done::boolean as is_done,
    due_to::timestamp as due_at,
    assigned_to_user::bigint as assigned_user_id
  from source
)

select
  -- surrogate key because activity_id is not unique in the provided dataset
  {{ dbt_utils.generate_surrogate_key([
    'activity_id',
    'deal_id',
    'activity_type',
    'is_done',
    'due_at',
    'assigned_user_id'
  ]) }} as activity_sk,

  activity_id,
  deal_id,
  activity_type,
  is_done,
  due_at,
  due_at::date as due_date,
  assigned_user_id
from typed
