
-- Purpose: rename + type cast raw deal_changes events
-- Note: no business logic; stage transitions & funnel logic handled downstream (intermediate/marts)

{{ config(materialized='view') }}

with source as (

    select *
    from {{ source('pipedrive', 'deal_changes') }}

),

staged as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'deal_id',
            'changed_field_key',
            'new_value',
            'change_time'
        ]) }} as deal_change_sk,
    	deal_id::bigint as deal_id,
        changed_field_key::text as changed_field_key,
        new_value::text as new_value,
        change_time::timestamp as change_timestamp
    from source

)

select *
from staged
