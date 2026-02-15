
-- Purpose: Stage definitions from raw Pipedrive "stages" table (rename + type casting).
-- This is a lookup table used downstream for funnel mapping.
-- No business logic here; mappings are handled in intermediate/marts.

{{ config(materialized='view') }}

with source as (

    select *
    from {{ source('pipedrive', 'stages') }}

),

staged as (

    select
        stage_id::int as stage_id,
        stage_name::text as stage_name
    from source

)

select *
from staged
