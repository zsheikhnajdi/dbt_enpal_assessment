
-- Purpose: rename + type cast raw metadata fields (custom deal fields)
-- Note: no business logic; retained for optional enrichment use cases

{{ config(materialized='view') }}

with source as (

    select *
    from {{ source('pipedrive', 'fields') }}

),

renamed as (

    select
    	id::int as field_id,
    	field_key::varchar as field_key,
    	name::varchar as field_name,
    	field_value_options
	from source


)

select * from renamed
