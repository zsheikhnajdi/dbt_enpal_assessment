
-- Purpose: rename + type cast raw activity_types (lookup dimension)
-- Note: no business logic; supports activity enrichment in downstream models

{{ config(materialized='view') }}

with source as (

    select *
    from {{ source('pipedrive', 'activity_types') }}

),

renamed as (

    select
    	id::int as activity_type_id,
    	name::varchar as activity_type_name,
    	active::boolean as is_active

	from source

)


select * from renamed
