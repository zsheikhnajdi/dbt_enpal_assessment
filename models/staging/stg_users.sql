
-- Purpose: CRM users and deal owners from raw Pipedrive "users" table (rename + type casting).
-- Used for enrichment/joins (e.g., assigned user, deal owner) where needed.
-- No business logic; keep staging layer minimal and stable.

{{ config(materialized='view') }}

with source as (

    select *
    from {{ source('pipedrive', 'users') }}

),

renamed as (

    select
    	id::int as user_id,
    	name::varchar as user_name,
    	email::varchar as user_email,
    	modified::timestamp as modified_at
	from source


)

select * from renamed
