/* =====================================================================
   DATA VALIDATION & PROFILING QUERIES (Manual Checks)
   Project: dbt_enpal_assessment (Pipedrive Analytics)
   Purpose:
     - Quick sanity checks on raw source tables (public.*)
     - Validation checks on dbt outputs (public_pipedrive_analytics.*)
   Notes:
     - These queries are for exploration/manual validation (not dbt models).
     - Run sections as needed (do not run DROP unless you really want it).
   ===================================================================== */

-- ---------------------------------------------------------------------
-- SECTION 0: Inventory - list tables in schema
-- ---------------------------------------------------------------------
select table_name
from information_schema.tables
where table_schema = 'public'
order by table_name;

-- ---------------------------------------------------------------------
-- SECTION 0b: (DANGEROUS) Reset analytics schema - use only if needed
-- ---------------------------------------------------------------------
-- drop schema if exists public_pipedrive_analytics cascade;

-- ---------------------------------------------------------------------
-- SECTION 1: RAW SOURCE TABLES (public.*) - Row counts
-- ---------------------------------------------------------------------
select count(*) as deal_changes_rows   from public.deal_changes;
select count(*) as stages_rows        from public.stages;
select count(*) as fields_rows        from public.fields;
select count(*) as users_rows         from public.users;
select count(*) as activity_rows      from public.activity;
select count(*) as activity_types_rows from public.activity_types;

-- ---------------------------------------------------------------------
-- SECTION 1b: RAW SOURCE TABLES - Quick samples (limit)
-- ---------------------------------------------------------------------
select * from public.deal_changes limit 20;

select *
from public.stages
order by 1;

select * from public.activity limit 20;

-- ---------------------------------------------------------------------
-- SECTION 1c: RAW SOURCE TABLES - Table schema (columns & types)
-- Example: stages table
-- ---------------------------------------------------------------------
select column_name, data_type
from information_schema.columns
where table_schema = 'public'
  and table_name  = 'stages'
order by ordinal_position;

-- ---------------------------------------------------------------------
-- SECTION 1d: RAW SOURCE TABLES - Deal changes distribution
-- ---------------------------------------------------------------------
select changed_field_key, count(*) as cnt
from public.deal_changes
group by 1
order by cnt desc;

-- ---------------------------------------------------------------------
-- SECTION 2: RAW SOURCE QUALITY CHECKS (public.activity) - Duplicates
-- ---------------------------------------------------------------------

-- How many activity rows vs distinct activity_id?
select
  count(*) as total_rows,
  count(distinct activity_id) as distinct_activity_ids
from public.activity;

-- Top duplicated activity_ids (how many rows per id)
select
  activity_id,
  count(*) as cnt
from public.activity
group by 1
having count(*) > 1
order by cnt desc
limit 50;

-- Duplicates with "distinct payload" count (are duplicates identical or different?)
select
  activity_id,
  count(*) as total_rows,
  count(distinct (type, assigned_to_user, deal_id, done, due_to)) as distinct_payloads
from public.activity
group by 1
having count(*) > 1
order by total_rows desc
limit 50;

-- Show fully duplicated rows (same payload repeated)
select *
from (
    select
      *,
      count(*) over (
        partition by activity_id, type, assigned_to_user, deal_id, done, due_to
      ) as dup_cnt
    from public.activity
) t
where dup_cnt > 1
order by activity_id
limit 200;

-- Inspect all rows for duplicated activity_ids (manual review)
select *
from public.activity
where activity_id in (
    select activity_id
    from public.activity
    group by activity_id
    having count(*) > 1
)
order by activity_id
limit 500;

-- Fully duplicated rows check in raw deal_changes (same payload repeated)
select
  deal_id,
  changed_field_key,
  new_value,
  change_time,
  count(*) as dup_cnt
from public.deal_changes
group by 1,2,3,4
having count(*) > 1
order by dup_cnt desc
limit 50;

-- duplicated rows check in raw deal_changes for deal_id + changed_field_key + change_time
select
  deal_id,
  changed_field_key,
  change_time,
  count(*) as dup_cnt
from public.deal_changes
group by 1,2,3
having count(*) > 1
order by dup_cnt desc
limit 50;

-- ---------------------------------------------------------------------
-- SECTION 3: STAGING LAYER VALIDATION (public_pipedrive_analytics.stg_*)
-- ---------------------------------------------------------------------

-- Row counts (staging outputs)
select count(*) as stg_deal_changes_rows   from public_pipedrive_analytics.stg_deal_changes;
select count(*) as stg_stages_rows        from public_pipedrive_analytics.stg_stages;
select count(*) as stg_activity_rows      from public_pipedrive_analytics.stg_activity;
select count(*) as stg_users_rows         from public_pipedrive_analytics.stg_users;
select count(*) as stg_activity_types_rows from public_pipedrive_analytics.stg_activity_types;
select count(*) as stg_fields_rows        from public_pipedrive_analytics.stg_fields;

-- Quick samples (manual inspection)
select *
from public_pipedrive_analytics.stg_deal_changes
order by deal_id asc
limit 100;

select * from public_pipedrive_analytics.stg_stages limit 100;
select * from public_pipedrive_analytics.stg_activity limit 100;
select * from public_pipedrive_analytics.stg_users limit 100;
select * from public_pipedrive_analytics.stg_activity_types limit 100;
select * from public_pipedrive_analytics.stg_fields limit 100;

-- Deal changes distribution in staging
select
  changed_field_key,
  count(*) as cnt
from public_pipedrive_analytics.stg_deal_changes
group by 1
order by cnt desc;

-- Null checks (staging)
select count(*) as null_changed_field_key
from public_pipedrive_analytics.stg_deal_changes
where changed_field_key is null;

select count(*) as null_change_timestamp
from public_pipedrive_analytics.stg_deal_changes
where change_timestamp is null;

select count(*) as bad_rows_any_required_null
from public_pipedrive_analytics.stg_deal_changes
where deal_id is null
   or change_timestamp is null
   or changed_field_key is null;

-- Focus: only stage_id changes (staging)
select count(*) as stage_id_change_rows
from public_pipedrive_analytics.stg_deal_changes
where changed_field_key = 'stage_id';

-- ---------------------------------------------------------------------
-- SECTION 4: INTERMEDIATE LAYER VALIDATION
-- Models:
--   - int_deal_stage_transitions
--   - int_deal_stage_events
--   - int_sales_call_events
-- ---------------------------------------------------------------------

-- Row counts
select count(*) as int_deal_stage_transitions_rows
from public_pipedrive_analytics.int_deal_stage_transitions;

select count(*) as int_deal_stage_events_rows
from public_pipedrive_analytics.int_deal_stage_events;

select count(*) as int_sales_call_events_rows
from public_pipedrive_analytics.int_sales_call_events;

-- ---------------------------------------------------------------------
-- 4a) Deal stage transitions - quick sample
-- ---------------------------------------------------------------------
select *
from public_pipedrive_analytics.int_deal_stage_transitions
order by entered_at desc
limit 100;

-- ---------------------------------------------------------------------
-- 4b) stg_stages integrity checks (nulls + duplicates)
-- ---------------------------------------------------------------------
select count(*) as null_stage_id
from public_pipedrive_analytics.stg_stages
where stage_id is null;

select count(*) as null_stage_name
from public_pipedrive_analytics.stg_stages
where stage_name is null;

select stage_id, count(*) as cnt
from public_pipedrive_analytics.stg_stages
group by stage_id
having count(*) > 1;

-- ---------------------------------------------------------------------
-- 4c) stg_activity quick profiling (post-staging)
-- ---------------------------------------------------------------------
select
  count(*) as total_rows,
  count(distinct activity_id) as distinct_activity_ids
from public_pipedrive_analytics.stg_activity;


select activity_id
from public_pipedrive_analytics.stg_activity
group by 1
having count(*) > 1;

select *
from public_pipedrive_analytics.stg_activity
where activity_id in(
	select activity_id
	from public_pipedrive_analytics.stg_activity
	group by 1
	having count(*) > 1 
);

select
    activity_id,
    deal_id,
    activity_type,
    is_done,
    due_at,
    due_date,
    assigned_user_id,
    count(*) as cnt
from public_pipedrive_analytics.stg_activity
group by
    activity_id,
    deal_id,
    activity_type,
    is_done,
    due_at,
    due_date,
    assigned_user_id
having count(*) > 1;

-- Surrogate key should be unique (row identifier)
select
  count(*) as total_rows,
  count(distinct activity_sk) as distinct_activity_sks
from public_pipedrive_analytics.stg_activity;

-- Should return 0 rows if activity_sk is unique
select activity_sk, count(*) as cnt
from public_pipedrive_analytics.stg_activity
group by 1
having count(*) > 1;

select activity_type, count(*) as cnt
from public_pipedrive_analytics.stg_activity
group by 1
order by cnt desc;

select deal_id, activity_type, count(*) as cnt
from public_pipedrive_analytics.stg_activity
where activity_type in ('meeting','sc_2')
  and deal_id is not null
group by 1,2
having count(*) > 1
order by cnt desc
limit 20;


-- Investigate duplicate Sales Call 2 activities for specific deal
select
  *
from public_pipedrive_analytics.stg_activity
where deal_id = 793245
  and activity_type = 'sc_2'
order by due_at nulls last, activity_id, assigned_user_id;

-- ---------------------------------------------------------------------
-- 4d) int_deal_stage_events - quick sample + duplicate check
-- ---------------------------------------------------------------------
select *
from public_pipedrive_analytics.int_deal_stage_events
order by entered_at desc

-- Detect duplicate groups (should be 0)
select count(*) as duplicate_groups
from (
    select
        deal_id,
        entered_at,
        stage_id
    from public_pipedrive_analytics.int_deal_stage_events
    group by 1,2,3
    having count(*) > 1
) t;

-- Stage name nulls (should be 0)
select count(*) as null_stage_name
from public_pipedrive_analytics.int_deal_stage_events
where stage_name is null;

-- Find the CURRENT (latest) stage of each deal
select
  deal_id,
  stage_id,
  stage_name,
  entered_at
from (
  select
    deal_id,
    stage_id,
    stage_name,
    entered_at,
    row_number() over (partition by deal_id order by entered_at desc) as rn
  from public_pipedrive_analytics.int_deal_stage_events
) x
where rn = 1
order by entered_at desc;



-- ---------------------------------------------------------------------
-- 4e) int_sales_call_events - quick sample + distribution
-- ---------------------------------------------------------------------
select *
from public_pipedrive_analytics.int_sales_call_events
limit 100;

-- How many calls per deal (top 20)
select
  deal_id,
  count(*) as total_calls
from public_pipedrive_analytics.int_sales_call_events
group by 1
order by total_calls desc
limit 20;

-- call_number distribution
select call_number, count(*) as cnt
from public_pipedrive_analytics.int_sales_call_events
group by 1
order by 1;

select call_number, count(distinct deal_id) as deals_cnt
from public_pipedrive_analytics.int_sales_call_events
group by 1
order by 1;

-- Deals with both calls (call 1 and call 2)
select count(*) as deals_with_both_calls
from (
  select deal_id
  from public_pipedrive_analytics.int_sales_call_events
  group by deal_id
  having count(distinct call_number) = 2
) t;

-- For deals with both calls: show first call1/call2 dates
select
  deal_id,
  min(case when call_number = 1 then activity_date end) as call1,
  min(case when call_number = 2 then activity_date end) as call2
from public_pipedrive_analytics.int_sales_call_events
group by deal_id
having count(distinct call_number) = 2;

-- Sanity: there should be max 1 row per deal_id + call_number
select deal_id, call_number, count(*) as cnt
from public_pipedrive_analytics.int_sales_call_events
group by 1,2
having count(*) > 1;

select deal_id, count(distinct call_number)
from public_pipedrive_analytics.int_sales_call_events
group by 1
having count(distinct call_number) > 2;

-- ---------------------------------------------------------------------
-- 4f) Deal timeline (stages + calls) - full event timeline per deal
-- ---------------------------------------------------------------------
select
    deal_id,
    event_type,
    event_name,
    event_date,
    month,
    entered_at,  
    activity_date,   
    call_number,     
    stage_id,        
    stage_name       
from (

    -- 1) Stage events
    select
        deal_id,
        'stage' as event_type,
        stage_name as event_name,
        entered_at::date as event_date,
        month,
        entered_at,
        null::date as activity_date,
        null::int as call_number,
        stage_id,
        stage_name
    from public_pipedrive_analytics.int_deal_stage_events

    union all

    -- 2) Sales calls
    select
        deal_id,
        'call' as event_type,
        case
            when call_number = 1 then 'Sales Call 1'
            when call_number = 2 then 'Sales Call 2'
            else 'Sales Call (other)'
        end as event_name,
        activity_date as event_date,
        month,
        null::timestamp as entered_at,
        activity_date,
        call_number,
        null::int as stage_id,
        null::text as stage_name
    from public_pipedrive_analytics.int_sales_call_events

) t
order by deal_id, event_date, event_type;


-- ---------------------------------------------------------------------
-- SECTION 5: REPORTING / MART VALIDATION
-- Model:
--   - rep_sales_funnel_monthly
-- ---------------------------------------------------------------------

select *
from public_pipedrive_analytics.rep_sales_funnel_monthly
order by month, funnel_step;

-- Check for null values
select *
from public_pipedrive_analytics.rep_sales_funnel_monthly
where month is null
   or kpi_name is null
   or deals_count is null;

-- Check funnel behavior per month
select month, sum(deals_count)
from public_pipedrive_analytics.rep_sales_funnel_monthly
group by month
order by month;


-- ---------------------------------------------------------------------
-- SECTION 6: SUMMARY (manual notes)
-- ---------------------------------------------------------------------
/*
What this script validates:

1) Raw sources (public.*)
   - Table inventory and row counts
   - Quick samples to validate shapes
   - activity duplicates / payload differences

2) Staging layer (public_pipedrive_analytics.stg_*)
   - Row counts per staging model
   - Null checks for key fields in stg_deal_changes
   - Distribution of changed_field_key
   - Focus check on stage_id changes

3) Intermediate layer (public_pipedrive_analytics.int_*)
   - Row counts for transitions/events/calls
   - Duplicate group checks (stage events uniqueness)
   - Null checks on stage_name after enrichment
   - Current stage per deal
   - Sales calls distribution and deals with both calls

4) Reporting layer
   - Final report preview and ordering

Tip:
- For automated integrity checks, refer to dbt tests in *.yml files
  (not_null, relationships, accepted_values, dbt_utils.unique_combination_of_columns).
*/
