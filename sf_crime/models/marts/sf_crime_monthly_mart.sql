{{ config(materialized='table') }}

with staged as (
    select
        *,
        -- compute month ending (last day of the month)
        last_day(incidentDatetime) as monthEnding,
        -- label for filters/dropdowns
        'Month ending ' || strftime(last_day(incidentDatetime), '%Y-%m-%d') as monthLabel
    from {{ ref('sf_crime_staging') }}
),

-- Counts per Police District per month
districtCounts as (
    select
        monthEnding,
        monthLabel,
        district,
        incidentCategoryBroad as crimeType,
        count(*) as count
    from staged
    group by monthEnding, monthLabel, district, incidentCategoryBroad
),

-- City-wide counts (All SF)
sfCounts as (
    select
        monthEnding,
        monthLabel,
        'All SF' as district,
        incidentCategoryBroad as crimeType,
        count(*) as count
    from staged
    group by monthEnding, monthLabel, incidentCategoryBroad
),

-- Combine district and SF counts
monthlyMart as (
    select * from districtCounts
    union all
    select * from sfCounts
)

-- Final table, sorted
select
    monthEnding,
    monthLabel,
    district,
    crimeType,
    count
from monthlyMart
order by monthEnding, district, crimeType