{{ config(materialized='table') }}

with staged as (
    select
        *,
        -- compute month ending (last day of the month)
        date_trunc('month', incidentDatetime) + interval '1 month - 1 day' as monthEnding,
        -- label for filters/dropdowns
        'Month ending ' || strftime(
            date_trunc('month', incidentDatetime) + interval '1 month - 1 day',
            '%Y-%m-%d'
        ) as monthLabel
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
order by monthEnding, district, crimeType;