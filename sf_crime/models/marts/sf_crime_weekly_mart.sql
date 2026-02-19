{{ config(materialized='table') }}

with staged as (
    select
        *,
        -- compute the week ending (Saturday)
        date_trunc(
            'day',
            incidentDatetime + (6 - extract(dow from incidentDatetime)) * interval '1 day'
        ) as weekEnding,
        -- label for dropdowns/filters (DuckDB compatible)
        'Week ending ' || strftime(
            date_trunc(
                'day',
                incidentDatetime + (6 - extract(dow from incidentDatetime)) * interval '1 day'
            ),
            '%Y-%m-%d'
        ) as weekLabel
    from {{ ref('sf_crime_staging') }}
),

-- Weekly intersection aggregates
weeklyIntersection as (
    select
        intersection,
        weekEnding,
        count(distinct incidentNumber) as intersection7DayTtl,
        count(distinct incidentNumber)/7.0 as intersection7DayAvg
    from staged
    group by intersection, weekEnding
),

-- Weekly district aggregates
weeklyDistrict as (
    select
        district,
        weekEnding,
        count(*) as district7dTtl
    from staged
    group by district, weekEnding
),

-- Weekly neighborhood aggregates
weeklyNeighborhood as (
    select
        neighborhood,
        weekEnding,
        count(*) as neighborhood7dTtl
    from staged
    group by neighborhood, weekEnding
),

-- Incident category counts per intersection/week
catCounts as (
    select
        intersection,
        weekEnding,
        incidentCategory,
        severityRank,
        count(distinct incidentNumber) as incidentCount
    from staged
    group by intersection, weekEnding, incidentCategory, severityRank
),

-- Determine dominant category per intersection/week
dominantCategory as (
    select
        intersection,
        weekEnding,
        incidentCategory as dominantCategory7d
    from (
        select *,
               row_number() over (
                   partition by intersection, weekEnding
                   order by incidentCount desc, severityRank asc
               ) as rn
        from catCounts
    ) t
    where rn = 1
)

-- Final mart table
select
    s.*,
    wi.intersection7DayTtl,
    wi.intersection7DayAvg,
    wd.district7dTtl,
    wn.neighborhood7dTtl,
    dc.dominantCategory7d,
from staged s
left join weeklyIntersection wi
    on s.intersection = wi.intersection
    and s.weekEnding = wi.weekEnding
left join weeklyDistrict wd
    on s.district = wd.district
    and s.weekEnding = wd.weekEnding
left join weeklyNeighborhood wn
    on s.neighborhood = wn.neighborhood
    and s.weekEnding = wn.weekEnding
left join dominantCategory dc
    on s.intersection = dc.intersection
    and s.weekEnding = dc.weekEnding