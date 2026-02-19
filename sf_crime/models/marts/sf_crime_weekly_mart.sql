with staged as (
    select
        *,
        -- compute the week ending (Saturday)
        date_trunc('day', incident_datetime + (6 - extract(dow from incident_datetime)) * interval '1 day') as week_ending,
        -- label for dropdowns/filters
        to_char(
            date_trunc('day', incident_datetime + (6 - extract(dow from incident_datetime)) * interval '1 day'),
            'Week ending YYYY-MM-DD'
        ) as week_label
    from {{ ref('staged') }}  -- assuming your staged.csv is loaded as a DBT source/table
),

-- Weekly intersection aggregates
weekly_intersection as (
    select
        intersection,
        week_ending,
        count(distinct incident_number) as intersection_7day_ttl,
        count(distinct incident_number)/7.0 as intersection_7day_avg
    from staged
    group by intersection, week_ending
),

-- Weekly district aggregates
weekly_district as (
    select
        police_district,
        week_ending,
        count(*) as district_7d_ttl
    from staged
    group by police_district, week_ending
),

-- Weekly neighborhood aggregates
weekly_neighborhood as (
    select
        analysis_neighborhood,
        week_ending,
        count(*) as neighborhood_7d_ttl
    from staged
    group by analysis_neighborhood, week_ending
),

-- Incident category counts per intersection/week
cat_counts as (
    select
        intersection,
        week_ending,
        incident_category,
        severity_rank,
        count(distinct incident_number) as incident_count
    from staged
    group by intersection, week_ending, incident_category, severity_rank
),

-- Determine dominant category per intersection/week
dominant_category as (
    select
        intersection,
        week_ending,
        incident_category as dominant_category_7d
    from (
        select
            *,
            row_number() over (
                partition by intersection, week_ending
                order by incident_count desc, severity_rank asc
            ) as rn
        from cat_counts
    ) t
    where rn = 1
)

-- Final mart table
select
    s.*,
    wi.intersection_7day_ttl,
    wi.intersection_7day_avg,
    wd.district_7d_ttl,
    wn.neighborhood_7d_ttl,
    dc.dominant_category_7d
from staged s
left join weekly_intersection wi
    on s.intersection = wi.intersection
    and s.week_ending = wi.week_ending
left join weekly_district wd
    on s.police_district = wd.police_district
    and s.week_ending = wd.week_ending
left join weekly_neighborhood wn
    on s.analysis_neighborhood = wn.analysis_neighborhood
    and s.week_ending = wn.week_ending
left join dominant_category dc
    on s.intersection = dc.intersection
    and s.week_ending = dc.week_ending