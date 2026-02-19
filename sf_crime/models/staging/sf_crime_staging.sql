{{ config(materialized='table') }}

with raw as (
    select *
    from {{ ref('sf_crime_raw') }}
),

-- Drop nulls in required columns and rows outside SF
clean_required as (
    select *
    from raw
    where "Incident Category" is not null
      and Latitude is not null
      and Longitude is not null
      and "Analysis Neighborhood" is not null
      and "Intersection" is not null
      and "Police District" != 'Out of SF'
),

-- Reformat datetime columns
clean_datetime as (
    select
        *,
        strptime("Incident Datetime", '%Y/%m/%d %I:%M:%S %p') as incidentDatetime,
        strptime("Report Datetime", '%Y/%m/%d %I:%M:%S %p') as reportDatetime
    from clean_required
),

-- Standardize incident category strings
clean_category as (
    select
        *,
        trim("Incident Category") as incidentCategoryRaw,  -- keep original for fallback
        lower(trim("Incident Category")) as incidentCategoryNorm  -- lowercase for matching
    from clean_datetime
),

category_map as (
    select *,
        case
            -- map lowercase variants to canonical nice-case names
            when incidentCategoryNorm = 'weapons offence' then 'Weapons Offense'
            when incidentCategoryNorm = 'weapons carrying etc' then 'Weapons Carrying'
            when incidentCategoryNorm = 'offences against the family and children' then 'Offences Against the Family and Children'
            when incidentCategoryNorm in ('motor vehicle theft?', 'motor vehicle theif') then 'Motor Vehicle Theft'
            when incidentCategoryNorm = 'suspicious occ' then 'Suspicious'
            when incidentCategoryNorm like 'human trafficking%' then 'Human Trafficking'
            when incidentCategoryNorm = 'homicide' then 'Homicide'
            when incidentCategoryNorm = 'rape' then 'Rape'
            when incidentCategoryNorm = 'robbery' then 'Robbery'
            when incidentCategoryNorm = 'assault' then 'Assault'
            when incidentCategoryNorm = 'prostitution' then 'Prostitution'
            when incidentCategoryNorm = 'arson' then 'Arson'
            when incidentCategoryNorm = 'burglary' then 'Burglary'
            when incidentCategoryNorm = 'motor vehicle theft' then 'Motor Vehicle Theft'
            when incidentCategoryNorm = 'larceny theft' then 'Larceny Theft'
            when incidentCategoryNorm = 'stolen property' then 'Stolen Property'
            when incidentCategoryNorm = 'malicious mischief' then 'Malicious Mischief'
            when incidentCategoryNorm = 'vandalism' then 'Vandalism'
            when incidentCategoryNorm = 'fraud' then 'Fraud'
            when incidentCategoryNorm = 'embezzlement' then 'Embezzlement'
            when incidentCategoryNorm = 'forgery and counterfeiting' then 'Forgery and Counterfeiting'
            when incidentCategoryNorm = 'drug offense' then 'Drug Offense'
            else incidentCategoryRaw  -- fallback to original value if unrecognized
        end as incidentCategory
    from clean_category
),

-- Filter canonical categories
canonical as (
    select *
    from category_map
    where incidentCategory in (
        'Homicide','Rape','Robbery','Human Trafficking','Assault',
        'Prostitution','Offences Against the Family and Children',
        'Arson','Burglary','Motor Vehicle Theft','Larceny Theft',
        'Stolen Property','Malicious Mischief','Vandalism',
        'Fraud','Embezzlement','Forgery and Counterfeiting',
        'Drug Offense','Weapons Carrying','Weapons Offense'
    )
),

-- Map severity rank and deduplicate per Incident Number
severity_ranked as (
    select *
    from (
        select *,
            case incidentCategory
                when 'Homicide' then 1
                when 'Rape' then 2
                when 'Robbery' then 3
                when 'Human Trafficking' then 4
                when 'Assault' then 5
                when 'Prostitution' then 6
                when 'Offences Against the Family and Children' then 7
                when 'Arson' then 8
                when 'Burglary' then 9
                when 'Motor Vehicle Theft' then 10
                when 'Larceny Theft' then 11
                when 'Stolen Property' then 12
                when 'Malicious Mischief' then 13
                when 'Vandalism' then 14
                when 'Fraud' then 15
                when 'Embezzlement' then 16
                when 'Forgery and Counterfeiting' then 17
                when 'Drug Offense' then 18
                when 'Weapons Carrying' then 19
                when 'Weapons Offense' then 20
            end as severityRank,
            row_number() over (
                partition by "Incident Number"
                order by 
                    case incidentCategory
                        when 'Homicide' then 1
                        when 'Rape' then 2
                        when 'Robbery' then 3
                        when 'Human Trafficking' then 4
                        when 'Assault' then 5
                        when 'Prostitution' then 6
                        when 'Offences Against the Family and Children' then 7
                        when 'Arson' then 8
                        when 'Burglary' then 9
                        when 'Motor Vehicle Theft' then 10
                        when 'Larceny Theft' then 11
                        when 'Stolen Property' then 12
                        when 'Malicious Mischief' then 13
                        when 'Vandalism' then 14
                        when 'Fraud' then 15
                        when 'Embezzlement' then 16
                        when 'Forgery and Counterfeiting' then 17
                        when 'Drug Offense' then 18
                        when 'Weapons Carrying' then 19
                        when 'Weapons Offense' then 20
                    end asc,
                    incidentDatetime desc
            ) as rn
        from canonical
    ) t
    where rn = 1
),

--  Create broad categories
broad_categories as (
    select *,
        case 
            when incidentCategory in (
                'Homicide','Robbery','Assault','Prostitution','Offences Against the Family and Children',
                'Weapons Carrying','Weapons Offense'
            ) or incidentCategory like 'Human Trafficking%' then 'Violent'
            when incidentCategory in ('Arson','Burglary','Motor Vehicle Theft','Larceny Theft','Stolen Property','Malicious Mischief','Vandalism') then 'Property'
            when incidentCategory in ('Rape','Sex Offense') then 'Sexual'
            when incidentCategory in ('Fraud','Embezzlement','Forgery and Counterfeiting') then 'Fiscal/Fraud'
            when incidentCategory = 'Drug Offense' then 'Drug'
            else 'Other'
        end as incidentCategoryBroad
    from severity_ranked
),

-- Extract date parts
final as (
    select *,
        extract(year from incidentDatetime) as year,
        strftime(incidentDatetime, '%b') as month,
        extract(day from incidentDatetime) as day,
        extract(hour from incidentDatetime) as hour,
        case when extract(hour from incidentDatetime) between 6 and 17 then 'day' else 'night' end as nightOrDay
    from broad_categories
)

select
    "Incident Number" as incidentNumber,
    incidentDatetime,
    reportDatetime,
    incidentCategory,
    incidentCategoryBroad,
    severityRank,
    "Incident Day of Week" as incidentDayOfWeek,
    "Analysis Neighborhood" as neighborhood,
    "Police District" as district,
    Latitude as latitude,
    Longitude as longitude,
    Resolution as resolution,
    "Intersection" as intersection,
    year,
    month,
    day,
    hour,
    nightOrDay
from final