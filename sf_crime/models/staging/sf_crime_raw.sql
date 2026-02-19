{{ config(materialized='table') }}

with raw as (
    select *
    from read_csv_auto('sf_crime.csv')
)

select *
from raw