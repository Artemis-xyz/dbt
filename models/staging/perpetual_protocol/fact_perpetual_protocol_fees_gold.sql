{{ config(materialized="table") }}
select chain, date, app, fees, category
from {{ ref("fact_perpetual_protocol_fees") }}
