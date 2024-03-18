{{ config(materialized="table") }}
select date, native_token_fees, fees, chain
from {{ ref("fact_stacks_native_fees") }}
