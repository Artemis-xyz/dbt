{{ config(materialized="view", snowflake_warehouse="OSMOSIS") }}
select
    block_timestamp::date as date,
    count(distinct tx_from) as daa,
    count(*) as txns,
    'osmosis' as chain
from osmosis_flipside.core.fact_transactions
group by date
