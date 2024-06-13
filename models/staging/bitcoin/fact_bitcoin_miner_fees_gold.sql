{{ config(materialized="table", snowflake_warehouse="BITCOIN") }}
select date, total_reward, block_reward, fees, chain
from {{ ref("fact_bitcoin_miner_fees") }}
