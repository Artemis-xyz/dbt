{{ config(materialized="table") }}
select date, total_reward, block_reward, fees, chain
from {{ ref("fact_bitcoin_miner_fees") }}
