{{config(materialized="table", snowflake_warehouse="MENTO")}}


select
    block_timestamp::date as date
    , chain
    , sum(coalesce(amount_in, amount_out)) as spot_volume
    , count(distinct transaction_hash) as spot_txns
    , count(distinct trader_address) as spot_dau
from {{ref("fact_mento_celo_event_Swap")}}
group by 1, 2