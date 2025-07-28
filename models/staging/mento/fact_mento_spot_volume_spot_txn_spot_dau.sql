{{config(materialized="table", snowflake_warehouse="MENTO")}}

with
    spot_volume_spot_txn_spot_dau as (
        select
            block_timestamp::date as date
            , chain
            , sum(coalesce(amount_in, amount_out)) as spot_volume
            , count(distinct transaction_hash) as spot_txns
            , count(distinct trader_address) as spot_dau
        from {{ref("fact_mento_celo_event_Swap")}}
        group by 1, 2
    )
select
    date
    , chain
    , spot_volume
    , spot_txns
    , spot_dau
    , sum(spot_volume) over (order by date rows unbounded preceding) as cumulative_spot_volume
from spot_volume_spot_txn_spot_dau


