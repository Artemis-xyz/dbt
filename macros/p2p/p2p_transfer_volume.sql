{% macro p2p_transfer_volume(chain) %}
with 
    native_transfers as (
        select 
            block_timestamp::date as date,
            sum(amount_usd) as amount
        from {{ ref("fact_"~ chain ~"_p2p_native_transfers")}}
        group by date
    ),
    token_transfers as (
        select
            block_timestamp::date as date,
            sum(amount_usd) as amount
        from {{ ref("fact_"~ chain ~"_p2p_token_transfers")}}
        group by date
    ),
    stablecoin_transfers as (
        select
            block_timestamp::date as date,
            sum(amount_usd) as amount
        from {{ ref("fact_"~ chain ~ "_p2p_stablecoin_transfers")}}
        group by date
    )

select 
    date,
    '{{chain}}' as chain,
    t1.amount as p2p_native_transfer_volume,
    t2.amount as p2p_token_transfer_volume,
    t3.amount as p2p_stablecoin_transfer_volume,
    coalesce(t1.amount, 0) + coalesce(t2.amount, 0) + coalesce(t3.amount, 0)  as p2p_transfer_volume
from native_transfers t1
left join token_transfers t2 using(date)
left join stablecoin_transfers t3 using(date)
order by date
{% endmacro %}