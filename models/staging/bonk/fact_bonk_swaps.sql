{{
    config(
        materialized='incremental',
        unique_key='tx_id',
        snowflake_warehouse='BONK',
    )
}}

with data as (
    select 
        sum(IFF(tx_from = 'WLHv2UAZm6z4KyaaELi5pjdbJh6RESMva1Rnn8pJVVh', amount, NULL)) as token_out_amount
        , max(IFF(tx_from = 'WLHv2UAZm6z4KyaaELi5pjdbJh6RESMva1Rnn8pJVVh', mint, NULL)) as token_out
        , sum(IFF(tx_to = 'WLHv2UAZm6z4KyaaELi5pjdbJh6RESMva1Rnn8pJVVh', amount, NULL)) as token_in_amount
        , max(IFF(tx_to = 'WLHv2UAZm6z4KyaaELi5pjdbJh6RESMva1Rnn8pJVVh', mint, NULL)) as token_in
        , max(signers[0]::string) as swapper
        ,  max(date_trunc('day', block_timestamp)) as date
        , tx_id
        , count(*) as number_of_swaps
    from solana_flipside.core.fact_transfers 
    where (tx_from = 'WLHv2UAZm6z4KyaaELi5pjdbJh6RESMva1Rnn8pJVVh' or tx_to = 'WLHv2UAZm6z4KyaaELi5pjdbJh6RESMva1Rnn8pJVVh')
    {% if is_incremental() %}
        and block_timestamp >= dateadd(day, -3, to_date(sysdate()))
    {% else %}
        and block_timestamp >= '2025-04-20'
    {% endif %}
    group by tx_id
)
, solana_prices as (
        select date as date, shifted_token_price_usd as price
    from pc_dbt_db.prod.fact_coingecko_token_date_adjusted_gold
    where
        coingecko_id = 'solana'
        and date < dateadd(day, -1, to_date(sysdate()))
    union
    select dateadd('day', -1, to_date(sysdate())) as date, token_current_price as price
    from pc_dbt_db.prod.fact_coingecko_token_realtime_data
    where token_id = 'solana'
)
select 
    data.date
    , swapper
    , token_in
    , token_in_amount
    , token_out
    , token_out_amount
    , price
    , tx_id
    , case 
        when token_in in ('So11111111111111111111111111111111111111112', 'So11111111111111111111111111111111111111111')
        then token_in_amount * price
        when token_out in ('So11111111111111111111111111111111111111112', 'So11111111111111111111111111111111111111111')
        then token_out_amount * price
        else 0
    end as amount_usd
from data
left join solana_prices on data.date = solana_prices.date
where number_of_swaps = 2
order by amount_usd desc
