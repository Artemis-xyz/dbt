{{ config(materialized="incremental", unique_key="date") }}

with
    min_date as (
        select min(block_timestamp) as start_timestamp, tx_from
        from sei_flipside.core.fact_transactions as t
        group by tx_from
    ),
    new_users as (
        select
            count(distinct tx_from) as wasm_new_users,
            date_trunc('day', start_timestamp) as start_date
        from min_date
        group by start_date
    ),
    sei_raw_data as (
        select
            trunc(block_timestamp, 'day') as date,
            tx_id,
            tx_from,
            (split(fee, 'usei')[0] / pow(10, 6)) as tx_fee
        from sei_flipside.core.fact_transactions
        where
            date < to_date(sysdate())
            {% if is_incremental() %}
                and block_timestamp >= (select dateadd('day', -3, max(date)) from {{ this }})
            {% endif %}

    ),
    daily as (
        select
            date,
            count(DISTINCT tx_id) as txns,
            count(distinct tx_from) as daa,
            sum(tx_fee) as gas,
            count(DISTINCT tx_id) / 86400 as avg_tps
        from sei_raw_data
        group by date
    ),
    prices as ({{ get_coingecko_price_with_latest("sei-network") }})
select 
    daily.date
    , 'sei' as chain
    , avg_tps as wasm_avg_tps
    , txns as wasm_txns
    , daa as wasm_daa
    , gas as wasm_gas
    , gas * price as wasm_gas_usd
    , 0 as wasm_revenue
    , (daa - wasm_new_users) as wasm_returning_users
    , wasm_new_users
from daily
left join prices on daily.date = prices.date
left join new_users on daily.date = new_users.start_date
where
daily.date < date(sysdate())
order by date
