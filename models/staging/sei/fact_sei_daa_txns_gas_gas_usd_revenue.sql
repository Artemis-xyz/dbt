{{ config(materialized="incremental", unique_key="date") }}

with
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
                and block_timestamp >= (select max(date) from {{ this }})
            {% endif %}

    ),
    daily as (
        select
            date,
            count(tx_id) as txns,
            count(distinct tx_from) as daa,
            sum(tx_fee) as gas
        from sei_raw_data
        group by date
    ),
    prices as ({{ get_coingecko_price_with_latest("sei-network") }})
select daily.date, 'sei' as chain, txns, daa, gas, gas * price as gas_usd, 0 as revenue
from daily
left join prices on daily.date = prices.date
order by date
