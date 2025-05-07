{{ config(materialized="table") }}
with
    prices as ({{ get_coingecko_price_with_latest("ethereum") }}),
    linea_submission_and_finalization_transactions as (
        select distinct tx_hash,
        from ethereum_flipside.core.ez_decoded_event_logs
        where
            lower(contract_address)
            = lower('0xd19d4B5d358258f05D7B411E21A1460D11B0876F')
            and event_name in ('DataSubmitted', 'DataFinalized', 'BlockFinalized')
    ),
    linea_expenses as (
        select t.block_timestamp::date as date, sum(gas_used * gas_price / 1e9) as gas,
        from linea_submission_and_finalization_transactions
        join
            ethereum_flipside.core.fact_transactions t
            on linea_submission_and_finalization_transactions.tx_hash = t.tx_hash
        group by date
    ),
    fees as (
        select
            to_timestamp(block_timestamp)::date as date,
            sum((gas_used * gas_price) / 1e18) as gas,
        from {{ ref("fact_linea_transactions") }}
        group by date
    )

select
    fees.date,
    'linea' as chain,
    fees.gas as gas,
    fees.gas * prices.price as gas_usd,
    coalesce(linea_expenses.gas, 0) as l1_data_cost_native,
    coalesce(linea_expenses.gas, 0) * prices.price as l1_data_cost,
    fees.gas - l1_data_cost_native as revenue_native,
    gas_usd - l1_data_cost as revenue
from fees
left join linea_expenses on fees.date = linea_expenses.date
left join prices on fees.date = prices.date
where fees.date < to_date(sysdate())
order by date desc
