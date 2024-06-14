with
    raw_data as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_mantle_daa"),
                    ref("fact_mantle_txns"),
                    ref("fact_mantle_gas"),
                    ref("fact_mantle_expenses"),
                ]
            )
        }}
    ),
    grouped_data as (
        select
            date,
            sum(daa) as daa,
            sum(txns) as txns,
            sum(gas) as gas,
            sum(expenses) as expenses,
            'mantle' as chain
        from raw_data
        group by 1
    ),
    -- USING BITDAO instead of MANTLE because it has larger history
    bitdao_prices as ({{ get_coingecko_price_with_latest("bitdao") }}),
    mnt_prices as ({{ get_coingecko_price_with_latest("mantle") }}),
    mantle_prices as (
        select coalesce(bitdao_prices.date, mnt_prices.date) as date, coalesce(bitdao_prices.price, mnt_prices.price) as price
        from bitdao_prices
        full outer join mnt_prices on bitdao_prices.date = mnt_prices.date
    ),
    eth_prices as ({{ get_coingecko_price_with_latest("ethereum") }}),
    gas_usd_table as (
        select grouped_data.date, gas * price as gas_usd
        from grouped_data
        left join mantle_prices on grouped_data.date = mantle_prices.date
    ),
    expenses_usd_table as (
        select grouped_data.date, expenses * price as expenses_usd, expenses
        from grouped_data
        left join eth_prices on grouped_data.date = eth_prices.date
    ),
    revenue as (
        select gas_usd_table.date, gas_usd - expenses_usd as revenue, expenses_usd, expenses
        from gas_usd_table
        left join expenses_usd_table on gas_usd_table.date = expenses_usd_table.date
    )
select grouped_data.date, daa, txns, gas, gas_usd, revenue, 'mantle' as chain, revenue.expenses as l1_data_cost_native, revenue.expenses_usd as l1_data_cost
from grouped_data
left join gas_usd_table on grouped_data.date = gas_usd_table.date
left join revenue on grouped_data.date = revenue.date
order by 1 asc
