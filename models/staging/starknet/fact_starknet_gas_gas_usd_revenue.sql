with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_starknet_gas") }}
    ),
    gas_data as (
        select date(value:"date") as date, value:"value"::float as gas
        from
            {{ source("PROD_LANDING", "raw_starknet_gas") }},
            lateral flatten(input => parse_json(source_json))
        where extraction_date = (select max_date from max_extraction)
    ),
    prices as ({{ get_coingecko_price_with_latest("ethereum") }}),

    expenses as (
        select block_timestamp::date as date, sum(gas_used * gas_price) / 1e9 as gas
        from ethereum_flipside.core.fact_transactions
        where
            -- https://l2beat.com/scaling/projects/starknet
            (
                lower(from_address)
                = lower('0x2C169DFe5fBbA12957Bdd0Ba47d9CEDbFE260CA7')
                or lower(to_address)
                = lower('0xFD14567eaf9ba941cB8c8a94eEC14831ca7fD1b4')
                or lower(to_address)
                = lower('0x47312450B3Ac8b5b8e247a6bB6d523e7605bDb60')
            )
            and block_timestamp >= dateadd(day, -5, (select min(date) from gas_data))
        group by 1
    )
select
    gas_data.date,
    'starknet' as chain,
    gas_data.gas,
    gas_data.gas * prices.price as gas_usd,
    gas_usd - coalesce(expenses.gas * prices.price, 0) as revenue
from gas_data
left join prices on gas_data.date = prices.date
left join expenses on gas_data.date = expenses.date
where gas_data.date < '2024-02-20'
