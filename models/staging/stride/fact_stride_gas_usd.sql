{{
    config(
        materialized="view",
    )
}}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_stride_gas") }}
    ),
    latest_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_stride_gas") }}
        where extraction_date = (select max_date from max_extraction)
    ),
    flattened_data as (
        select
            to_date(value:date::string) as date,
            value:"gas"::float as gas,
            value:"coin"::string as coin
        from latest_data, lateral flatten(input => data)
    ),
    statom_price as ({{ get_coingecko_price_with_latest("stride-staked-atom") }}),
    stride_price as ({{ get_coingecko_price_with_latest("stride") }}),
    stosmo_price as ({{ get_coingecko_price_with_latest("stride-staked-osmo") }}),
    stevm_price as ({{ get_coingecko_price_with_latest("stride-staked-evmos") }}),
    atom_price as ({{ get_coingecko_price_with_latest("cosmos") }}),
    price_data as (
        select *, 'stuatom' as coin
        from statom_price
        union all
        select *, 'ustrd' as coin
        from stride_price
        union all
        select *, 'stuosmo' as coin
        from stosmo_price
        union all
        select *, 'stuevmos' as coin
        from stevm_price
        union all
        select *, 'uatom' as coin
        from atom_price
    ),
    combined_data as (
        select t1.date, gas, gas * coalesce(price, 0) as gas_usd
        from flattened_data t1
        left join price_data on t1.coin = price_data.coin and t1.date = price_data.date

    )
select date, sum(gas_usd) as gas_usd, 'stride' as chain
from combined_data
group by date
