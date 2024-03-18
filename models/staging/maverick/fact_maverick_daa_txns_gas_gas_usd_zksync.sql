{{
    config(
        materialized="table",
        unique_key="date",
        snowflake_warehouse="FUNDAMENTAL_METRICS_WAREHOUSE_SM",
    )
}}

with
    dates as (
        select extraction_date, flat_json.value:"hash"::string as tx_hash
        from
            {{ source("PROD_LANDING", "raw_maverick_zksync_data") }} t1,
            lateral flatten(input => parse_json(source_json)) as flat_json
        group by tx_hash, extraction_date
    ),
    max_extraction_per_day as (
        select tx_hash, max(extraction_date) as extraction_date
        from dates
        group by tx_hash
        order by tx_hash
    ),
    flattened_json as (
        select
            extraction_date,
            to_date(flat_json.value:"block_date"::string) as date,
            flat_json.value:"from_address"::string as from_address,
            flat_json.value:"hash"::string as tx_hash,
            flat_json.value:"tx_fee"::float as tx_fee
        from
            {{ source("PROD_LANDING", "raw_maverick_zksync_data") }},
            lateral flatten(input => parse_json(source_json)) as flat_json
    ),
    map_reduce_json as (
        select t1.*
        from flattened_json t1
        left join max_extraction_per_day t2 on t1.tx_hash = t2.tx_hash
        where t1.extraction_date = t2.extraction_date
    ),
    prices as ({{ get_coingecko_price_with_latest("ethereum") }}),
    txn_data as (
        select distinct
            t1.date,
            t1.tx_hash,
            t1.from_address,
            t1.tx_fee as gas,
            prices.price as price,
            t1.tx_fee * prices.price as gas_usd
        from map_reduce_json t1
        left join prices on t1.date = prices.date
        where
            t1.date < to_date(sysdate())
            {% if is_incremental() %}
                and t1.block_timestamp
                >= (select max(date) + interval '1 DAY' from {{ this }})
            {% endif %}

    )
select
    date,
    'zksync' as chain,
    'maverick_protocol' as app,
    'DeFi' as category,
    count(distinct tx_hash) as txns,
    count(distinct from_address) as daa,
    sum(gas) as gas,
    sum(gas_usd) as gas_usd
from txn_data
group by date
order by date desc
