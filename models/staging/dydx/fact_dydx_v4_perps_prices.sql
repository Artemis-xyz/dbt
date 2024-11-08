{{
    config({
        "materialized": "incremental",
        "snowflake_warehouse": "DYDX",
    })
}}

with all_dates as (
    select
        extraction_date,
        TO_TIMESTAMP_NTZ(value:block_timestamp)::date as date
    from {{ source("PROD_LANDING", "raw_dydx_v4_price") }},
    lateral flatten (input => parse_json(source_json)) as flat_json
    {% if is_incremental() %}
    where TO_TIMESTAMP_NTZ(value:block_timestamp)::date >= (select max(block_timestamp) from {{ this }})
    {% endif %}
)
, dates as (
    SELECT
        MAX(extraction_date) as max_extraction,
        date
    FROM all_dates
    GROUP BY 2
)
, flattened_json as (
    SELECT
        TO_TIMESTAMP_NTZ(value:block_timestamp)::timestamp as block_timestamp,
        value:tx_hash::string as tx_hash,
        value:E::number as E,
        value:G::number as G,
        value:I::number as I,
        value:perpetual_id::number as perpetual_id,
        extraction_date
    FROM {{ source("PROD_LANDING", "raw_dydx_v4_price") }} raw,
        LATERAL FLATTEN (input => PARSE_JSON(source_json)) AS flat_json
    {% if is_incremental() %}
    where TO_TIMESTAMP_NTZ(value:block_timestamp)::date >= (select max(block_timestamp) from {{ this }})
    {% endif %}
)
select
    f.block_timestamp,
    f.tx_hash,
    f.perpetual_id,
    left(ticker, length(ticker)-4) as symbol,
    abs((f.E + f.G) / f.I) * POW(10, (-6 - m.atomic_resolution)) as price, -- based on equation found here https://docs.dydx.exchange/api_integration-guides/how_to_interpret_block_data_for_trades
    f.E,
    f.G,
    f.I,
    m.clob_pair_id,
    m.atomic_resolution
from flattened_json f
join dates d on d.max_extraction = f.extraction_date AND d.date = date(f.block_timestamp)
left join {{ ref("dim_dydx_perps_markets") }} m on f.perpetual_id = m.clob_pair_id
where m.ticker like '%USD%' -- in case they add non-USD pairs
