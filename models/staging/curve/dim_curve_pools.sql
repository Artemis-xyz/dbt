with
    latest_entry as (
        select
            flat_json.value:"chain"::string as chain,
            flat_json.value:"protocol"::string as app,
            flat_json.value:"pool_type"::string as pool_type,
            flat_json.value:"registration_address"::string as registration_address,
            flat_json.value:"pool_address"::string as pool_address,
            flat_json.value:"token"::string as token,
            flat_json.value:"amplification_coefficient"::int
            as amplification_coefficient,
            flat_json.value:"name"::string as name,
            flat_json.value:"symbol"::string as symbol,
            flat_json.value:"swap_fee"::int as swap_fee,
            flat_json.value:"admin_fee"::int as admin_fee,
            flat_json.value:"mid_fee"::int as mid_fee,
            flat_json.value:"out_fee"::int as out_fee,
            flat_json.value:"coin_0"::string as coin_0,
            flat_json.value:"coin_1"::string as coin_1,
            flat_json.value:"coin_2"::string as coin_2,
            flat_json.value:"coin_3"::string as coin_3,
            flat_json.value:"underlying_coin_0"::string as underlying_coin_0,
            flat_json.value:"underlying_coin_1"::string as underlying_coin_1,
            flat_json.value:"underlying_coin_2"::string as underlying_coin_2,
            flat_json.value:"underlying_coin_3"::string as underlying_coin_3
        from
            {{ source("PROD_LANDING", "raw_dim_curve_pools") }},
            lateral flatten(input => parse_json(source_json)) as flat_json
        where
            extraction_date = (
                select max(extraction_date)
                from {{ source("PROD_LANDING", "raw_dim_curve_pools") }}
            )
    )

select
    chain,
    app,
    'DeFi' as category,
    pool_type,
    registration_address,
    pool_address,
    token,
    amplification_coefficient,
    name,
    symbol,
    swap_fee,
    admin_fee,
    mid_fee,
    out_fee,
    coin_0,
    coin_1,
    coin_2,
    coin_3,
    underlying_coin_0,
    underlying_coin_1,
    underlying_coin_2,
    underlying_coin_3
from latest_entry
