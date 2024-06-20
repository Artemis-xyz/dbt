with
    latest_entry as (
        select
            flat_json.value:"chain"::string as chain,
            flat_json.value:"protocol"::string as app,
            flat_json.value:"pool_type"::string as pool_type,
            flat_json.value:"registration_address"::string as registration_address,
            flat_json.value:"pool_address"::string as pool_address,
            flat_json.value:"token"::string as token,
            flat_json.value:"amplification_coefficient"::int as amplification_coefficient,
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
    max(app) as app,
    'DeFi' as category,
    max(pool_type) as pool_type,
    max(registration_address) as registration_address,
    pool_address,
    max(token) as token,
    max(amplification_coefficient) as amplification_coefficient,
    max(name) as name,
    max(symbol) as symbol,
    max(swap_fee) as swap_fee,
    max(admin_fee) as admin_fee,
    max(mid_fee) as mid_fee,
    max(out_fee) as out_fee,
    max(coin_0) as coin_0,
    max(coin_1) as coin_1,
    max(coin_2) as coin_2,
    max(coin_3) as coin_3,
    max(underlying_coin_0) as underlying_coin_0,
    max(underlying_coin_1) as underlying_coin_1,
    max(underlying_coin_2) as underlying_coin_2,
    max(underlying_coin_3) as underlying_coin_3
from latest_entry
group by pool_address, chain
