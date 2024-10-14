{{
    config(
        materialized="table",
        snowflake_warehouse="UNISWAP_SM",
        database="uniswap",
        schema="raw",
        alias="fact_dex_token_prices",
    )
}}

with
    dex_prices as (
       {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_uniswap_v2_ethereum_priced_tokens"),
                    ref("fact_uniswap_v3_arbitrum_priced_tokens"),
                    ref("fact_uniswap_v3_avalanche_priced_tokens"),
                    ref("fact_uniswap_v3_base_priced_tokens"),
                    ref("fact_uniswap_v3_blast_priced_tokens"),
                    ref("fact_uniswap_v3_bsc_priced_tokens"),
                    ref("fact_uniswap_v3_ethereum_priced_tokens"),
                    ref("fact_uniswap_v3_optimism_priced_tokens"),
                    ref("fact_uniswap_v3_polygon_priced_tokens"),
                ],
            )
        }}
    )
 select
    block_timestamp
    , 'DeFi' as category
    , event_index
    , tx_hash
    , chain
    , app
    , version
    , source
    , pair
    , token_address
    , symbol
    , price
from dex_prices