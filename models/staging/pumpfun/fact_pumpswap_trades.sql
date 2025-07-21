{{
    config(
        materialized="incremental",
        snowflake_warehouse="SNOWPARK_WAREHOUSE",
        alias="fact_pumpswap_trades",
        unique_key=['ez_swaps_id'],
    )
}}

with encoded_swaps as (
    SELECT 
       *
    FROM {{ ref('fact_pumpswap_trades_encoded') }}
    WHERE (block_timestamp::date > '2025-03-19') --a-nd block_timestamp::date < '2025-04-04')
    {% if is_incremental() %}
        and block_timestamp::date > (select dateadd('day', -3, max(block_timestamp::date)) from {{ this }})
    {% endif %}
)

, pumpswap_idl AS (
  SELECT 
    PARSE_JSON(idls.parquet_raw::STRING) AS idl
  FROM {{ source('SNOWPIPE_DB', 'FACT_ARTEMIS_ABIS') }} idls
  WHERE contract_address = 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA'
)

, pumpswap_idl_old AS (
  SELECT 
    PARSE_JSON(idls.parquet_raw::STRING) AS idl
  FROM {{ source('SNOWPIPE_DB', 'FACT_ARTEMIS_ABIS') }} idls
  WHERE contract_address = 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA_v2'
)

, decoded_swaps as (
    SELECT
        encoded_swaps.*,
        CASE 
            WHEN encoded_swaps.block_timestamp::date < '2025-05-12' THEN PARSE_JSON(DECODE_SOLANA_INNER_INSTRUCTION(pumpswap_idl_old.idl::STRING, encoded_data))
            WHEN encoded_swaps.block_timestamp::date > '2025-05-12' THEN PARSE_JSON(DECODE_SOLANA_INNER_INSTRUCTION(pumpswap_idl.idl::STRING, encoded_data))
        END AS decoded_data
    FROM encoded_swaps, pumpswap_idl, pumpswap_idl_old
)
, decoded_data_extract as (
    select
        d.*,
        CASE 
            WHEN quote_mint = 'DEkqHyPN7GMRJ5cArtQFAWefqbZb33Hyf6s5iCwjEonT' THEN 9
            ELSE p.decimals
        END AS decimals,
        p.price,
        d.decoded_data:protocol_fee::NUMBER as protocol_fee,
        (protocol_fee / POW(10, decimals)) * price as fee,
        d.decoded_data:protocol_fee_basis_points::NUMBER as protocol_fee_basis_points,
        d.decoded_data:protocol_fee_recipient::STRING as protocol_fee_recipient,
        d.decoded_data:coin_creator_fee::NUMBER as creator_fee,
        d.decoded_data:coin_creator_basis_points::NUMBER as creator_fee_basis_points,
        d.decoded_data:coin_creator::STRING as creator,
        d.decoded_data:pool::STRING as pool,
        d.decoded_data:lp_fee::NUMBER as lp_fee,
        d.decoded_data:lp_fee_basis_points::NUMBER as lp_fee_basis_points,
        d.decoded_data:pool_quote_token_reserves::NUMBER as pool_quote_token_reserves
    from decoded_swaps d
    left join {{ source('SOLANA_FLIPSIDE_PRICE', 'ez_prices_hourly') }} p
        on p.hour = date_trunc('hour', block_timestamp) and p.token_address = d.quote_mint
) select * from decoded_data_extract