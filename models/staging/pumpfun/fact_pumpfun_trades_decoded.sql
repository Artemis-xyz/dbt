
{{
    config(
        materialized="incremental",
        unique_key=['ez_swaps_id'],
        snowflake_warehouse="SNOWPARK_WAREHOUSE",
        alias="fact_pumpfun_trades_decoded",
    )
}}

with encoded_data as (
    select * 
    from {{ ref('fact_pumpfun_trades_encoded') }}
    where (program_id = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P')
    AND (substr(encoded_data, 0, 9) = '2zjR1PvPv') 
    {% if is_incremental() %}
        and block_timestamp::date > (select dateadd('day', -3, max(block_timestamp::date)) from {{ this }})
    {% endif %}
)
, pump_idl AS (
  SELECT 
    PARSE_JSON(idls.parquet_raw::STRING) AS idl
  FROM {{ source('SNOWPIPE_DB', 'FACT_ARTEMIS_ABIS') }} idls
  WHERE contract_address = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P'
  limit 1
)
, decoded_data AS (
  SELECT 
    encoded_data.*,
    PARSE_JSON(DECODE_SOLANA_INNER_INSTRUCTION(pump_idl.idl::STRING, encoded_data)) AS decoded_data
  FROM encoded_data, pump_idl
)
, decoded_extract as (
    SELECT 
      decoded_data.*,
      decoded_data:fee::NUMBER / 1e9 AS fee_native,
      fee_native * price as fee,
      decoded_data:fee_basis_points::NUMBER AS fee_basis_points,
      decoded_data:fee_recipient::STRING AS fee_recipient,
      decoded_data:creator_fee::NUMBER / 1e9 AS creator_fee_native,
      creator_fee_native * price as creator_fee,
      decoded_data:creator_fee_basis_points::NUMBER AS creator_fee_basis_points,
      decoded_data:creator::STRING AS creator
    FROM decoded_data
    left join {{ source('SOLANA_FLIPSIDE_PRICE', 'ez_prices_hourly') }} p
        on p.hour = date_trunc('hour', block_timestamp) and p.token_address = 'So11111111111111111111111111111111111111112'
)
select * from decoded_extract