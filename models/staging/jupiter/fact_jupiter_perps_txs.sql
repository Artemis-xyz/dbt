{{
    config(
        materialized='incremental',
        snowflake_warehouse='JUPITER',
    )
}}

with hex_cte as (
    SELECT
        block_timestamp,
        tx_id,
        PC_DBT_DB.PROD.BASE58_TO_HEX(f.value:data) as hex_data,
        f.value:data as base58_data
    FROM solana_flipside.core.fact_events,
    LATERAL FLATTEN(input => get_path(inner_instruction, 'instructions')) AS f
    where
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        block_timestamp >= (select dateadd('day', -7, max(block_timestamp)) from {{ this }})
    {% else %}
        block_timestamp > date('2023-07-17')
    {% endif %}
    and program_id = 'PERPHjGBqRHArX4DySjwM6UJHiR3sWAatqfdBS2qQJu'
    and f.value:data is not null
    and succeeded = 1
)
, agg as (
    SELECT
        block_timestamp,
        tx_id,
        'inc' as type,
        PC_DBT_DB.PROD.BIG_ENDIAN_HEX_TO_DECIMAL(SUBSTRING(hex_data,582+1,16))/1e6 as size_usd, -- position size fee
        PC_DBT_DB.PROD.BIG_ENDIAN_HEX_TO_DECIMAL(SUBSTRING(hex_data,680+1,16))/1e6 as fees_usd, --open fee
        PC_DBT_DB.PROD.HEX_TO_BASE58(SUBSTRING(hex_data,454+1,64)) as owner, -- owner/trader address
        PC_DBT_DB.PROD.HEX_TO_BASE58(SUBSTRING(hex_data,243,64)) as mint, 
        PC_DBT_DB.PROD.BIG_ENDIAN_HEX_TO_DECIMAL(SUBSTRING(hex_data,630+1,16))/1e6 as price,
        hex_data
    FROM hex_cte
    WHERE SUBSTRING(hex_data,17,16) = 'f5715534d6bb9984' -- IncreasePosition

    UNION ALL

    SELECT
        block_timestamp,
        tx_id,
        'dec' as type,
        PC_DBT_DB.PROD.BIG_ENDIAN_HEX_TO_DECIMAL(SUBSTRING(hex_data, 584+1,16))/1e6 as size_usd, --open fee
        case when substring(hex_data,1+651,1) = 1
            then PC_DBT_DB.PROD.BIG_ENDIAN_HEX_TO_DECIMAL(SUBSTRING(hex_data,1+668,16)) / 1e6
            else PC_DBT_DB.PROD.BIG_ENDIAN_HEX_TO_DECIMAL(SUBSTRING(hex_data,1+652,16)) / 1e6
            end as fee_usd, --close fee, has an optional param priceSlippage before it so we need this case when.
        PC_DBT_DB.PROD.HEX_TO_BASE58(SUBSTRING(hex_data,456+1,64)) as owner, -- owner/trader address
        PC_DBT_DB.PROD.HEX_TO_BASE58(SUBSTRING(hex_data,243,64)) as mint,
        -- Price calculation with dynamic offset
        PC_DBT_DB.PROD.BIG_ENDIAN_HEX_TO_DECIMAL(
            SUBSTRING(
                hex_data,
                619 -- Base offset to price
                + (CASE WHEN SUBSTRING(hex_data, 618, 1) = '1'
                        THEN 16
                        ELSE 0
                    END) -- Add offset if transferToken present
                , 16
            )
        )/1e6 as price,        
        hex_data
    FROM hex_cte
    WHERE substring(hex_data,1+16,16) = '409c2b4a6d83107f' -- DecreasePosition

    UNION ALL


    SELECT
        block_timestamp,
        tx_id,
        'liq' as type,
        PC_DBT_DB.PROD.BIG_ENDIAN_HEX_TO_DECIMAL(SUBSTRING(hex_data,1+354,16)) / 1e6 as size_usd, --close fee
        PC_DBT_DB.PROD.BIG_ENDIAN_HEX_TO_DECIMAL(SUBSTRING(hex_data,1+564,16)) / 1e6 as fee_usd, --close fee
        PC_DBT_DB.PROD.HEX_TO_BASE58(SUBSTRING(hex_data,1+388,64)) as owner, -- owner/trader address
        PC_DBT_DB.PROD.HEX_TO_BASE58(SUBSTRING(hex_data,291,64)) as mint, 
        case when block_timestamp >= date('2023-10-16 07:33:16.000')
                THEN PC_DBT_DB.PROD.BIG_ENDIAN_HEX_TO_DECIMAL(SUBSTRING(hex_data,549,16))/1e6 
            ELSE
                PC_DBT_DB.PROD.BIG_ENDIAN_HEX_TO_DECIMAL(SUBSTRING(hex_data,531,16))/1e6 
        END as price,
        hex_data    
    FROM hex_cte
    WHERE substring(hex_data,1+16,16) IN ('68452084d423bf2f', '806547a880485654') --LiquidatePosition, LiquidateFullPosition
)
SELECT
    block_timestamp,
    tx_id,
    'solana' as chain,
    'jupiter' as app,
    size_usd,
    fees_usd as fee_usd,
    owner,
    mint,
    price,
    hex_data
FROM agg
