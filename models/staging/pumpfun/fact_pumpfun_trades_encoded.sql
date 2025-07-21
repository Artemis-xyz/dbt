
{{
    config(
        materialized="incremental",
        unique_key=['ez_swaps_id'],
        snowflake_warehouse="ANALYTICS_XL",
        alias="fact_pumpfun_trades_encoded",
    )
}}

--collect inner Anchor CPI events based on encoded CPI instruction data from new IDL (after May 11 2025)
WITH inner_event_with_data as (
    SELECT 
        *,
        instruction:data::STRING as encoded_data,
        CASE 
            WHEN inner_index is null THEN ROW_NUMBER() OVER (PARTITION BY tx_id ORDER BY (instruction_index * 1000))
            WHEN inner_index is not null THEN ROW_NUMBER() OVER (PARTITION BY tx_id ORDER BY (instruction_index * 1000 + inner_index))
        END AS event_number,
        CONCAT(tx_id,'-',event_number, '-pump.fun') AS log_id
    FROM {{ source('SOLANA_FLIPSIDE', 'fact_events_inner') }}
    WHERE (program_id = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P')
    AND (substr(encoded_data, 0, 9) = '2zjR1PvPv')
    AND block_timestamp::date > '2025-05-11'
    {% if is_incremental() %}
        and block_timestamp::date > (select dateadd('day', -3, max(block_timestamp::date)) from {{ this }})
    {% endif %}
), ez_dex_swaps_encoded_fees as (
    SELECT
        dex_swaps.*,
        inner_events.encoded_data
    FROM {{ source('SOLANA_FLIPSIDE_DEFI', 'ez_dex_swaps') }} as dex_swaps
    LEFT JOIN inner_event_with_data as inner_events
        ON dex_swaps._log_id = inner_events.log_id
    WHERE dex_swaps.program_id = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P'
    AND dex_swaps.block_timestamp::date > '2025-05-11'
    {% if is_incremental() %}
        and dex_swaps.block_timestamp::date > (select dateadd('day', -3, max(block_timestamp::date)) from {{ this }})
    {% endif %}
) select * from ez_dex_swaps_encoded_fees

