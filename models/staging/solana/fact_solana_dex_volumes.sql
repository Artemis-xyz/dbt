-- This query is used to calculate the daily volume of dex swaps on Solana.
-- It excludes marginfi flash loans and is used to calculate the daily volume of dex swaps on Solana.

{{ config(materialized="table", snowflake_warehouse="ANALYTICS_XL") }}

WITH raydium_volume AS (
    WITH all_marginfi_flash_loans AS (
        SELECT *
        FROM solana_flipside.core.ez_events_decoded
        WHERE program_id = 'MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA' and event_type = 'lendingAccountStartFlashloan'
    )

    SELECT
        DATE_TRUNC('day', block_timestamp) AS date, 
        SUM(
            CASE
                WHEN swap_from_amount_usd IS NOT NULL AND swap_to_amount_usd IS NOT NULL THEN swap_to_amount_usd
                ELSE COALESCE(swap_from_amount_usd, swap_to_amount_usd)
            END
        ) AS trading_volume
    FROM solana_flipside.defi.ez_dex_swaps
    WHERE
    CASE
        WHEN swap_to_amount_usd IS NOT NULL AND swap_from_amount_usd IS NOT NULL AND swap_from_amount_usd > 0 THEN swap_to_amount_usd / swap_from_amount_usd BETWEEN 0.6 AND 1.6
        ELSE COALESCE(swap_to_amount_usd, swap_from_amount_usd) < 10000000  
    END 
    AND swap_program IN (
        'raydium constant product market maker',
        'raydium concentrated liquidity',
        'Raydium Liquidity Pool V4',
        'raydium liquidity pool program id v5'
    )
    AND tx_id NOT IN (SELECT tx_id FROM all_marginfi_flash_loans)
    GROUP BY 1
), 

orca_volume AS (
    WITH all_marginfi_flash_loans AS (
        SELECT *
        FROM solana_flipside.core.ez_events_decoded
        WHERE program_id = 'MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA' and event_type = 'lendingAccountStartFlashloan'
    )

    SELECT
        DATE_TRUNC('day', block_timestamp) AS date, 
        SUM(
            CASE
                WHEN swap_from_amount_usd IS NOT NULL AND swap_to_amount_usd IS NOT NULL THEN swap_to_amount_usd
                ELSE COALESCE(swap_from_amount_usd, swap_to_amount_usd)
            END
        ) AS trading_volume
    FROM solana_flipside.defi.ez_dex_swaps
    WHERE
    CASE
        WHEN swap_to_amount_usd IS NOT NULL AND swap_from_amount_usd IS NOT NULL AND swap_from_amount_usd > 0 THEN swap_to_amount_usd / swap_from_amount_usd BETWEEN 0.6 AND 1.6
        ELSE COALESCE(swap_to_amount_usd, swap_from_amount_usd) < 10000000  
    END 
    AND swap_program IN (
        'orca token swap',
        'ORCA Token Swap V2',
        'orca whirlpool program'
    )
    AND tx_id NOT IN (SELECT tx_id FROM all_marginfi_flash_loans)
    GROUP BY 1
), 

other_volume AS (
    WITH all_marginfi_flash_loans AS (
        SELECT *
        FROM solana_flipside.core.ez_events_decoded
        WHERE program_id = 'MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA' and event_type = 'lendingAccountStartFlashloan'
    )

    SELECT
        DATE_TRUNC('day', block_timestamp) AS date, 
        SUM(
            CASE
                WHEN swap_from_amount_usd IS NOT NULL AND swap_to_amount_usd IS NOT NULL THEN swap_to_amount_usd
                ELSE COALESCE(swap_from_amount_usd, swap_to_amount_usd)
            END
        ) AS trading_volume
    FROM solana_flipside.defi.ez_dex_swaps
    WHERE
    CASE
        WHEN swap_to_amount_usd IS NOT NULL AND swap_from_amount_usd IS NOT NULL AND swap_from_amount_usd > 0 THEN swap_to_amount_usd / swap_from_amount_usd BETWEEN 0.6 AND 1.6
        ELSE COALESCE(swap_to_amount_usd, swap_from_amount_usd) < 10000000  
    END 
    AND swap_program IN (
        'Saber Stable Swap',
        'phoenix',
        'stepn swap',
        'bonkswap'
    )
    AND tx_id NOT IN (SELECT tx_id FROM all_marginfi_flash_loans)
    GROUP BY 1
), 

pump_fun_volume as (
    SELECT
        DATE_TRUNC('day', block_timestamp) AS date, 
        SUM(
            CASE
                WHEN swap_from_amount_usd IS NOT NULL AND swap_to_amount_usd IS NOT NULL THEN swap_to_amount_usd
                ELSE COALESCE(swap_from_amount_usd, swap_to_amount_usd)
            END
        ) AS trading_volume
    FROM solana_flipside.defi.ez_dex_swaps
    WHERE swap_program IN ('pump.fun')
    GROUP BY 1
    ORDER BY 1 ASC
),  

pumpswap_volume as (
    WITH included_pump_txs AS (
        SELECT DISTINCT tx_id
        FROM SOLANA_FLIPSIDE.CORE.EZ_EVENTS_DECODED e,
        LATERAL FLATTEN(input => e.decoded_instruction:accounts) AS flattened
        WHERE
        LOWER(e.program_id) = LOWER('pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA')
        AND flattened.value:name::STRING = 'quote_mint'
        AND (e.decoded_instruction:name = 'sell' OR e.decoded_instruction:name = 'buy')
        AND LOWER(flattened.value:pubkey::STRING) IN (LOWER('So11111111111111111111111111111111111111112'), 
                                                LOWER('mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So'),
                                                LOWER('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'),
                                                LOWER('Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB'),
                                                LOWER('DEkqHyPN7GMRJ5cArtQFAWefqbZb33Hyf6s5iCwjEonT')
            )
    ) 

    SELECT
        DATE_TRUNC('day', block_timestamp) AS date, 
        SUM(
            CASE
                WHEN swap_from_amount_usd IS NOT NULL AND swap_to_amount_usd IS NOT NULL THEN swap_to_amount_usd
                WHEN swap_from_amount_usd IS NULL AND swap_to_amount_usd IS NOT NULL THEN swap_to_amount_usd
                WHEN swap_to_amount_usd IS NULL AND swap_from_amount_usd IS NOT NULL THEN swap_from_amount_usd
                ELSE 0
            END
        ) AS trading_volume
    FROM solana_flipside.defi.ez_dex_swaps AS ez
    INNER JOIN included_pump_txs AS ipt 
        ON LOWER(ez.tx_id) = LOWER(ipt.tx_id)
    WHERE swap_program IN ('pumpswap')
    GROUP BY 1
    ORDER BY 1 DESC
), 

lifinity_volume as (
    SELECT
        date, 
        daily_volume AS trading_volume
    FROM pc_dbt_db.prod.fact_lifinity_dex_volumes   
    ORDER BY 1 ASC
), 

meteora_volume as (
    SELECT
        date, 
        spot_volume AS trading_volume
    FROM meteora.prod_core.ez_metrics
    ORDER BY 1 ASC
), 

jupiter_volume as (
    SELECT
        date, 
        trading_volume + aggregator_volume_overall AS trading_volume
    FROM jupiter.prod_core.ez_metrics
    ORDER BY 1 ASC
)

SELECT
    COALESCE(raydium.date, orca.date, other.date, pump_fun.date, lifinity.date, pumpswap.date, meteora.date, jupiter.date) AS date, 
    COALESCE(raydium.trading_volume, 0) AS raydium_volume, 
    COALESCE(orca.trading_volume, 0) AS orca_volume, 
    COALESCE(other.trading_volume, 0) AS other_volume, 
    COALESCE(pump_fun.trading_volume, 0) AS pump_fun_volume, 
    COALESCE(lifinity.trading_volume, 0) AS lifinity_volume, 
    COALESCE(pumpswap.trading_volume, 0) AS pumpswap_volume, 
    COALESCE(meteora.trading_volume, 0) AS meteora_volume, 
    COALESCE(jupiter.trading_volume, 0) AS jupiter_volume, 
    COALESCE(raydium.trading_volume, 0) + 
    COALESCE(orca.trading_volume, 0) + 
    COALESCE(other.trading_volume, 0) + 
    COALESCE(pump_fun.trading_volume, 0) + 
    COALESCE(lifinity.trading_volume, 0) + 
    COALESCE(pumpswap.trading_volume, 0) + 
    COALESCE(meteora.trading_volume, 0) + 
    COALESCE(jupiter.trading_volume, 0) AS daily_volume_usd
FROM raydium_volume AS raydium
FULL JOIN orca_volume AS orca
    ON raydium.date = orca.date
FULL JOIN other_volume AS other
    ON raydium.date = other.date
FULL JOIN pump_fun_volume AS pump_fun
    ON raydium.date = pump_fun.date
FULL JOIN lifinity_volume AS lifinity
    ON raydium.date = lifinity.date
FULL JOIN pumpswap_volume AS pumpswap
    ON raydium.date = pumpswap.date
FULL JOIN meteora_volume AS meteora
    ON raydium.date = meteora.date
FULL JOIN jupiter_volume AS jupiter
    ON raydium.date = jupiter.date
