{{ config(
    materialized="table",
    snowflake_warehouse="METAPLEX"
) }}

all_metaplex_transactions AS (
    SELECT
        tx_id,
        program_id,
        DATE_TRUNC('DAY', block_timestamp) AS day
    FROM
        solana_flipside.core.fact_events
    WHERE
        program_id IN (SELECT program_id FROM {{ ref('fact_metaplex_programs') }})
        AND succeeded = TRUE
    
    UNION

    SELECT
        tx_id,
        program_id,
        DATE_TRUNC('DAY', block_timestamp) AS day
    FROM
        solana_flipside.core.fact_events_inner
    WHERE
        program_id IN (SELECT program_id FROM {{ ref('fact_metaplex_programs') }})
        AND succeeded = TRUE
),

all_days AS (
    SELECT DISTINCT day FROM all_metaplex_transactions
),

program_day_grid AS (
    SELECT 
        mp.program_id,
        mp.program_name,
        ad.day
    FROM 
        {{ ref('fact_metaplex_programs') }} mp
    CROSS JOIN 
        all_days ad
),

daily_transaction_counts AS (
    SELECT
        amt.program_id,
        amt.day,
        COUNT(DISTINCT amt.tx_id) AS daily_signed_transactions
    FROM
        all_metaplex_transactions amt
    GROUP BY
        amt.program_id,
        amt.day
),

daily_transaction_counts_full AS (
    SELECT
        pg.program_id,
        pg.program_name,
        pg.day,
        COALESCE(dtc.daily_signed_transactions, 0) AS daily_signed_transactions
    FROM
        program_day_grid pg
    LEFT JOIN
        daily_transaction_counts dtc
        ON pg.program_id = dtc.program_id AND pg.day = dtc.day
),

cumulative_transactions AS (
    SELECT
        dtcf.program_id,
        dtcf.program_name,
        dtcf.day,
        dtcf.daily_signed_transactions,
        SUM(dtcf.daily_signed_transactions) OVER (
            PARTITION BY dtcf.program_id 
            ORDER BY dtcf.day ASC 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_signed_transactions
    FROM
        daily_transaction_counts_full dtcf
),

pivoted_transactions AS (
    SELECT
        day,
        MAX(CASE WHEN program_name = 'Token Metadata' THEN cumulative_signed_transactions END) AS token_metadata,
        MAX(CASE WHEN program_name = 'Bubblegum' THEN cumulative_signed_transactions END) AS bubblegum,
        MAX(CASE WHEN program_name = 'Core' THEN cumulative_signed_transactions END) AS core,
        MAX(CASE WHEN program_name = 'Candy Machine v3' THEN cumulative_signed_transactions END) AS candy_machine_v3,
        MAX(CASE WHEN program_name = 'Candy Machine v2' THEN cumulative_signed_transactions END) AS candy_machine_v2,
        MAX(CASE WHEN program_name = 'Candy Machine v1' THEN cumulative_signed_transactions END) AS candy_machine_v1,
        MAX(CASE WHEN program_name = 'Auction House' THEN cumulative_signed_transactions END) AS auction_house,
        MAX(CASE WHEN program_name = 'Auctioneer' THEN cumulative_signed_transactions END) AS auctioneer,
        MAX(CASE WHEN program_name = 'Candy Guard' THEN cumulative_signed_transactions END) AS candy_guard,
        MAX(CASE WHEN program_name = 'Core Candy Guard' THEN cumulative_signed_transactions END) AS core_candy_guard,
        MAX(CASE WHEN program_name = 'Core Candy Machine' THEN cumulative_signed_transactions END) AS core_candy_machine,
        MAX(CASE WHEN program_name = 'Gumdrop' THEN cumulative_signed_transactions END) AS gumdrop,
        MAX(CASE WHEN program_name = 'Hydra' THEN cumulative_signed_transactions END) AS hydra,
        MAX(CASE WHEN program_name = 'Inscriptions' THEN cumulative_signed_transactions END) AS inscriptions,
        MAX(CASE WHEN program_name = 'MPL-Hybrid' THEN cumulative_signed_transactions END) AS mpl_hybrid,
        MAX(CASE WHEN program_name = 'Token Auth Rules' THEN cumulative_signed_transactions END) AS token_auth_rules,
        SUM(cumulative_signed_transactions) AS cumulative_all_programs
    FROM
        cumulative_transactions
    GROUP BY
        day
)

SELECT 
    TO_CHAR(day, 'YYYY-MM-DD') AS date,
    token_metadata,
    bubblegum,
    core,
    candy_machine_v3,
    candy_machine_v2,
    candy_machine_v1,
    auction_house,
    auctioneer,
    candy_guard,
    core_candy_guard,
    core_candy_machine,
    gumdrop,
    hydra,
    inscriptions,
    mpl_hybrid,
    token_auth_rules,
    cumulative_all_programs
FROM 
    pivoted_transactions
ORDER BY 
    date DESC;
