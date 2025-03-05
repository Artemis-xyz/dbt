{{ config(
    materialized="incremental",
    snowflake_warehouse="METAPLEX",
    unique_key= ["date", "program_id"]
) }}

with all_metaplex_transactions AS (
    SELECT
        DATE_TRUNC('DAY', block_timestamp) AS date,
        tx_id,
        program_id
    FROM
        {{ ref('fact_filtered_metaplex_solana_events') }}
        WHERE LOWER(program_id) <> LOWER('BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY')
        {% if is_incremental() %}
            and block_timestamp > (SELECT MAX(date) FROM {{ this }})
        {% else %}
            and block_timestamp >= date('2021-08-01')
        {% endif %}

    UNION

    SELECT
        DATE_TRUNC('DAY', block_timestamp) AS date,
        tx_id,
        program_id
    FROM
        {{ source('SOLANA_FLIPSIDE', 'fact_events_inner') }}
    WHERE
        program_id IN (SELECT program_id FROM {{ ref('fact_metaplex_programs') }})
        AND succeeded = TRUE
        AND LOWER(program_id) <> LOWER('BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY')
        {% if is_incremental() %}
            AND block_timestamp > (SELECT MAX(date) FROM {{ this }})
        {% else %}
            AND block_timestamp >= date('2021-08-01')
        {% endif %}
),

all_days AS (
    SELECT DISTINCT date FROM all_metaplex_transactions
),

program_day_grid AS (
    SELECT 
        ad.date,
        mp.program_id,
        mp.program_name
    FROM 
        {{ ref('fact_metaplex_programs') }} mp
    CROSS JOIN 
        all_days ad
    WHERE LOWER(mp.program_id) <> LOWER('BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY')
),

daily_transaction_counts AS (
    SELECT
        amt.date,
        amt.program_id,
        COUNT(DISTINCT amt.tx_id) AS daily_signed_transactions
    FROM
        all_metaplex_transactions amt
    GROUP BY
        amt.program_id,
        amt.date
),

daily_transaction_counts_full AS (
    SELECT
        pg.program_id,
        pg.program_name,
        pg.date,
        COALESCE(dtc.daily_signed_transactions, 0) AS daily_signed_transactions
    FROM
        program_day_grid pg
    LEFT JOIN
        daily_transaction_counts dtc
        ON pg.program_id = dtc.program_id AND pg.date = dtc.date
    WHERE LOWER(pg.program_id) <> LOWER('BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY')
),

cumulative_transactions AS (
    SELECT
        dtcf.program_id,
        dtcf.program_name,
        dtcf.date,
        dtcf.daily_signed_transactions,
        SUM(dtcf.daily_signed_transactions) OVER (
            PARTITION BY dtcf.program_id 
            ORDER BY dtcf.date ASC 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_signed_transactions
    FROM
        daily_transaction_counts_full dtcf
)

SELECT 
    date(date) AS date,
    program_id,
    program_name,
    daily_signed_transactions,
    cumulative_signed_transactions
FROM 
    cumulative_transactions
WHERE date < to_date(sysdate()) and LOWER(program_id) <> LOWER('BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY')
ORDER BY 
    date DESC
