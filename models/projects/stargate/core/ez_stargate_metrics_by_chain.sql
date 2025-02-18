{{
    config(
        materialized="table",
        snowflake_warehouse="STARGATE",
        database="stargate",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

WITH
    aggregated_data AS (
        SELECT 
            src_block_timestamp::date AS date,
            src_chain as chain,
            src_address,
            COUNT(*) AS transactions,
            MIN(src_block_timestamp::date) AS first_seen
        FROM {{ ref("fact_stargate_v2_transfers") }}
        GROUP BY date, src_chain, src_address
    ),
    flows as (
        select date, chain, inflow, outflow
        from {{ ref("fact_stargate_bridge_volume") }}
        where chain is not null
    ), 
    first_seen_global AS (
        SELECT src_address, MIN(first_seen) AS first_seen_date
        FROM aggregated_data
        GROUP BY src_address
    ),

    chain_metrics AS (
        SELECT 
            a.date,
            a.chain,
            SUM(a.transactions) AS txns,
            COUNT(DISTINCT a.src_address) AS dau,
            COUNT(DISTINCT CASE WHEN f.first_seen_date = a.first_seen THEN a.src_address END) AS new_addresses,
            COUNT(DISTINCT CASE WHEN a.transactions > 1 THEN a.src_address END) AS returning_addresses,
        FROM aggregated_data a
        LEFT JOIN first_seen_global f ON a.src_address = f.src_address
        GROUP BY a.date, a.chain
    )

SELECT 
    date
    , chain
    , txns
    , dau
    , new_addresses
    , returning_addresses
    , inflow
    , outflow
FROM chain_metrics
left join flows using (date, chain)
where date < to_date(sysdate())
