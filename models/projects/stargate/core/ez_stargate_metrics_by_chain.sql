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
    )
    , flows as (
        select date, chain, inflow, outflow
        from {{ ref("fact_stargate_bridge_volume") }}
        where chain is not null
    ) 
    , treasury_models as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_stargate_v2_arbitrum_treasury_balance"),
                    ref("fact_stargate_v2_avalanche_treasury_balance"),
                    ref("fact_stargate_v2_base_treasury_balance"),
                    ref("fact_stargate_v2_bsc_treasury_balance"),
                    ref("fact_stargate_v2_ethereum_treasury_balance"),
                    ref("fact_stargate_v2_optimism_treasury_balance"),
                    ref("fact_stargate_v2_polygon_treasury_balance"),
                    ref("fact_stargate_v2_mantle_treasury_balance"),
                ],
            )
        }}
    )
    , treasury_metrics as (
        select
            date
            , chain
            , sum(balance) as treasury
        from treasury_models
        where balance > 2 and balance is not null
        group by date, chain
    )
    , tvl_models as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_stargate_v2_arbitrum_tvl"),
                    ref("fact_stargate_v2_avalanche_tvl"),
                    ref("fact_stargate_v2_base_tvl"),
                    ref("fact_stargate_v2_bsc_tvl"),
                    ref("fact_stargate_v2_ethereum_tvl"),
                    ref("fact_stargate_v2_optimism_tvl"),
                    ref("fact_stargate_v2_polygon_tvl"),
                    ref("fact_stargate_v2_mantle_tvl"),
                    ref("fact_stargate_v2_sei_tvl"),
                ],
            )
        }}
    )
    , tvl_metrics as (
        select
            date
            , chain
            , sum(balance) as tvl
        from tvl_models
        where balance > 2 and balance is not null
        group by date, chain
    )
    , first_seen_global AS (
        SELECT src_address, MIN(first_seen) AS first_seen_date
        FROM aggregated_data
        GROUP BY src_address
    )
    , chain_metrics AS (
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
    , hydra_models as (
        {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_stargate_v2_berachain_hydra_assets"),
                    ref("fact_stargate_v2_sei_hydra_assets"),
                ],
            )
        }}
    )
    , hydra_metrics as (
        select
            date
            , chain
            , sum(amount) as hydra_tvl
        from hydra_models
        group by date, chain
    )

SELECT 
    chain_metrics.date
    , 'stargate' as artemis_id
    , chain_metrics.chain

    --Standardized Metrics

    -- Usage Data
    , chain_metrics.txns as bridge_txns
    , chain_metrics.dau as bridge_dau
    , tvl_metrics.tvl as bridge_tvl
    , tvl_metrics.tvl as tvl
    , hydra_metrics.hydra_tvl
    , chain_metrics.new_addresses
    , chain_metrics.returning_addresses
    

    -- Treasury Data
    , treasury_metrics.treasury

    -- Bespoke Metrics
    , flows.inflow
    , flows.outflow

FROM chain_metrics
left join flows using (date, chain)
full outer join treasury_metrics using (date, chain)
left join tvl_metrics using (date, chain)
left join hydra_metrics using (date, chain)
where date < to_date(sysdate())
