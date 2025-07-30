-- depends_on {{ ref("fact_bitcoin_issuance_circulating_supply_silver") }}
{{
    config(
        materialized="incremental",
        snowflake_warehouse="BITCOIN",
        database="bitcoin",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=false,
        tags=["ez_metrics"],
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with
    fundamental_data as (
        select
            date,
            sum(txns) as txns,
            sum(daa) as dau,
            sum(fees_native) as fees_native,
            sum(fees) as fees,
            sum(fees) / sum(txns) as avg_txn_fee,
            sum(revenue) as revenue,
            'bitcoin' as chain
        from
            (
                {{
                    dbt_utils.union_relations(
                        relations=[
                            ref("fact_bitcoin_daa"),
                            ref("fact_bitcoin_txns"),
                            ref("fact_bitcoin_fees_revenue"),
                        ]
                    )
                }}
            )
        group by 1
    ),
    issuance_data as ({{ get_issuance_metrics("bitcoin") }}),
    price_data as ({{ get_coingecko_metrics("bitcoin") }}),
    defillama_data as ({{ get_defillama_metrics("bitcoin") }}),
    github_data as ({{ get_github_metrics("bitcoin") }}),
    rolling_metrics as ({{ get_rolling_active_address_metrics("bitcoin") }}),
    etf_metrics as (
        SELECT
            date,
            sum(net_etf_flow_native) as net_etf_flow_native,
            sum(net_etf_flow) as net_etf_flow,
            sum(cumulative_etf_flow_native) as cumulative_etf_flow_native,
            sum(cumulative_etf_flow) as cumulative_etf_flow
        FROM {{ ref("ez_bitcoin_etf_metrics") }}
        GROUP BY 1
    ), 
    bitcoin_dex_volumes as (
        SELECT
            date,
            volume_usd as dex_volumes
        FROM {{ ref("fact_bitcoin_dex_volumes") }}
    )
select
    fundamental_data.date
    , 'bitcoin' as artemis_id

    -- Market Data Metrics
    , price
    , market_cap as mc
    , fdmc
    , tvl

    --Usage Data
    , dau AS chain_dau
    , dau
    , wau AS chain_wau
    , wau
    , mau AS chain_mau
    , mau
    , txns AS chain_txns
    , txns
    , avg_txn_fee AS chain_avg_txn_fee
    , avg_txn_fee
    , bitcoin_dex_volumes.dex_volumes AS chain_spot_volume

    --Fee Data
    , fees_native
    , fees as chain_fees
    , fees
    
    --Financial Statements
    , revenue
    , gross_emissions AS token_incentives
    , revenue - token_incentives AS earnings

    --Supply Data
    , issuance * price AS gross_emissions
    , circulating_supply AS circulating_supply_native
    
    --Developer Data
    , weekly_commits_core_ecosystem
    , weekly_commits_sub_ecosystem
    , weekly_developers_core_ecosystem
    , weekly_developers_sub_ecosystem
    
    --ETF Data
    , net_etf_flow_native
    , net_etf_flow
    , cumulative_etf_flow_native
    , cumulative_etf_flow

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from fundamental_data
left join issuance_data on fundamental_data.date = issuance_data.date
left join price_data on fundamental_data.date = price_data.date
left join defillama_data on fundamental_data.date = defillama_data.date
left join github_data on fundamental_data.date = github_data.date
left join rolling_metrics on fundamental_data.date = rolling_metrics.date
left join etf_metrics on fundamental_data.date = etf_metrics.date
left join bitcoin_dex_volumes on fundamental_data.date = bitcoin_dex_volumes.date
where true
{{ ez_metrics_incremental("fundamental_data.date", backfill_date) }}
and fundamental_data.date < to_date(sysdate())
