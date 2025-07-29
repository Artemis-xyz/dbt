{{
    config(
        materialized="incremental",
        snowflake_warehouse="CARDANO",
        database="cardano",
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
            max(txns) as txns,
            max(daa) as dau,
            max(gas_usd) as fees,
            max(gas) as fees_native,
            max(revenue_native) as revenue_native,
            max(revenue) as revenue,
            max(max_supply_native) as max_supply_native,
            max(total_supply_native) as total_supply_native,
            max(issued_supply_native) as issued_supply_native,
            max(treasury_native) as treasury_native,
            'cardano' as chain
        from (
            {{
                dbt_utils.union_relations(
                    relations=[
                        ref("fact_cardano_daa"),
                        ref("fact_cardano_txns"),
                        ref("fact_cardano_fees_and_revenue"),
                        ref("fact_cardano_supply")
                    ]
                )
            }}
        )
        group by 1
    ),
    price_data as ({{ get_coingecko_metrics("cardano") }}),
    defillama_data as ({{ get_defillama_metrics("cardano") }}),
    github_data as ({{ get_github_metrics("cardano") }}),
    nft_metrics as ({{ get_nft_metrics("cardano") }})  

select
    f.date
    , f.chain
    , txns
    , dau
    , fees_native
    , fees
    , revenue
    , revenue_native
    , fees / txns as avg_txn_fee
    , dex_volumes
    , nft_trading_volume
    -- Standardized Metrics
    
    -- Market Data
    , price
    , market_cap
    , fdmc
    , token_volume
    
    -- Chain Metrics
    , txns as chain_txns
    , dau as chain_dau
    , avg_txn_fee as chain_avg_txn_fee
    , dex_volumes as chain_spot_volume
    , nft_trading_volume as chain_nft_trading_volume
    
    -- Cash Flow Metrics
    , fees as chain_fees
    
    -- Crypto Metrics
    , tvl
    
    -- Supply Metrics
    , max_supply_native
    , total_supply_native
    , issued_supply_native
    -- There are no unvested tokens
    , issued_supply_native - 0 AS circulating_supply_native
    
    -- Protocol Metrics
    , treasury_native
    , treasury_native * price AS treasury
    , treasury_native AS own_token_treasury_native
    , treasury_native * price AS own_token_treasury
    , 0 AS net_treasury_native
    , 0 * price AS net_treasury

    -- Developer Metrics
    , weekly_commits_core_ecosystem
    , weekly_commits_sub_ecosystem
    , weekly_developers_core_ecosystem
    , weekly_developers_sub_ecosystem
    , token_turnover_circulating
    , token_turnover_fdv
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from fundamental_data f
left join price_data on f.date = price_data.date
left join defillama_data on f.date = defillama_data.date
left join github_data on f.date = github_data.date
left join nft_metrics on f.date = nft_metrics.date
where true
{{ ez_metrics_incremental('f.date', backfill_date) }}
and f.date < to_date(sysdate())