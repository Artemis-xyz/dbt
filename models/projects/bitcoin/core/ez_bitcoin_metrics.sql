-- depends_on {{ ref("fact_bitcoin_issuance_circulating_supply_silver") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="BITCOIN",
        database="bitcoin",
        schema="core",
        alias="ez_metrics",
    )
}}

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
    , fundamental_data.chain
    , txns
    , dau
    , wau
    , mau
    , fees_native
    , fees
    , avg_txn_fee
    , revenue
    , issuance
    , circulating_supply
    , bitcoin_dex_volumes.dex_volumes
    -- Standardized Metrics
    -- Market Data Metrics
    , price
    , market_cap
    , fdmc
    , tvl
    -- Chain Usage Metrics
    , dau AS chain_dau
    , wau AS chain_wau
    , mau AS chain_mau
    , txns AS chain_txns
    , avg_txn_fee AS chain_avg_txn_fee
    , bitcoin_dex_volumes.dex_volumes AS chain_spot_volume
    -- Cashflow metrics
    , fees as chain_fees
    
    -- Supply Metrics
    , issuance AS gross_emissions_native
    , issuance * price AS gross_emissions
    , circulating_supply AS circulating_supply_native
    , gross_emissions AS token_incentives
    , revenue - token_incentives AS earnings
    -- Developer metrics
    , weekly_commits_core_ecosystem
    , weekly_commits_sub_ecosystem
    , weekly_developers_core_ecosystem
    , weekly_developers_sub_ecosystem
    -- ETF Metrics
    , net_etf_flow_native
    , net_etf_flow
    , cumulative_etf_flow_native
    , cumulative_etf_flow
from fundamental_data
left join issuance_data on fundamental_data.date = issuance_data.date
left join price_data on fundamental_data.date = price_data.date
left join defillama_data on fundamental_data.date = defillama_data.date
left join github_data on fundamental_data.date = github_data.date
left join rolling_metrics on fundamental_data.date = rolling_metrics.date
left join etf_metrics on fundamental_data.date = etf_metrics.date
left join bitcoin_dex_volumes on fundamental_data.date = bitcoin_dex_volumes.date
where fundamental_data.date < to_date(sysdate())
