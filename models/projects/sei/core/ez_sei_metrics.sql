{{
    config(
        materialized="table",
        snowflake_warehouse="sei",
        database="sei",
        schema="core",
        alias="ez_metrics",
    )
}}
with
    sei_fundamental_metrics as (select * from {{ ref("fact_sei_daa_txns_gas_gas_usd_revenue") }})
    , sei_avg_bps as (select * from {{ ref("fact_sei_avg_bps_silver") }})
    , price_data as ({{ get_coingecko_metrics("sei") }})
    , defillama_data as ({{ get_defillama_metrics("sei") }})
select
    coalesce(f.date, bps.date, price.date, defillama.date) as date
    , 'sei' as chain
    , txns
    , daa as dau
    , gas as fees_native
    , gas_usd as fees
    , revenue
    , avg_bps
    , avg_tps
    , tvl
    , dex_volumes
    , price
    , mc
from sei_fundamental_metrics as f
full join sei_avg_bps as evm_bps using (date)
full join price_data as price using (date)
full join defillama_data as defillama using (date)
where 
coalesce(f.date, bps.date, price.date, defillama.date) < date(sysdate())