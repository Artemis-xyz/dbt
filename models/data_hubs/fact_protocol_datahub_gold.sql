{{ config(
    materialized="table",
) }}

SELECT 
    date,
    dh.artemis_id,
    cat."VALUES" as sectors,
    dau::number as dau,
    txns::number as txns,
    volume::number as volume,
    ecosystem_revenue::number as ecosystem_revenue,
    tvl::number as tvl,
    price::number as price,
    market_cap::number as market_cap,
    fdmc::number as fdmc,
    token_volume::number as token_volume
FROM {{ source('PC_DBT_DB', 'fact_datahub_metrics_silver') }} dh
LEFT JOIN {{ source('POSTGRES_REPLICATED', 'core_asset') }} ca ON REPLACE(ca.artemis_id, '-', '_') = dh.artemis_id 
LEFT JOIN {{ source('POSTGRES_REPLICATED', 'core_assettag') }} cat on cat.asset_id = ca.id AND cat."KEY" = 'groups'