{{ config(materialized="table") }}

SELECT 
    date,
    dh.artemis_id,
    cat."VALUES" as sectors,
    dau::number as dau,
    txns::number as txns,
    gross_protocol_revenue::number as gross_protocol_revenue,
    tvl::number as tvl
FROM {{ source('PC_DBT_DB', 'fact_datahub_silver') }} dh
LEFT JOIN {{ source('POSTGRES_REPLICATED', 'core_asset') }} ca ON ca.artemis_id = dh.artemis_id 
LEFT JOIN {{ source('POSTGRES_REPLICATED', 'core_assettag') }} cat on cat.asset_id = ca.id AND cat."KEY" = 'groups'