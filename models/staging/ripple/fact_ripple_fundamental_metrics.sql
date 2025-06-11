{{
    config(
        materialized="incremental",
        snowflake_warehouse="RIPPLE",
        unique_key="date"
    )
}}

SELECT
    datetime::date as date,
    sum(usd_fee) as chain_fees,
    sum(fee) as chain_fees_native,
    count(distinct account) as chain_dau, -- Signing pubkeys are automatically generated at transaction signing time, the "account" field uniquely identifies the sender https://xrpl.org/docs/references/protocol/transactions/common-fields
    count(distinct transaction_hash) as chain_txns
FROM {{ source("SONARX_XRP", "transactions") }}
WHERE success
{% if is_incremental() %}
    AND datetime >= (select dateadd(day, -3, max(date)) from {{ this }})
{% endif %}
GROUP BY 1