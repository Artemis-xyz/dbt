{{
    config(
        materialized="view",
        unique_key=["tx_hash", "event_index"],
        warehouse="GMX",
        database="gmx",
        schema="raw",
        alias="fact_perps_token_prices"
    )
}}

SELECT
    block_timestamp,
    tx_hash,
    t1.chain,
    'gmx' as app,
    tracked_metadata.symbol,
    price,
    token_address
FROM {{ ref('fact_gmx_all_versions_trades') }} t1
inner join  {{ref('wrapped_token_majors_by_chain')}} tracked_metadata
    on lower(t1.token_address) = lower(tracked_metadata.contract_address) 
    and t1.chain = tracked_metadata.chain
