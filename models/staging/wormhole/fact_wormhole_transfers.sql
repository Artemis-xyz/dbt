{{ config(snowflake_warehouse="BRIDGE_MD", materialized="table") }}

with
    flattened_json as (
        select
            value:"id"::string as id,
            value:"timestamp"::timestamp as timestamp,
            value:"standardizedProperties":"amount"::string as amount,
            value:"standardizedProperties":"appIds" as app_ids,
            value:"standardizedProperties":"fee"::string as fee,
            value:"standardizedProperties":"feeAddress"::string as fee_address,
            value:"standardizedProperties":"feeChain"::string as fee_chain,
            value:"standardizedProperties":"fromAddress"::string as from_address,
            value:"standardizedProperties":"fromChain"::integer as from_chain,
            value:"standardizedProperties":"toAddress"::string as to_address,
            value:"standardizedProperties":"toChain"::integer as to_chain,
            value:"standardizedProperties":"tokenAddress"::string as token_address,
            value:"standardizedProperties":"tokenChain"::integer as token_chain,
            value:"usdAmount"::float as amount_usd,
            value:"symbol"::string as symbol,
            extraction_date
        from
            {{ source("PROD_LANDING", "raw_wormhole_transactions") }},
            lateral flatten(input => parse_json(source_json)) as flat_json
        where value:"standardizedProperties" is not null
    ),

    recent_data as (
        select id, max(extraction_date) as extraction_date
        from flattened_json
        group by 1
    )

select
    id,
    timestamp,
    amount,
    app_ids,
    fee,
    fee_address,
    fee_chain,
    from_address,
    from_chain,
    to_address,
    to_chain,
    token_address,
    token_chain,
    amount_usd,
    symbol
from flattened_json t1
left join recent_data t2 using (id)
where t1.extraction_date = t2.extraction_date and token_address != ''
