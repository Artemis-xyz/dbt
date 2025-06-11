{{ config(snowflake_warehouse="WORMHOLE", materialized="table") }}

with
decoded_wormhole_data as (
    select 
        value:"id"::string as id,
        value:"sourceChain":"timestamp"::timestamp as src_timestamp,
        value:"sourceChain":"transaction":"txHash"::string as src_tx_hash,
        value:"sourceChain":"from"::string as src_from_address,
        value:"sourceChain":"to"::string as src_to_address,
        value:"targetChain":"timestamp"::timestamp as dst_timestamp,
        value:"targetChain":"transaction":"txHash"::string as dst_tx_hash,
        value:"targetChain":"from"::string as dst_from_address,
        value:"targetChain":"to"::string as dst_to_address,
        TRY_TO_DOUBLE(value:"content":"standarizedProperties":"amount"::string) as amount,
        TRY_TO_DOUBLE(value:"data":"tokenAmount"::string) as amount_adjusted,
        TRY_TO_DOUBLE(value:"data":"usdAmount"::string) as amount_usd,
        value:"content":"standarizedProperties":"appIds" as app_ids,
        TRY_TO_DOUBLE(value:"content":"standarizedProperties":"fee"::string) as fee,
        value:"content":"standarizedProperties":"feeAddress"::string as fee_address,
        value:"content":"standarizedProperties":"feeChain"::string as fee_chain,
        case 
            when LENGTH(TRIM(value:"content":"standarizedProperties":"fromAddress"::string)) > 0 then value:"content":"standarizedProperties":"fromAddress"::string 
            else value:"sourceChain":"from"::string 
        end as from_address,
        coalesce(value:"content":"standarizedProperties":"fromChain"::integer, value:"sourceChain":"chainId"::integer) as from_chain,
        case 
            when LENGTH(TRIM(value:"content":"standarizedProperties":"toAddress"::string)) > 0 then value:"content":"standarizedProperties":"toAddress"::string 
            else value:"sourceChain":"to"::string 
        end as to_address,
        coalesce(value:"content":"standarizedProperties":"toChain"::integer, value:"targetChain":"chainId"::integer) as to_chain,
        value:"content":"standarizedProperties":"tokenAddress"::string as token_address,
        value:"content":"standarizedProperties":"tokenChain"::integer as token_chain,
        value:"data":"symbol"::string as symbol,
        value:"content":"standarizedProperties":"normalizedDecimals"::integer as normalized_decimals,
        value:"sourceChain":"status"::string as src_status,
        value:"targetChain":"status"::string as dst_status,
        value as payload,
        extraction_date
    from {{ source("PROD_LANDING", "raw_wormhole_operations") }} t1,
    lateral flatten(input => parse_json(source_json)) as flat_json
    where from_chain is not null and to_chain is not null and src_status = 'confirmed' and dst_status = 'completed'
)
select
    id,
    max_by(src_timestamp, extraction_date) as src_timestamp,
    max_by(src_tx_hash, extraction_date) as src_tx_hash,
    max_by(src_from_address, extraction_date) as src_from_address,
    max_by(src_to_address, extraction_date) as src_to_address,
    max_by(dst_timestamp, extraction_date) as dst_timestamp,
    max_by(dst_tx_hash, extraction_date) as dst_tx_hash,
    max_by(dst_from_address, extraction_date) as dst_from_address,
    max_by(dst_to_address, extraction_date) as dst_to_address,
    max_by(amount, extraction_date) as amount,
    max_by(amount_adjusted, extraction_date) as amount_adjusted,
    max_by(amount_usd, extraction_date) as amount_usd,
    max_by(app_ids, extraction_date) as app_ids,
    max_by(fee, extraction_date) as fee,
    max_by(fee_address, extraction_date) as fee_address,
    max_by(fee_chain, extraction_date) as fee_chain,
    max_by(from_address, extraction_date) as from_address,
    max_by(from_chain, extraction_date) as from_chain,
    max_by(to_address, extraction_date) as to_address,
    max_by(to_chain, extraction_date) as to_chain,
    max_by(token_address, extraction_date) as token_address,
    max_by(token_chain, extraction_date) as token_chain,
    max_by(symbol, extraction_date) as symbol,
    max_by(normalized_decimals, extraction_date) as normalized_decimals,
    max_by(src_status, extraction_date) as src_status,
    max_by(dst_status, extraction_date) as dst_status,
    max_by(payload, extraction_date) as payload,
    max(extraction_date) as extraction_date
from decoded_wormhole_data t1
GROUP BY id
