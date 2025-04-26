{{ config(snowflake_warehouse="DEBRIDGE", materialized="table") }}

with debridge_order_metadata_backfil as (
    select 
        TRIM(order_id::string, '"') as order_id
        , TRIM(sender_address_1, '"') as maker_src
        , TRIM(sender_address_2::string, '"') as sender
        , TRIM(receipient_address_1::string, '"') as receiver
        , TRIM(source_token_address, '"') as src_chain_id
        , TRIM(origin_chain_id::string, '"') as src_token_address
        , source_amount_native as src_amount_native
        , dst_chain_id
        , to_timestamp(TRIM(fulfillment_time::string, '"')) as dst_block_timestamp
        , TRIM(dst_token_address::string, '"') as dst_token_address
        , dst_amount_native
    from {{ source('PROD_LANDING', 'dln_orders_backfil_csv') }}
),
flatten_debridge_order_metadata as (
    select 
        value:"orderId":"stringValue"::string as order_id
        , value:"makerSrc":"stringValue"::string as maker_src
        , value:"orderAuthorityAddressDst":"stringValue"::string as sender
        , value:"receiverDst":"stringValue"::string as receiver
        , value:"giveOfferWithMetadata":"chainId":"bigIntegerValue"::int as src_chain_id
        , value:"giveOfferWithMetadata":"tokenAddress":"stringValue"::string as src_token_address
        , value:"giveOfferWithMetadata":"amount":"stringValue"::string as src_amount_native
        , value:"takeOfferWithMetadata":"chainId":"bigIntegerValue"::int as dst_chain_id
        , to_timestamp_ntz(value:"fulfilledDstEventMetadata":"blockTimeStamp"::string) as dst_block_timestamp
        , value:"takeOfferWithMetadata":"tokenAddress":"stringValue"::string as dst_token_address
        , value:"takeOfferWithMetadata":"amount":"stringValue"::string as dst_amount_native
        , extraction_date
    from {{ source('PROD_LANDING', 'raw_debridge_order_metadata') }},
    lateral flatten(input => PARSE_JSON(source_json))
),
debridge_order_metadata as (
    select
        order_id
        , max_by(maker_src, extraction_date) as maker_src
        , max_by(sender, extraction_date) as sender
        , max_by(receiver, extraction_date) as receiver
        , max_by(src_chain_id, extraction_date) as src_chain_id
        , max_by(src_token_address, extraction_date) as src_token_address
        , max_by(src_amount_native, extraction_date) as src_amount_native
        , max_by(dst_chain_id, extraction_date) as dst_chain_id
        , max_by(dst_block_timestamp, extraction_date) as dst_block_timestamp
        , max_by(dst_token_address, extraction_date) as dst_token_address
        , max_by(dst_amount_native, extraction_date) as dst_amount_native
        , max(extraction_date) as extraction_date
    from flatten_debridge_order_metadata
    where order_id is not null
    group by order_id 
),
debridge_order_metadata_full as (
    select 
        order_id
        , maker_src
        , sender
        , receiver
        , src_chain_id
        , src_token_address
        , src_amount_native
        , dst_chain_id
        , dst_block_timestamp
        , dst_token_address
        , dst_amount_native
        , extraction_date
    from debridge_order_metadata
    union all
    select 
        order_id
        , maker_src
        , sender
        , receiver
        , src_chain_id
        , src_token_address
        , src_amount_native
        , dst_chain_id
        , dst_block_timestamp
        , dst_token_address
        , dst_amount_native
        , '1999-01-01 00:00:00'::timestamp as extraction_date
    from debridge_order_metadata_backfil
)
select 
    order_id
    , max_by(maker_src, extraction_date) as maker_src
    , max_by(sender, extraction_date) as sender
    , max_by(receiver, extraction_date) as receiver
    , max_by(src_chain_id, extraction_date) as src_chain_id
    , max_by(src_token_address, extraction_date) as src_token_address
    , max_by(src_amount_native, extraction_date) as src_amount_native
    , max_by(dst_chain_id, extraction_date) as dst_chain_id
    , max_by(dst_block_timestamp, extraction_date) as dst_block_timestamp
    , max_by(dst_token_address, extraction_date) as dst_token_address
    , max_by(dst_amount_native, extraction_date) as dst_amount_native
from debridge_order_metadata_full
group by order_id
