{{ config(snowflake_warehouse="DEBRIDGE", materialized="table") }}

select 
    TRIM(order_id::string, '"') as order_id
    , to_timestamp(TRIM(fulfillment_time::string, '"')) as fulfillment_time
    , TRIM(sender_address_1, '"') as maker
    , TRIM(source_token_address, '"') as origin_chain_id
    , TRIM(origin_chain_id::string, '"') as source_token_address
    , source_amount_native
    , TRIM(receipient_address_1::string, '"') as receipient_address
    , dst_chain_id
    , TRIM(dst_token_address::string, '"') as dst_token_address
    , dst_amount_native
    , TRIM(sender_address_2::string, '"') as authority_src_chain
    , TRIM(receipient_address_2::string, '"') as authority_destination_chain
from {{ source('PROD_LANDING', 'dln_orders_backfil_csv') }}
