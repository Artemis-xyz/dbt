{{
    config(
        materialized="table",
        snowflake_warehouse="LITECOIN",
        database="litecoin",
        schema="core",
        alias="ez_transfers",
    )
}}

select 
    hash,
    size,
    virtual_size,
    version,
    lock_time,
    block_hash,
    block_number,
    block_timestamp,
    block_timestamp_month,
    input_count,
    output_count,
    input_value / 100000000 as input_value_ltc,
    output_value / 100000000 as output_value_ltc,
    is_coinbase,
    fee / 100000000 as fee_ltc,
    inputs,
    outputs,
    unique_id
from {{ ref('fact_litecoin_transfers') }}
where block_timestamp < to_date(sysdate()) 