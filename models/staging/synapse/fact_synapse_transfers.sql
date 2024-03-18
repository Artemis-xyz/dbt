{{ config(materialized="table") }}

with
    max_extraction as (
        select source_json:"date" as date, max(extraction_date) as extraction_date
        from {{ source("PROD_LANDING", "raw_synapse_transfers") }}
        group by 1
    ),

    data as (
        select *
        from {{ source("PROD_LANDING", "raw_synapse_transfers") }} t
        left join
            max_extraction m
            on m.date = t.source_json:"date"
            and m.extraction_date = t.extraction_date
    ),

    transfers as (
        select
            value:"fromInfo":"address"::string as depositor,
            value:"toInfo":"address"::string as recipient,
            to_timestamp(value:"fromInfo":"time"::integer) as origin_block_timestamp,
            value:"fromInfo":"chainID"::integer as origin_chain_id,
            value:"fromInfo":"tokenAddress"::string as origin_token_address,
            value:"fromInfo":"tokenSymbol"::string as origin_token_symbol,
            value:"fromInfo":"formattedValue"::float as origin_token_amount,
            value:"fromInfo":"hash"::string as origin_tx_hash,
            to_timestamp(value:"toInfo":"time"::integer) as destination_block_timestamp,
            value:"toInfo":"chainID"::integer as destination_chain_id,
            value:"toInfo":"tokenAddress"::string as destination_token_address,
            value:"toInfo":"tokenSymbol"::string as destination_token_symbol,
            value:"toInfo":"formattedValue"::float as destination_token_amount,
            value:"toInfo":"hash"::string as destination_tx_hash,
            as_boolean(value:"swapSuccess") as is_swap,
            value:"kappa"::string as synapse_tx_hash
        from data, lateral flatten(input => parse_json(source_json):data) as flat_json
    )

select
    depositor,
    recipient,
    origin_block_timestamp,
    origin_chain_id,
    origin_token_address,
    origin_token_symbol,
    origin_token_amount,
    origin_tx_hash,
    destination_block_timestamp,
    destination_chain_id,
    destination_token_address,
    destination_token_symbol,
    destination_token_amount,
    destination_tx_hash,
    is_swap,
    synapse_tx_hash
from
    (
        select
            row_number() over (
                partition by synapse_tx_hash
                order by origin_block_timestamp, destination_block_timestamp
            ) as synapse_tx_hash_id,
            *
        from transfers
    )
where synapse_tx_hash_id = 1
