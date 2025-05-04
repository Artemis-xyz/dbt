{{ config(materialized="incremental", snowflake_warehouse="TRON", unique_key=["transaction_hash", "event_index"]) }}
with
prices as ({{get_multiple_coingecko_price_with_latest('tron')}})
, token_transfers as (
    select
        datetime as block_timestamp
        , block_number
        , transaction_hash
        , transaction_index
        , log_index as event_index
        , trx_contract_address as contract_address
        , trx_from_address as from_address
        , trx_to_address as to_address
        , source_value as amount_raw
        , value as amount_native
    from SONARX_TRON.TRON_SHARE.TOKEN_TRANSFERS
    where type = 'TRC20' and transaction_info_result = 'SUCCESS'
    {% if is_incremental() %}
            and datetime >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
        {% endif %}
)
select
    block_timestamp
    , block_number
    , transaction_hash
    , transaction_index
    , event_index
    , token_transfers.contract_address
    , from_address
    , to_address
    , amount_raw
    , amount_native
    , amount_native * prices.price as amount
    , prices.price
from token_transfers
left join prices
    on token_transfers.block_timestamp::date = prices.date
    and lower(token_transfers.contract_address) = lower(prices.contract_address)
where amount_raw > 0