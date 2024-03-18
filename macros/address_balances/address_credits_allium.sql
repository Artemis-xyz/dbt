{% macro address_credits_allium(chain) %}

    select
        to_address as address,
        'native_token' as contract_address,
        block_timestamp,
        cast(amount as float) as credit,
        cast(usd_amount as float) as credit_usd,
        transaction_hash as tx_hash,
        unique_id
    from {{ chain }}_allium.assets.trx_token_transfers
    where
        {% if chain == "tron" %}
            -- Null Address on Tron
            lower(to_address) <> lower('T9yD14Nj9j7xAB4dbGeiX9h8unkKHxuWwb')
        {% endif %}
        and to_date(block_timestamp) < to_date(sysdate())
        and to_address <> from_address
        {% if is_incremental() %}
            and block_timestamp
            >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
        {% endif %}
    union all
    select
        to_address as address,
        token_address as contract_address,
        block_timestamp,
        cast(raw_amount as float) as credit,
        null as credit_usd,
        transaction_hash as tx_hash,
        unique_id
    from {{ chain }}_allium.assets.trc20_token_transfers
    where
        {% if chain == "tron" %}
            -- Null Address on Tron
            lower(to_address) <> lower('T9yD14Nj9j7xAB4dbGeiX9h8unkKHxuWwb')
        {% endif %}
        {% if is_incremental() %}
            and block_timestamp
            >= (select dateadd('day', -7, max(block_timestamp)) from {{ this }})
        {% endif %}
        and to_date(block_timestamp) < to_date(sysdate())
        and to_address <> from_address

{% endmacro %}
