{% macro address_debits_allium(chain) %}

    select
        from_address as address,
        'native_token' as contract_address,
        block_timestamp,
        cast(amount * -1 as float) as debit,
        cast(usd_amount * -1 as float) as debit_usd,
        transaction_hash as tx_hash,
        unique_id
    from {{ chain }}_allium.assets.trx_token_transfers
    where
        {% if chain == "tron" %}
            -- Null Address on Tron
            lower(from_address) <> lower('T9yD14Nj9j7xAB4dbGeiX9h8unkKHxuWwb')
        {% endif %}
        and to_date(block_timestamp) < to_date(sysdate())
        {% if is_incremental() %}
            and block_timestamp
            >= (select dateadd('day', -7, max(block_timestamp)) from {{ this }})
        {% endif %}
        and to_address <> from_address
    union all
    select
        from_address as address,
        token_address as contract_address,
        block_timestamp,
        cast(raw_amount * -1 as float) as debit,
        null as debit_usd,
        transaction_hash as tx_hash,
        unique_id
    from {{ chain }}_allium.assets.trc20_token_transfers
    where
        {% if chain == "tron" %}
            -- Null Address on Tron
            lower(from_address) <> lower('T9yD14Nj9j7xAB4dbGeiX9h8unkKHxuWwb')
        {% endif %}
        and to_date(block_timestamp) < to_date(sysdate())
        {% if is_incremental() %}
            and block_timestamp
            >= (select dateadd('day', -7, max(block_timestamp)) from {{ this }})
        {% endif %}
        and to_address <> from_address

{% endmacro %}
