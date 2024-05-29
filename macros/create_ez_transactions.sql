{% macro create_ez_transactions(chain) %}

    select
        tx_hash,
        contract_address,
        block_timestamp,
        raw_date,
        from_address,
        tx_fee,
        {% if chain in ("starknet") %}
            currency,
        {% endif %}
        gas_usd,
        chain,
        name,
        app,
        friendly_name,
        sub_category,
        category,
        {% if (chain not in ("tron", "starknet")) %} user_type
        {% else %} null as user_type
        {% endif %}
        {% if (chain not in ("near", "starknet")) %}
            , balance_usd, native_token_balance, stablecoin_balance
        {% else %}
            ,
            null as balance_usd,
            null as native_token_balance,
            null as stablecoin_balance
        {% endif %}
        {% if chain in (
            "arbitrum",
            "avalanche",
            "base",
            "bsc",
            "ethereum",
            "optimism",
            "polygon",
        ) %}, probability, engagement_type
        {% else %}, null as probability, null as engagement_type
        {% endif %}
    from pc_dbt_db.prod.fact_{{ chain }}_transactions
    where
        raw_date < to_date(sysdate())
        {% if is_incremental() %}
            and block_timestamp
            >= (select dateadd('day', -5, max(block_timestamp)) from {{ this }})
        {% endif %}

{% endmacro %}
