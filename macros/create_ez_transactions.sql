{% macro create_ez_transactions(chain, model_version='') %}

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
        {% if chain in ("near") %}
            tx_succeeded,
        {% endif %}
        {% if (chain not in ("tron", "starknet", "mantle")) %} user_type,
        {% else %} null as user_type,
        {% endif %}
        {% if (chain not in ("near", "starknet", "mantle")) %}
            balance_usd, native_token_balance, stablecoin_balance,
        {% else %}
            null as balance_usd,
            null as native_token_balance,
            null as stablecoin_balance,
        {% endif %}
        {% if chain in (
            "arbitrum",
            "avalanche",
            "base",
            "bsc",
            "ethereum",
            "optimism",
            "polygon",
        ) %}probability, engagement_type
        {% else %}null as probability, null as engagement_type
        {% endif %}
        {% if model_version == 'v2' and chain in (
            "arbitrum",
            "avalanche",
            "base",
            "bsc",
            "ethereum",
            "optimism",
            "polygon",
            "near",
            "tron",
            "mantle"
        ) %}
            ,last_updated_timestamp
        {% endif %}
    from pc_dbt_db.prod.fact_{{ chain }}_transactions{% if model_version == 'v2' %}_v2{% endif %}
    where
        raw_date < to_date(sysdate())
        {% if is_incremental() %}
            {% if model_version == 'v2' and chain in (
            "arbitrum",
            "avalanche",
            "base",
            "bsc",
            "ethereum",
            "optimism",
            "polygon",
            "near",
            "tron",
            "mantle"
        ) %}
                and last_updated_timestamp > (select max(last_updated_timestamp) from {{ this }})
            {% else %}
                and (block_timestamp
                >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
                or app is not null)
            {% endif %}
        {% endif %}

{% endmacro %}
