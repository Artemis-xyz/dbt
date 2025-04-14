{% macro get_fundamental_data_for_chain_by_contract(chain, model_version='') %}
{% set model_name = "fact_" ~ chain ~ "_transactions" ~ ("_v2" if model_version == "v2" else "") %}
    with
        contract_data as (
            select
                contract_address,
                raw_date as date,
                max(name) name,
                max(chain) as chain,
                max(app) as app,
                max(friendly_name) as friendly_name,
                sum(tx_fee) gas,
                sum(gas_usd) gas_usd,
                count(*) txns,
                count(distinct from_address) dau,
                max(category) category
            from {{ ref(model_name) }}
            where
                not equal_null(category, 'EOA')
                {% if is_incremental() %}
                    and date >= (select dateadd('day', -7, max(date)) from {{ this }})
                {% endif %}
            group by date, contract_address
        )
    select
        contract_data.date,
        contract_data.contract_address,
        chain,
        name,
        app,
        friendly_name,
        category,
        gas,
        gas_usd,
        txns,
        dau
    from contract_data
{% endmacro %}
