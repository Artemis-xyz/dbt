{% macro get_fundamental_data_for_chain_by_contract(chain, model_version='') %}
{% set model_name = "fact_" ~ chain ~ "_transactions" ~ ("_v2" if model_version == "v2" else "") %}
    with
        real_users as (
            select
                contract_address,
                from_address
            from {{ ref(model_name) }}
                where raw_date < to_date(sysdate())
                group by contract_address, from_address
                having count(*) >= 2 and sum(gas_usd) > 0.0001
            ),
        contract_data as (
            select
                m.contract_address,
                raw_date as date,
                max(name) name,
                max(chain) as chain,
                max(app) as app,
                max(friendly_name) as friendly_name,
                sum(tx_fee) gas,
                sum(gas_usd) gas_usd,
                count(*) txns,
                count(distinct m.from_address) dau,
                count(distinct ru.from_address) real_users,
                max(category) category
            from {{ ref(model_name) }} m
            left join real_users ru
                on m.from_address = ru.from_address
                and m.contract_address = ru.contract_address
            where
                not equal_null(category, 'EOA')
                {% if is_incremental() %}
                    and date >= (select dateadd('day', -3, max(date)) from {{ this }})
                {% endif %}
                and raw_date < to_date(sysdate())
            group by date, m.contract_address
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
        dau,
        real_users
    from contract_data
{% endmacro %}
