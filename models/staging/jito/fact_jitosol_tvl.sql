{{
    config(
        materialized="table",
        snowflake_warehouse="JITO"
    )
}}

{% set stake_accounts_query %}
  select distinct stake_account from {{ ref('fact_jitosol_stake_accounts') }}
{% endset %}

{% set stake_accounts_results = run_query(stake_accounts_query) %}

{% if execute %}
    {% set stake_accounts_list = stake_accounts_results.columns[0].values() %}
{% else %}
    {% set stake_accounts_list = [] %}
{% endif %}

{{ get_treasury_balance(chain='solana', addresses = stake_accounts_list, earliest_date='2022-10-24') }}