{{
    config(
        materialized="incremental",
        unique_key=["date", "contract_address", "address"],
        snowflake_warehouse="SOLANA_2XLG"
    )
}}

{% set token_addresses = var('token_addresses_list', []) %}


-- NOTE: owner_addresses here are either program_ids (via PDAs), or EOAs

WITH 
{% if token_addresses | length > 0 %}
    all_addresses AS (
        {{ get_all_addresses_under_owners(token_addresses) }}
    ),
{% endif %}
cleaned_up_token_owner_hierarchy AS (
    {{ get_valid_solana_token_account_owners() }}
),
forward_filled_balances AS (
    SELECT *
    FROM {{ ref("fact_solana_address_balances_by_token_forward_filled") }} a
    {% if token_addresses | length > 0 %}
        INNER JOIN all_addresses
            ON a.address = all_addresses.address
    {% endif %}
    WHERE 1=1
    {% if is_incremental() %}
        AND a.block_timestamp >= dateadd(day, -3, to_date(sysdate()))
    {% endif %}
),
l0 AS (
    SELECT
        a.address AS base_address,
        a.address AS parent_address,
        0 AS level
    FROM forward_filled_balances a
),
{% set max_levels = 3 %}
{% for level in range(1, max_levels + 1) %}
    l{{ level }} AS (
        SELECT
            l{{ level - 1 }}.base_address,
            p.owner AS parent_address,
            l{{ level - 1 }}.level + 1 AS level
        FROM l{{ level - 1 }}
        INNER JOIN cleaned_up_token_owner_hierarchy p
            ON l{{ level - 1 }}.parent_address = p.account_address
    ),
{% endfor %}
final_parents AS (
    SELECT
        base_address,
        parent_address
    FROM (
        SELECT
            base_address,
            parent_address,
            ROW_NUMBER() OVER (PARTITION BY base_address ORDER BY level DESC) AS rn
        FROM (
            SELECT * FROM l0
            {% for level in range(1, max_levels + 1) %}
            UNION ALL
            SELECT * FROM l{{ level }}
            {% endfor %}
        ) all_levels
    ) sub
    WHERE rn = 1
),
balances_with_owner_address as (
    select 
        forward_filled_balances.*,
        COALESCE(fp.parent_address, forward_filled_balances.address) AS owner_address -- owner address is either program_id or EOA
    from forward_filled_balances
    left join final_parents fp on forward_filled_balances.address = fp.base_address
),
app_contracts as (
    select distinct
        address,
        contract.name,
        contract.chain,
        contract.artemis_category_id as category,
        contract.artemis_sub_category_id as sub_category,
        contract.artemis_application_id as app,
        contract.friendly_name
    from {{ ref("dim_all_addresses_labeled_gold") }} as contract
    where chain = 'solana'
)
select
    bwa.*,
    coalesce(owner_app_contracts.name, app_contracts.name) as name,
    coalesce(owner_app_contracts.category, app_contracts.category) as category,
    coalesce(owner_app_contracts.sub_category, app_contracts.sub_category) as sub_category,
    coalesce(owner_app_contracts.app, app_contracts.app) as app,
    coalesce(owner_app_contracts.friendly_name, app_contracts.friendly_name) as friendly_name
from balances_with_owner_address bwa
left join app_contracts as owner_app_contracts on lower(bwa.owner_address) = lower(owner_app_contracts.address)
left join app_contracts on lower(bwa.address) = lower(app_contracts.address)
