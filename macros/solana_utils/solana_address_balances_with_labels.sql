{% macro solana_address_balances_with_labels(start_date, end_date, max_date, min_date) %}

-- This model covers the following possibilities:
-- 1. Full refresh with start and end date (for historical backfilling)
-- 2. Incremental refresh with start and end date (for historical backfilling)
-- 3. Incremental refresh for current day (for incremental runs)

-- What will be caught and will fail
-- 1. Full refresh without start and end date
   -- This is because a full refresh will be too massive to handle, so it should fail

-- NOTE: owner_addresses here are either program_ids (via PDAs), or EOAs

WITH 
cleaned_up_token_owner_hierarchy AS (
    {{ get_valid_solana_token_account_owners() }}
),
forward_filled_balances AS (
    SELECT *
    FROM {{ ref("fact_solana_address_balances_forward_filled_2024_h1") }} a
    WHERE 1=1
    {% if start_date and end_date %}
        AND date >= to_date('{{ start_date }}')
        AND date <= to_date('{{ end_date }}')
    {% elif is_incremental() %}
        AND date >= dateadd(day, -3, to_date(sysdate()))
    {% else %}
        -- this is expected to break. We don't want to full refresh without start + end date range
        WHERE 1=1
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

{% endmacro %}