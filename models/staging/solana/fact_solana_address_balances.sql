{{
    config(
        materialized="incremental",
        unique_key=["date", "contract_address", "address"],
        snowflake_warehouse="SOLANA_2XLG"
    )
}}

{% set token_addresses = var('token_addresses_list', ['KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD']) %}


-- NOTE: owner_addresses here are either program_ids (via PDAs), or EOAs
-- We can also add an optional type column here to define program_id or EOA


WITH 
{% if token_addresses | length > 0 %}
    all_addresses AS (
        {{ get_all_addresses_under_program_id(token_addresses) }}
    ),
{% endif %}
cleaned_up_token_owner_hierarchy AS (
    {{ get_valid_solana_token_account_owners() }}
),
base_case AS (
    SELECT
        a.address AS base_address,
        a.address AS parent_address,
        0 AS level
    FROM {{ ref("fact_solana_address_balances_by_token_forward_filled") }} a
    {% if token_addresses | length > 0 %}
        INNER JOIN all_addresses
            ON a.address = all_addresses.address
    {% endif %}
    WHERE 1=1
    {% if is_incremental() %}
        AND a.block_timestamp >= dateadd(day, -1, to_date(sysdate()))
    {% endif %}
),
l1 AS (
    SELECT
        a.base_address,
        p.owner AS parent_address,
        a.level + 1 AS level
    FROM base_case a
    INNER JOIN cleaned_up_token_owner_hierarchy p
        ON a.parent_address = p.account_address
),
l2 AS (
    SELECT
        l1.base_address,
        p.owner AS parent_address,
        l1.level + 1 AS level
    FROM l1
    INNER JOIN cleaned_up_token_owner_hierarchy p
        ON l1.parent_address = p.account_address
),
l3 AS (
    SELECT
        l2.base_address,
        p.owner AS parent_address,
        l2.level + 1 AS level
    FROM l2
    INNER JOIN cleaned_up_token_owner_hierarchy p
        ON l2.parent_address = p.account_address
)



-- left join and order by level desc I think should work?


-- Recursive CTE with recursion capped at 10 levels
, RECURSIVE address_hierarchy (
    original_address,
    current_address,
    parent_address,
    level
) AS (

    -- Base case
    SELECT
        a.address AS original_address,
        a.address AS current_address,
        NULL AS parent_address,
        0 AS level
    FROM {{ ref("fact_solana_address_balances_by_token_forward_filled") }} a
    {% if token_addresses | length > 0 %}
        INNER JOIN all_addresses
            ON a.address = all_addresses.address
    {% endif %}
    WHERE 1=1
    {% if is_incremental() %}
        AND block_timestamp >= dateadd(day, -1, to_date(sysdate()))
    {% endif %}

    UNION ALL

    -- Recursive case: get parent of current address
    SELECT
        ah.original_address,
        b.parentAddress AS current_address,
        b.parentAddress AS parent_address,
        ah.level + 1 AS level
    FROM address_hierarchy ah
    JOIN cleaned_up_token_owner_hierarchy b
        ON ah.current_address = b.childAddress
    WHERE ah.level < 10
)

-- Extract the topmost reachable parent
, final_parents AS (
    SELECT
        original_address,
        current_address AS ultimate_parent_address
    FROM (
        SELECT
            original_address,
            current_address,
            ROW_NUMBER() OVER (PARTITION BY original_address ORDER BY level DESC) AS rn
        FROM address_hierarchy
    ) sub
    WHERE rn = 1
)

SELECT
    a.*,
    COALESCE(fp.ultimate_parent_address, a.address) AS owner_address -- owner address is either program_id or EOA
FROM {{ ref("fact_solana_address_balances_by_token_forward_filled") }} a
LEFT JOIN final_parents fp ON a.address = fp.original_address
