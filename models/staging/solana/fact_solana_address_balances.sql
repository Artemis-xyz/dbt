{{
    config(
        materialized="incremental",
        unique_key=["date", "contract_address", "address"],
        snowflake_warehouse="SOLANA_XLG"
    )
}}

{% set token_addresses = var('token_addresses_list', []) %}


-- NOTE: owner_addresses here are either program_ids (via PDAs), or EOAs
-- We can also add an optional type column here to define program_id or EOA


WITH cleaned_up_token_owner_hierarchy AS (
    SELECT * FROM solana_flipside.core.fact_token_account_owners
    WHERE account_owner NOT IN (
        '11111111111111111111111111111111',
        'Vote111111111111111111111111111111111111111',
        'Stake11111111111111111111111111111111111111',
        'Config1111111111111111111111111111111111111',
        'ComputeBudget111111111111111111111111111111',
        'AddressLookupTab1e1111111111111111111111111',
        'ZkE1Gama1Proof11111111111111111111111111111',
        'KeccakSecp256k11111111111111111111111111111',
        'Ed25519SigVerify111111111111111111111111111',
        'Feature111111111111111111111111111111111111',

        -- loaders
        'NativeLoader1111111111111111111111111111111',
        'BPFLoader1111111111111111111111111111111111',
        'BPFLoader2111111111111111111111111111111111',
        'BPFLoaderUpgradeab1e11111111111111111111111',
        'LoaderV411111111111111111111111111111111111',

        -- sys vars
        'SysvarRent111111111111111111111111111111111',
        'SysvarC1ock11111111111111111111111111111111',
        'SysvarStakeHistory1111111111111111111111111',
        'Sysvar1nstructions1111111111111111111111111',
        'SysvarRecentB1ockHashes11111111111111111111',
        'Sysvar1111111111111111111111111111111111111'
    )
)
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
    WHERE 1=1
    {% if is_incremental() %}
        AND block_timestamp >= dateadd(day, -1, to_date(sysdate()))
    {% endif %}
    {% if token_addresses | length > 0 %}
        AND a.address IN (
            {{ "'" ~ token_addresses | join("','") ~ "'" }}
        )
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
