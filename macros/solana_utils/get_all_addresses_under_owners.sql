{% macro get_all_addresses_under_owners(program_ids, max_levels=5) %}

WITH cleaned_up_token_owner_hierarchy AS (
    {{ get_valid_solana_token_account_owners() }}
),

-- Generate the base level (l0)
l0 as (
    select 
        owner,
        account_address,
        0 AS level
    FROM cleaned_up_token_owner_hierarchy
    WHERE owner IN (
        {{ "'" ~ program_ids | join("','") ~ "'" }}
    )  
),

{% for level in range(1, max_levels + 1) %}
    -- Level {{ level }}
    l{{ level }} as (
        SELECT
            l{{ level - 1 }}.account_address as owner,
            h.account_address AS account_address,
            l{{ level - 1 }}.level + 1 AS level
        FROM l{{ level - 1 }}
        INNER JOIN cleaned_up_token_owner_hierarchy h
            ON h.owner = l{{ level - 1 }}.account_address 
    ),
{% endfor %}

-- Combine all addresses from all levels
combined_addresses AS (
    {% for level in range(0, max_levels + 1) %}
    -- Get both owner and account_address from level {{ level }}
    SELECT DISTINCT owner as address FROM l{{ level }}
    UNION
    SELECT DISTINCT account_address AS address FROM l{{ level }}
    {% if not loop.last %}UNION{% endif %}
    {% endfor %}
)

SELECT * FROM combined_addresses

{% endmacro %}
