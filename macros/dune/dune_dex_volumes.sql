{% macro dune_dex_volumes(chain) %}
    SELECT 
        block_date::date AS date,
        SUM(COALESCE(amount_usd, 0)) AS daily_volume
    FROM {{ source("DUNE_DEX_VOLUMES", "trades") }}
    WHERE blockchain = '{{ chain }}'
    GROUP BY 1
{% endmacro %}
