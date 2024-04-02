{% macro unpack_blocks_json_quicknode_streams(
    chain,
    block_column_map=[],
    receipts_column_name="receipts",
    block_number_column_name="block_number"
) %}
    with
        block_data as (
            select
                block_number,
                network,
                {% for json_name, conversion_macro, column_name in block_column_map %}
                    {{ conversion_macro('data:"block".' ~ json_name) }}
                    as {{ column_name }},
                {% endfor %}
                data:"{{ receipts_column_name }}" as receipts,  -- Will need this for events dataset
                data:"block" as raw_block_data,
            from {{ source("PROD_LANDING", "raw_" ~ chain ~ "_blocks_receipts") }},
            {% if is_incremental() %}
                where block_number >= (select max(block_number - 10) from {{ this }})
            {% endif %}
        ),

        {% set block_columns = namespace(items=[]) %}
        {% for json_name, type_conversion, column_name in block_column_map %}
            {% do block_columns.items.append(column_name) %}
        {% endfor %}
        raw_blocks as (
            select
                block_number,
                network,
                {% for column_name in block_columns.items %} {{ column_name }}, {% endfor %}
                receipts,
            from block_data
            {% if is_incremental() %}
                where block_number >= (select max(block_number - 10) from {{ this }})
            {% endif %}
            order by {{ block_number_column_name }}
        )
    select *
    from raw_blocks
    qualify row_number() over (partition by block_hash order by block_timestamp desc) = 1
{% endmacro %}
