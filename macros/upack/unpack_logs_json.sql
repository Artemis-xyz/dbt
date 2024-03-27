{% macro unpack_logs_json(
    chain,
    log_column_map=[],
    parent_column_map=[
        ("block_timestamp", "block_timestamp"),
        ("block_number", "block_number"),
        ("status", "status"),
        ("from_address", "origin_from_address"),
        ("to_address", "origin_to_address"),
    ]
) %}
    with
        {% set parent_columns = namespace(items=[]) %}
        {% for column_name, output_column_name in parent_column_map %}
            {% do parent_columns.items.append(column_name) %}
        {% endfor %}

        log_data as (
            {{
                unpack_json_array(
                    "fact_" ~ chain ~ "_transactions",
                    "logs",
                    column_map=log_column_map,
                    parent_columns=parent_columns.items,
                )
            }}
        )

        {% set log_columns = namespace(items=[]) %}
        {% for json_name, type_conversion, column_name in log_column_map %}
            {% do log_columns.items.append(column_name) %}
        {% endfor %}

    select
        {% for column_name, output_column_name in parent_column_map %}
            {{ column_name }} as {{ output_column_name }},
        {% endfor %}
        {% for column_name in log_columns.items %} {{ column_name }}, {% endfor %}
        topics[0]::string as topic_zero,
        {{ target.schema }}.concat_topics_and_data(topics, data) as event_data
    from log_data
    {% if is_incremental() %}
        where block_timestamp >= (select max(block_timestamp) from {{ this }})
    {% endif %}

{% endmacro %}
