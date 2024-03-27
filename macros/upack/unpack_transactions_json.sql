{% macro unpack_transactions_json(
    chain,
    transaction_column_map=[("hash", to_string, "transaction_hash")],
    receipts_column_map=[
        ("transactionHash", to_string, "transaction_hash")
    ]
) %}

    with
        transaction_data as (
            {{
                unpack_json_array(
                    "fact_" ~ chain ~ "_blocks",
                    "transactions",
                    parent_columns=["block_timestamp"],
                    column_map=transaction_column_map,
                )
            }}
        ),
        receipts_data as (
            {{
                unpack_json_array(
                    "fact_" ~ chain ~ "_blocks",
                    "receipts",
                    parent_columns=["block_timestamp"],
                    column_map=receipts_column_map,
                )
            }}
        )

        {% set transaction_columns = namespace(items=[]) %}
        {% for json_name, type_conversion, column_name in transaction_column_map %}
            {% do transaction_columns.items.append(column_name) %}
        {% endfor %}

        {% set receipt_columns = namespace(items=[]) %}
        {% for json_name, type_conversion, column_name in receipts_column_map %}
            {% if column_name not in transaction_columns.items %}
                {% do receipt_columns.items.append(column_name) %}
            {% endif %}
        {% endfor %}

    select
        transaction_data.block_timestamp,
        {% for column_name in transaction_columns.items %}
            transaction_data.{{ column_name }},
        {% endfor %}
        {% for column_name in receipt_columns.items %}
            receipts_data.{{ column_name }} {% if not loop.last %},{% endif %}
        {% endfor %}
    from transaction_data
    left join
        receipts_data
        on transaction_data.transaction_hash = receipts_data.transaction_hash
    {% if is_incremental() %}
        where
            transaction_data.block_timestamp
            >= (select max(block_timestamp) from {{ this }})
    {% endif %}
{% endmacro %}
