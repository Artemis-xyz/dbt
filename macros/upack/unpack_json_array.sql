{% macro unpack_json_array(
    source_table,
    source_column,
    parent_columns=[],
    column_map=[],
    incremental_column="block_timestamp"
) %}
    /*
    By default get all the columns from the json and decode as strings
    Assumptions:
        - This macro expects a single level json that is a list of objects
        acceptable: [{"key1": "value1", "key2": "value2"}, {"key1": "value3", "key2": "value4"}]
        not acceptable: [{"key1": {"key2": "value1", "key3": "value2"}}] -- cannot decode key2 or key3
    Parameters:
        parent_columns: array of column names
        column_map: array of tuples with the following format (json_name, conversion_macro, column_name)
    Notes:
        - Add new conversion_macro to utils.sql
*/
    {% if column_map == [] %}
        {% set query %}
            select 
                distinct f.value::string AS column_name
            from 
                (
                    select OBJECT_KEYS(k.value) as json_keys
                    from {{ ref(source_table) }}, table(flatten(parse_json({{ source_column }}))) k
                ),
                lateral FLATTEN(input => json_keys) f
        {% endset %}

        {% set results = run_query(query) %}

        {% if execute %} {% set column_names = results.columns[0].values() %}
        {% else %} {% set column_names = [] %}
        {% endif %}
    {% endif %}

    select
        {% for extra_column in parent_columns %} {{ extra_column }}, {% endfor %}

        {% if column_map == [] %}
            {% for column_name in column_names %}
                {% if column_name == "from" or column_name == "to" %}  -- fix bug so there is no to or from returned
                    raw_txn_json.value:"{{ column_name }}"::string
                    as {{ column_name }}_unpacked
                {% else %}
                    raw_txn_json.value:"{{ column_name }}"::string as {{ column_name }}
                {% endif %}

                {% if not loop.last %},{% endif %}
            {% endfor %}
        {% else %}
            {% for json_name, conversion_macro, column_name in column_map %}
                {{ conversion_macro("value:" ~ json_name) }} as {{ column_name }}
                {% if not loop.last %},{% endif %}
            {% endfor %}
        {% endif %}
    from
        {{ ref(source_table) }},
        lateral flatten(input => parse_json({{ source_column }})) as raw_txn_json
    {% if is_incremental() %}
        where
            {{ incremental_column }}
            >= (select max({{ incremental_column }}) from {{ this }})
    {% endif %}
{% endmacro %}
