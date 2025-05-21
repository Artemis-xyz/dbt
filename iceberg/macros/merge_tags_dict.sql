{% macro merge_tags_dict(tag_dict) %}
  {% if execute %}
    {% set m = model %}

    {% do run_query("""
        create table if not exists artemis_iceberg.metadata.table_tags (
            database_name string,
            schema_name string,
            table_name string,
            tag_key string,
            tag_value string,
            tagged_at timestamp
        )
    """) %}

    {% for k, v in tag_dict.items() %}
      {% set sql %}
        merge into artemis_iceberg.metadata.table_tags t
        using (
            select
                '{{ m.database }}' as database_name,
                '{{ m.schema }}' as schema_name,
                '{{ m.alias }}' as table_name,
                '{{ k }}' as tag_key,
                '{{ v }}' as tag_value,
                current_timestamp as tagged_at
        ) s
        on t.database_name = s.database_name
           and t.schema_name = s.schema_name
           and t.table_name = s.table_name
           and t.tag_key = s.tag_key
        when matched then update set
            tag_value = s.tag_value,
            tagged_at = s.tagged_at
        when not matched then insert (
            database_name, schema_name, table_name, tag_key, tag_value, tagged_at
        ) values (
            s.database_name, s.schema_name, s.table_name, s.tag_key, s.tag_value, s.tagged_at
        );
      {% endset %}
      {% do run_query(sql) %}
    {% endfor %}
  {% endif %}
{% endmacro %}