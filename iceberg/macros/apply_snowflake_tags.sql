{% macro apply_snowflake_tags(target_relation, meta=none) %}
  {% if meta is not none %}
    {% for tag in meta %}
      ALTER TABLE {{ target_relation }} SET TAG {{ tag.tag_name }} = '{{ tag.tag_value }}';
    {% endfor %}
  {% endif %}
{% endmacro %}