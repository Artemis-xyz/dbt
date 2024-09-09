{% macro generate_combinations(chains) %}
    {% set all_combinations = [] %}

    {% for r in range(1, chains | length + 1) %}
        {% for combination in  modules.itertools.combinations(chains, r) %}
            {% set all_combinations = all_combinations.append(combination) %}
        {% endfor %}
    {% endfor %}

    {{ return(all_combinations) }}
{% endmacro %}
