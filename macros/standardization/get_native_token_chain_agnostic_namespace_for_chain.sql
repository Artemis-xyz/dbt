{% macro get_native_token_chain_agnostic_namespace_for_chain(chain) %}
    {% if chain == 'ethereum' %}
        'eip155:1:native'
    {% elif chain == 'arbitrum' %}
        'eip155:42161:native'
    {% elif chain == 'base' %}
        'eip155:8453:native'
    {% elif chain == 'optimism' %}
        'eip155:10:native'
    {% elif chain == 'sei' %}
        'eip155:1329:native'
    {% elif chain == 'solana' %}
        'solana:5eykt4usfv8p8njdtrepy1vzqkqzkvdp:native'
    {% else %}
        '{{ chain }}-native-token'
    {% endif %}
{% endmacro %}