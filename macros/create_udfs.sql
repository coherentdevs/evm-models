{% macro create_udfs() %}
    {{ decode_input() }}
    {{ decode_logs() }}
    {{ hex_to_int() }}
{% endmacro %}



