{% macro create_udfs() %}
    {{ decode_input() }}
    {{ decode_fixed() }}
    {{ extract_arguments() }}
    {{ check_decode_success() }}
    {{ hex_to_int() }}
{% endmacro %}



