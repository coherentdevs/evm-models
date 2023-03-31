{% macro hex_to_int() %}
create
or replace function {{ target.schema }}.hex_to_int(hex string)
returns string
language python
runtime_version = '3.8'
handler = 'hex_to_int'
as
$$
def hex_to_int(hex) -> str:
    return (str(int(hex, 16)) if hex and hex != "0x" else None)
$$;
{% endmacro %}