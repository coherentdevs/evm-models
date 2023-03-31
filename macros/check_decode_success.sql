{% macro check_decode_success() %}
create
or replace function {{ target.schema }}.check_decode_success(decoded_array ARRAY)
returns boolean
language python
runtime_version = '3.8'
handler = 'check_decode_success'
as
$$
def check_decode_success(decoded_array) -> bool:
    for input_str in decoded_array:
        if "unable to decode" in input_str or "unknown type detected" in input_str:
            return False
    return True
$$;
{% endmacro %}