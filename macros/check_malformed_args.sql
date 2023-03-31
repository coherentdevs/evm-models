{% macro check_malformed_args_present() %}
create
or replace function {{ target.schema }}.check_malformed_args_present(args ARRAY)
returns boolean
language python
runtime_version = '3.8'
packages = ('regex')
handler = 'check_malformed_args_present'
as
$$
import regex
def check_malformed_args_present(args) -> bool:
    for arg in args:
        open_paren_count = len(regex.findall(r'\(', arg))
        close_paren_count = len(regex.findall(r'\)', arg))
        open_bracket_count = len(regex.findall(r'\[', arg))
        close_bracket_count = len(regex.findall(r'\]', arg))
        if open_paren_count != close_paren_count or open_bracket_count != close_bracket_count:
            return True
    return False
$$;
{% endmacro %}