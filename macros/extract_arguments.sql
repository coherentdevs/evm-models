{% macro extract_arguments() %}
create
or replace function {{ target.schema }}.extract_arguments(function_signature string)
returns ARRAY
language python
runtime_version = '3.8'
packages = ('regex')
handler = 'extract_arguments'
as
$$
import regex
def extract_arguments(func_signature):
    start = func_signature.find("(")
    end = func_signature.rfind(")")
    if start == -1 or end == -1:
        return []

    arguments_str = func_signature[start + 1 : end]
    if not arguments_str.strip():
        return [[], 0, False]

    pattern = regex.compile(r',(?![^\[\]]*\]|[^\(\)]*\))')
    dynamic_type = False
    arguments = pattern.split(arguments_str)
    for arg in arguments:
        if arg == "string" or arg == "bytes" or arg.endswith("[]") or arg.endswith(")"):
            dynamic_type = True

    return [arguments, len(arguments), dynamic_type]
$$;
{% endmacro %}