{% macro create_udfs() %}
create or replace function {{ target.schema }}.hex_to_int(hex string)
returns string
language python
runtime_version = '3.8'
handler = 'hex_to_int'
as
$$
def hex_to_int(hex) -> str:
    return (str(int(hex, 16)) if hex and hex != "0x" else None)
$$;

-- create or replace function {{ target.schema }}.decode_transaction_input(abi string, input string)
-- returns string
-- language python
-- runtime_version = '3.8'
-- handler = 'decode_transaction_input'
-- as
-- $$
-- from copy import deepcopy
-- from web3_input_decoder import decode_function
--
-- def decode_transaction_input(abi, input) -> str:
--     return decode_function(abi, input)
-- $$;
{% endmacro %}