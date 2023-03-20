{% macro create_udfs() %}
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
-- create
-- or replace function {{ target.schema }}.transform_to_json(input_string string)
-- returns string
-- language python
-- runtime_version = '3.8'
-- handler = 'transform_to_json'
-- as
-- $$
-- from typing import Dict
--
-- def transform_to_json(input_string) -> Dict:
--     return json.loads(input_string)
-- $$;
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

-- {% set sql %}
--         CREATE
-- api integration IF NOT EXISTS aws_ethereum_api api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::661245089684:role/snowflake-api-ethereum' api_allowed_prefixes = (
--             'https://e03pt6v501.execute-api.us-east-1.amazonaws.com/prod/',
--             'https://mryeusnrob.execute-api.us-east-1.amazonaws.com/dev/'
--         ) enabled = TRUE;
-- {% endset %}
--         {% do run_query(sql) %}
--
-- CREATE
-- OR REPLACE EXTERNAL FUNCTION {{ target.schema }}.udf_bulk_decode_logs(
--         json OBJECT
--     ) returns ARRAY api_integration = aws_ethereum_api AS {% if target.name == "prod" %}
--         'https://e03pt6v501.execute-api.us-east-1.amazonaws.com/prod/bulk_decode_logs'
--     {% else %}
--         'https://mryeusnrob.execute-api.us-east-1.amazonaws.com/dev/bulk_decode_logs'
--     {%- endif %};
