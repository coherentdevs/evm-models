{% macro decode_fixed() %}
create
or replace function {{ target.schema }}.decode_fixed(fixed_types ARRAY, input_data string)
returns ARRAY
language python
runtime_version = '3.8'
packages = ('gmpy2')
handler = 'decode_fixed'
as
$$
import gmpy2
def decode_fixed(fixed_types, input_data):
    stripped_method_id_data = input_data[10:]
    offset = 0
    decoded_input = []
    for fixed_type in fixed_types:
        try:
            if fixed_type.startswith('uint'):
                decoded_uint = decode_uint(stripped_method_id_data, offset)
                decoded_input.append(decoded_uint)
            elif fixed_type.startswith('int'):
                int_block = stripped_method_id_data[offset:offset + 64]
                first_hex = int_block[0]
                if int(gmpy2.mpz("0x" + first_hex, 16).digits(10)) >= 8:
                    base_2 = gmpy2.mpz("0x" + int_block, 16).digits(2)

                    flipped_binary_str = ""
                    for bit in base_2:
                        flipped_bit = '0' if bit == '1' else '1'
                        flipped_binary_str += flipped_bit
                    incremented_binary_result = gmpy2.mpz(flipped_binary_str, 2) + 1

                    decoded_input.append("-" + gmpy2.digits(incremented_binary_result, 10))
                else:
                    decoded_uint = decode_uint(stripped_method_id_data , offset)
                    decoded_input.append(decoded_uint)
            elif fixed_type == "address":
                decoded_num = "0x" + stripped_method_id_data[offset + 24:offset + 64]
                decoded_input.append(decoded_num)
            elif fixed_type == "bool":
                decoded_bool = stripped_method_id_data[offset:offset + 64][-1]
                if decoded_bool == "0":
                    decoded_input.append("False")
                else:
                    decoded_input.append("True")
            elif fixed_type.startswith("bytes") and fixed_type != "bytes":
                decoded_input.append("0x" + stripped_method_id_data[offset:offset + 64])
            else:
                decoded_input.append("unknown type detected")
        except:
            decoded_input.append("unable to decode " + fixed_type)

        offset += 64

    return decoded_input

def decode_uint(data, offset):
    uint_stripped = data[offset:offset + 64].lstrip("0")
    if len(uint_stripped) == 0:
        return "0"
    large_int = gmpy2.mpz("0x" + uint_stripped)
    return gmpy2.digits(large_int)
$$;
{% endmacro %}