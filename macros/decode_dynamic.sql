{% macro decode_dynamic() %}
create
or replace function {{ target.schema }}.decode_dynamic(dynamic_types ARRAY, input_data string, array_offset int)
returns ARRAY
language python
runtime_version = '3.8'
packages = ('gmpy2')
handler = 'decode_dynamic'
as
$$
import gmpy2
def decode_dynamic(dynamic_types, input_data, array_offset = 0):
    offset = 0 + array_offset
    decoded_input = []
    for dynamic_type in dynamic_types:
        try:
            if dynamic_type == "string":
                length_offset = int(gmpy2.mpz("0x" + input_data[offset:offset + 64], 16).digits(10)) * 2
                length_start = length_offset
                length = int(gmpy2.mpz("0x" + input_data[length_start: length_start + 64], 16).digits(10)) * 2
                if length == 0: decoded_input.append("")
                string_start = length_start + 64
                decoded_input.append(bytes.fromhex(input_data[string_start: string_start + length]).decode('utf-8'))
            elif dynamic_type == "bytes":
                length_offset = int(gmpy2.mpz("0x" + input_data[offset:offset + 64], 16).digits(10)) * 2
                length_start = length_offset
                length = int(gmpy2.mpz("0x" + input_data[length_start: length_start + 64], 16).digits(10)) * 2
                if length == 0: decoded_input.append("0x")
                string_start = length_start + 64
                decoded_input.append("0x" + input_data[string_start: string_start + length])
            elif dynamic_type.endswith("[]"):
                array_type = dynamic_type[:-2]
                if array_type.endswith("]") or array_type.endswith(")"):
                    decoded_input.append("unable to decode nested dynamic types")
                    offset += 64
                    break
                else:
                    decoded_array = []
                    length_offset = int(gmpy2.mpz("0x" + input_data[offset:offset + 64], 16).digits(10)) * 2
                    length = int(gmpy2.mpz("0x" + input_data[length_offset: length_offset + 64], 16).digits(10))
                    if array_type == "bytes" or array_type == "string":
                        array_offset = 0
                    else:
                        array_offset = length_offset + 64
                    for _ in range(0, length):
                        if array_type == "bytes" or array_type == "string":
                            decoded_res = decode_dynamic([array_type], input_data[length_offset+64:], array_offset)
                        else:
                            decoded_res = decode_dynamic([array_type], input_data, array_offset)
                        array_offset += 64
                        decoded_array.append(decoded_res[0])
                    decoded_input.append(decoded_array)
            elif dynamic_type.startswith('(') and dynamic_type.endswith(')'):
                decoded_input.append("unable to decode tuples")
                offset += 64
                break
            elif dynamic_type.startswith('uint'):
                decoded_uint = decode_uint(input_data, offset)
                decoded_input.append(decoded_uint)
            elif dynamic_type.startswith('int'):
                int_block = input_data[offset:offset + 64]
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
                    decoded_uint = decode_uint(input_data , offset)
                    decoded_input.append(decoded_uint)
            elif dynamic_type == "address":
                decoded_num = "0x" + input_data[offset + 24:offset + 64]
                decoded_input.append(decoded_num)
            elif dynamic_type == "bool":
                decoded_bool = input_data[offset:offset + 64][-1]
                if decoded_bool == "0":
                    decoded_input.append("False")
                else:
                    decoded_input.append("True")
            elif dynamic_type.startswith("bytes") and dynamic_type != "bytes":
                decoded_input.append("0x" + input_data[offset:offset + 64])
            else:
                decoded_input.append("unable to decode dynamic type" + dynamic_type)
        except:
            decoded_input.append("unable to decode " + dynamic_type)
        offset += 64
    return decoded_input

def decode_uint(data, offset):
    uint_stripped = data[offset:offset + 64].lstrip("0")
    if len(uint_stripped) == 0:
        return "0"
    large_int = gmpy2.mpz("0x" + uint_stripped)
    return gmpy2.digits(large_int)
$$
{% endmacro %}