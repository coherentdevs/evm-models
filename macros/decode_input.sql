{% macro decode_input() %}
create
or replace function {{ target.schema }}.decode_input(input string, hashed_signature string)
returns ARRAY
language python
runtime_version = '3.8'
packages = ('ujson', 'gmpy2')
handler = 'decode_input'
as
$$
import gmpy2
import ujson
def decode_input(abi, data):
    try:
        json_object = ujson.loads(abi)
        decoded_result = {}
        data_offset = 0
        decode_success = True
        for input_obj in json_object['inputs']:
            decode_single_res, decoded_successfully = decode_single(data, input_obj['type'], data_offset)
            data_offset += 64

            if decoded_successfully == False:
                decode_success = False
                break
            decoded_result[input_obj['name']] = decode_single_res

        return [decoded_result, decode_success]
    except Exception as e:
        return [str(e), False]

def decode_single(data, type, offset):
    if type == "string":
        length_offset = int(gmpy2.mpz("0x" + data[offset:offset + 64], 16).digits(10)) * 2
        length_start = length_offset
        length = int(gmpy2.mpz("0x" + data[length_start: length_start + 64], 16).digits(10)) * 2
        if length == 0: return "", True
        string_start = length_start + 64
        return bytes.fromhex(data[string_start: string_start + length]).decode('utf-8'), True
    elif type == "bytes":
        length_offset = int(gmpy2.mpz("0x" + data[offset:offset + 64], 16).digits(10)) * 2
        length_start = length_offset
        length = int(gmpy2.mpz("0x" + data[length_start: length_start + 64], 16).digits(10)) * 2
        if length == 0: return "0x", True
        string_start = length_start + 64
        return "0x" + data[string_start: string_start + length], True
    elif type.endswith("[]"):
        array_type = type[:-2]
        if array_type.endswith("]") or array_type.endswith(")"):
            return "unable to decode nested dynamic types", False
        else:
            decoded_array = []
            length_offset = int(gmpy2.mpz("0x" + data[offset:offset + 64], 16).digits(10)) * 2
            length = int(gmpy2.mpz("0x" + data[length_offset: length_offset + 64], 16).digits(10))
            if length > 10000: return "unable to decode array of length " + str(length), False
            if array_type == "bytes" or array_type == "string":
                array_offset = 0
            else:
                array_offset = length_offset + 64
            for _ in range(0, length):
                if array_type == "bytes" or array_type == "string":
                    decoded_res, decode_success = decode_single(data[length_offset+64:], array_type, array_offset)
                else:
                    decoded_res, decode_success = decode_single(data, array_type, array_offset)
                array_offset += 64
                decoded_array.append(decoded_res)
            return decoded_array, True
    elif type.startswith('uint'):
        decoded_uint = decode_uint(data, offset)
        return decoded_uint, True
    elif type.startswith('int'):
        int_block = data[offset:offset + 64]
        first_hex = int_block[0]
        if int(gmpy2.mpz("0x" + first_hex, 16).digits(10)) >= 8:
            base_2 = gmpy2.mpz("0x" + int_block, 16).digits(2)

            flipped_binary_str = ""
            for bit in base_2:
                flipped_bit = '0' if bit == '1' else '1'
                flipped_binary_str += flipped_bit
            incremented_binary_result = gmpy2.mpz(flipped_binary_str, 2) + 1

            return "-" + gmpy2.digits(incremented_binary_result, 10), True
        else:
            decoded_uint = decode_uint(data , offset)
            return decoded_uint, True
    elif type == "address":
        decoded_num = "0x" + data[offset + 24:offset + 64]
        return decoded_num, True
    elif type == "bool":
        decoded_bool = data[offset:offset + 64][-1]
        if decoded_bool == "0":
            return "False", True
        else:
            return "True", True
    elif type.startswith("bytes") and type != "bytes":
        return "0x" + data[offset:offset + 64], True
    else:
        return "unknown type detected", False


def decode_uint(data, offset):
    uint_stripped = data[offset:offset + 64].lstrip("0")
    if len(uint_stripped) == 0:
        return "0"
    large_int = gmpy2.mpz("0x" + uint_stripped)
    return gmpy2.digits(large_int)
$$;
{% endmacro %}