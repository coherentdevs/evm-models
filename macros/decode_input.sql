{% macro decode_input() %}
create
or replace function {{ target.schema }}.decode_input(input string, hashed_signature string)
returns ARRAY
language python
runtime_version = '3.8'
packages = ('regex', 'gmpy2')
handler = 'decode_input'
as
$$
import regex
import gmpy2
def decode_input(input_str, hashed_signature):
    types = extract_arguments(hashed_signature)
    offset = 0
    decoded_inputs = []
    stripped_method_id_data = input_str[10:]
    for type in types:
        try:
            parsed_type = parseType(type)
            decoded_input, characters_read = decodeSingle(parsed_type, stripped_method_id_data, offset)
        except:
            decoded_input = "unable to decode " + parsed_type["name"]
            characters_read = 64

        decoded_inputs.append(decoded_input)
        offset += characters_read

    return decoded_inputs

def extract_arguments(func_signature):
    start = func_signature.find("(")
    end = func_signature.rfind(")")
    if start == -1 or end == -1:
        return []

    arguments_str = func_signature[start + 1 : end]
    if not arguments_str.strip():
        return []

    pattern = regex.compile(r',(?![^\[\]]*\]|[^\(\)]*\))')
    arguments = [arg.strip() for arg in pattern.split(arguments_str)]

    return arguments

def parseType(type):
    tuple = None
    is_array = type.endswith("[]")
    if is_array:
        name = "array"
        base_type = type[:-2]
        if base_type.startswith("(") and base_type.endswith(")"):
            tuple = base_type
            base_type = "tuple"
        ret = {
            "isArray": is_array,
            "name": name,
            "arrayType": base_type,
            "tuple": tuple,
        }
    else:
        if type.startswith("(") and type.endswith(")"):
            type = "tuple"
            tuple = type
        ret = {
            "isArray": is_array,
            "name": type,
            "arrayType": None,
            "tuple": tuple,
        }
    return ret

def decode_uint(data, offset):
    bytes_to_read = 32
    characters_to_read = bytes_to_read * 2
    uint_stripped = data[offset:offset + characters_to_read].lstrip("0")
    if len(uint_stripped) == 0:
        return "0", characters_to_read
    large_int = gmpy2.mpz("0x" + uint_stripped)
    return gmpy2.digits(large_int), characters_to_read

def decodeSingle(parsed_type, data, offset, dynamic_bytes_data_start = 0):
    name = parsed_type["name"]
    if name == "array":
        decoded_array = []
        length_offset = int(gmpy2.mpz("0x" + data[offset:offset + 64], 16).digits(10)) * 2
        length = int(gmpy2.mpz("0x" + data[length_offset: length_offset + 64], 16).digits(10))
        dynamic_bytes_data_start = length_offset + 64
        array_offset = length_offset + 64

        for _ in range(0, length):
            type = parsed_type["arrayType"]
            new_input = {'isArray': False, 'name': type, 'tuple': parsed_type["tuple"]}
            if type == "bytes" or type == "string":
                result, characters_read = decodeSingle(new_input, data, array_offset, dynamic_bytes_data_start = dynamic_bytes_data_start)
            else:
                result, characters_read = decodeSingle(new_input, data, array_offset)
            decoded_array.append(result)
            array_offset += characters_read
        return str(decoded_array), 64
    elif name == "tuple":
        decoded_tuple = []
        tuple = parsed_type["tuple"]
        total_characters_read = 0
        tuple_offset = 0
        extracted_types = extract_arguments(tuple)
        for type in extracted_types:
            if type.startswith("(") and type.endswith(")"):
                new_input = {'isArray': False, 'name': "tuple", "tuple": type}
                result, characters_read = decodeSingle(new_input, data, offset + tuple_offset)
            else:
                new_input = {'isArray': False, 'name': type}
                result, characters_read = decodeSingle(new_input, data, offset + tuple_offset)
            decoded_tuple.append(result)
            total_characters_read += characters_read
            tuple_offset += characters_read
        return str(decoded_tuple), total_characters_read
    elif name.startswith('bytes') and name != "bytes":
        bytes_to_read = 32
        characters_to_read = bytes_to_read * 2
        return "0x" + data[offset:offset + characters_to_read], characters_to_read
    elif name.startswith('uint'):
        decoded_uint, characters_read = decode_uint(data, offset)
        return decoded_uint, characters_read
    elif name.startswith('int'):
        bytes_to_read = 32
        characters_to_read = bytes_to_read * 2
        int_block = data[offset:offset + characters_to_read]
        first_hex = int_block[0]
        if int(gmpy2.mpz("0x" + first_hex, 16).digits(10)) >= 8:
            base_2 = gmpy2.mpz("0x" + int_block, 16).digits(2)

            flipped_binary_str = ""
            for bit in base_2:
                flipped_bit = '0' if bit == '1' else '1'
                flipped_binary_str += flipped_bit
            incremented_binary_result = gmpy2.mpz(flipped_binary_str, 2) + 1

            return "-" + gmpy2.digits(incremented_binary_result, 10), characters_to_read
        else:
            decoded_uint, characters_read = decode_uint(data, offset)
            return decoded_uint, characters_read
    elif name == "address":
        bytes_to_read = 32
        characters_to_read = bytes_to_read * 2
        decoded_num = "0x" + data[offset + 24:offset + characters_to_read]
        return decoded_num, characters_to_read
    elif name == "bool":
        bytes_to_read = 32
        characters_to_read = bytes_to_read * 2
        decoded_bool = data[offset:offset + characters_to_read][-1]
        if decoded_bool == "0":
            return "False", characters_to_read
        else:
            return "True", characters_to_read
    elif name == "string":
        length_offset = int(gmpy2.mpz("0x" + data[offset:offset + 64], 16).digits(10)) * 2
        length_start = dynamic_bytes_data_start + length_offset
        length_end = length_start + 64
        length = int(gmpy2.mpz("0x" + data[length_start: length_end], 16).digits(10)) * 2
        if length == 0: return "", 64
        string_start = length_end
        return bytes.fromhex(data[string_start: string_start + length]).decode('utf-8'), 64
    elif name == "bytes":
        length_offset = int(gmpy2.mpz("0x" + data[offset:offset + 64], 16).digits(10)) * 2
        length_start = dynamic_bytes_data_start + length_offset
        length_end = length_start + 64
        length = int(gmpy2.mpz("0x" + data[length_start: length_end], 16).digits(10)) * 2
        if length == 0: return "0x", 64
        string_start = length_end
        return "0x" + data[string_start: string_start + length], 64
    else:
        return "unknown type detected, unable to decode", 64
$$;
{% endmacro %}