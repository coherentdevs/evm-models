{% macro decode_logs() %}
create
or replace function {{ target.schema }}.decode_logs(abi string, data string, topics string)
returns ARRAY
language python
runtime_version = '3.8'
packages = ('gmpy2', 'ujson', 'regex')
handler = 'decode_logs'
as
$$
import gmpy2
import ujson
import regex

def decode_logs(abi, data, topics):
    try:
        topics_cleaned = topics.replace('[', '').replace(']', '').replace('"', '')
        topics_array = topics_cleaned.split(",")
        json_object = ujson.loads(abi)
        topics_index = 1
        decoded_result = {}
        data_offset = 0
        decode_success = True
        if data == "":
            for input_obj in json_object['inputs']:
                decode_single_res, decoded_successfully, _bytes_read = decode_single(topics_array[topics_index][2:], input_obj['type'], 0)
                topics_index += 1

                if decoded_successfully == False:
                    decode_success = False
                    break

                decoded_result[input_obj['name']] = decode_single_res
            return [decoded_result, decode_success]
        elif len(topics_array) == 1:
            for input_obj in json_object['inputs']:
                decode_single_res, decoded_successfully, bytes_read = decode_single(data, input_obj['type'], data_offset)
                data_offset += bytes_read

                if decoded_successfully == False:
                    decode_success = False
                    break
                decoded_result[input_obj['name']] = decode_single_res
        else:

            for input_obj in json_object['inputs']:
                if input_obj['indexed'] == True:
                    decode_single_res, decoded_successfully, _bytes_read = decode_single(topics_array[topics_index][2:], input_obj['type'], 0)
                    topics_index += 1
                else:
                    decode_single_res, decoded_successfully, bytes_read = decode_single(data, input_obj['type'], data_offset)
                    data_offset += bytes_read

                if decoded_successfully == False:
                    decode_success = False
                    break
                decoded_result[input_obj['name']] = decode_single_res

        return [decoded_result, decode_success]
    except Exception as e:
        return [str(e), False]

def decode_single(data, type, offset):
    if data == "": return "", False
    if type == "string":
        length_offset = int(gmpy2.mpz("0x" + data[offset:offset + 64], 16).digits(10)) * 2
        length_start = length_offset
        length = int(gmpy2.mpz("0x" + data[length_start: length_start + 64], 16).digits(10)) * 2
        if length == 0: return "", True, 64
        string_start = length_start + 64
        return bytes.fromhex(data[string_start: string_start + length]).decode('utf-8'), True, 64
    elif type == "bytes":
        length_offset = int(gmpy2.mpz("0x" + data[offset:offset + 64], 16).digits(10)) * 2
        length_start = length_offset
        length = int(gmpy2.mpz("0x" + data[length_start: length_start + 64], 16).digits(10)) * 2
        if length == 0: return "0x", True, 64
        string_start = length_start + 64
        return "0x" + data[string_start: string_start + length], True, 64
    elif type.startswith('(') and type.endswith(')'):
        tuple_types = parse_tuple_types(type[1:-1])

        if contains_dynamic_types(tuple_types):
            tuple_start = int(gmpy2.mpz("0x" + data[offset:offset + 64], 16).digits(10)) * 2
            tuple_offset = 0
            decoded_array = []
            for t in tuple_types:
                decoded_res, decode_success, _bytes_read = decode_single(data[tuple_start:], t.strip(), tuple_offset)
                if decode_success == False: return "unable to decode tuple", False, 0
                tuple_offset += 64
                decoded_array.append(decoded_res)
            return tuple(decoded_array), True, 64
        else:
            decoded_array = []
            total_bytes_read = 0
            for t in tuple_types:
                decoded_res, decode_success, bytes_read = decode_single(data, t.strip(), offset)
                if decode_success == False: return "unable to decode tuple", False, 0
                total_bytes_read += bytes_read
                offset += bytes_read
                decoded_array.append(decoded_res)
            return tuple(decoded_array), True, total_bytes_read

    elif type.endswith("]"):

        if type.endswith("[]") == False:
            match = regex.search(r'\[(\d+)\]\Z', type)
            if match == None: return "invalid type", False, 0
            length = int(match.group(1))
            last_open_bracket_index = type.rfind('[')
            array_type = type[:last_open_bracket_index]
            decoded_array = []

            if length > 10000: return "unable to decode array of length " + str(length), False, 0

            array_offset = offset
            total_bytes_read = 0
            for _ in range(0, length):
                decoded_res, decode_success, bytes_read = decode_single(data, array_type, array_offset)
                array_offset += bytes_read
                total_bytes_read += bytes_read
                if decode_success == False: return "unable to decode array", False, 0
                decoded_array.append(decoded_res)

            return decoded_array, True, total_bytes_read
        else:
            array_type = type[:-2]
            decoded_array = []


            length_offset = int(gmpy2.mpz("0x" + data[offset:offset + 64], 16).digits(10)) * 2
            length = int(gmpy2.mpz("0x" + data[length_offset: length_offset + 64], 16).digits(10))
            if length > 10000: return "unable to decode array of length " + str(length), False, 0

            if array_type == "bytes" or array_type == "string" or (array_type.startswith('(') and array_type.endswith(')') and contains_dynamic_types(parse_tuple_types(array_type[1:-1]))):

                array_offset = 0
                for _ in range(0, length):
                    decoded_res, decode_success, _bytes_read = decode_single(data[length_offset+64:], array_type, array_offset)
                    if decode_success == False: return "unable to decode array", False, 0
                    array_offset += 64
                    decoded_array.append(decoded_res)
            else:
                array_offset = length_offset + 64
                for _ in range(0, length):
                    decoded_res, decode_success, bytes_read = decode_single(data, array_type, array_offset)
                    array_offset += bytes_read
                    if decode_success == False: return "unable to decode array", False, 0
                    decoded_array.append(decoded_res)

            return decoded_array, True, 64
    elif type.startswith('uint'):
        decoded_uint = decode_uint(data, offset)
        return decoded_uint, True, 64
    elif type.startswith('int'):
        int_block = data[offset:offset + 64]
        unsigned_value = gmpy2.mpz("0x" + int_block, 16)
        signed_value = unsigned_value

        if unsigned_value >> 255:
            signed_value = -(~unsigned_value & ((1 << 256) - 1)) - 1

        return float(signed_value), True, 64
    elif type == "address":
        address = "0x" + data[offset + 24:offset + 64]
        return address, True, 64
    elif type == "bool":
        decoded_bool = data[offset:offset + 64][-1]
        if decoded_bool == "0":
            return "False", True, 64
        else:
            return "True", True, 64
    elif type.startswith("bytes") and type != "bytes":
        return "0x" + data[offset:offset + 64], True, 64
    else:
        return "unknown type detected", False, 0

def decode_uint(data, offset):
    uint_stripped = data[offset:offset + 64].lstrip("0")
    if len(uint_stripped) == 0:
        return "0"
    value = gmpy2.mpz("0x" + uint_stripped)
    return float(value)

def contains_dynamic_types(tuple_types):
    for t in tuple_types:
        if t == "string" or t == "bytes" or t.endswith("[]"):
            return True

    return False

def parse_tuple_types(type):
    types = []
    depth = 0
    start = 0
    for i, c in enumerate(type):
        if c == '(':
            depth += 1
        elif c == ')':
            depth -= 1
        elif c == ',' and depth == 0:
            types.append(type[start:i].strip())
            start = i + 1

    types.append(type[start:].strip())

    return types
    
$$;
{% endmacro %}
