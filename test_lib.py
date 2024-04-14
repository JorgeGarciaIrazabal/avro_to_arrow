from avro_to_arrow import create_arrow_table_py, get_numbers

for i in get_numbers():
    print(i)

print(create_arrow_table_py())