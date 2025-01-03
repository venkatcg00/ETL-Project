from NO_SQL_DB_Setup import (
    get_max_record_id,
    get_max_increment_id,
    query_by_id,
    query_by_key,
)

print(get_max_increment_id())
print(get_max_record_id())
print(query_by_key(100, "eq"))
print(query_by_id(100, "eq"))
