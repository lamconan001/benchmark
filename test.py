from runner import parse_columns

sql_schema = """
CREATE TABLE test_table (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    created_at TIMESTAMP
);
"""

columns = parse_columns(sql_schema)
print("Parsed Columns:") 
for col in columns:
    print(col)