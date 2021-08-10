INSERT INTO my_table (table_value)
VALUES (%(filename)s)
ON CONFLICT(table_value)
DO NOTHING
