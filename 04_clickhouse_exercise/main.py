from clickhouse_driver import Client

# ClickHouse

ch_client = Client(
    host='localhost',
    user='user',
    password='strongpassword',
    port=9000  # TCP порт ClickHouse
)
ch_client.execute("CREATE TABLE IF NOT EXISTS test (id UInt32, name String) ENGINE = MergeTree() ORDER BY id")
ch_client.execute("INSERT INTO test (id, name) VALUES", [(1, 'Bob')])
rows = ch_client.execute("SELECT * FROM test")
print("ClickHouse:", rows)
