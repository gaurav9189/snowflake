import snowflake.connector
import os


def connection():
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"))
    return conn


cur = connection().cursor()

try:
    results = cur.execute("select 1, 2, 4")
    print(results.fetchall())
finally:
    print("all done")
