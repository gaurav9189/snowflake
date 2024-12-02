import snowflake.connector
import os


# Establish connection
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT")
)

# Run a query
try:
    with conn.cursor() as cur:
        cur.execute("show roles")
        results = cur.fetchall()
        # print("Query executed successfully:", results)
        print(results[0])
finally:
    conn.close()
