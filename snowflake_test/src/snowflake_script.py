import snowflake.connector
import os

def run_snowflake_query():
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    )

    try:
        cur = conn.cursor()
        cur.execute("SELECT CURRENT_VERSION()")
        result = cur.fetchone()
        print("Snowflake version:", result[0])
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    run_snowflake_query()

