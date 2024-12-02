import os
import logging
import snowflake.connector


class Snowflakemanager:
    def __init__(self, snowflake_user_env_var='SNOWFLAKE_USER',
                 snowflake_password_env_var='SNOWFLAKE_PASSWORD',
                 snowflake_account_env_var='SNOWFLAKE_ACCOUNT'):
        """
        Initializes the Snowflakemanager class.

        Args:
            snowflake_user_env_var (str): Environment variable name for the Snowflake user.
            snowflake_password_env_var (str): Environment variable name for the Snowflake password.
            snowflake_account_env_var (str): Environment variable name for the Snowflake account.
        """
        self.snowflake_user_env_var = snowflake_user_env_var
        self.snowflake_password_env_var = snowflake_password_env_var
        self.snowflake_account_env_var = snowflake_account_env_var

        self.connection_params = {
            'user': os.getenv(snowflake_user_env_var),
            'password': os.getenv(snowflake_password_env_var),
            'account': os.getenv(snowflake_account_env_var)
        }

        if not all(self.connection_params.values()):
            raise ValueError("All connection parameters must be set")

        logging.info(
            f"Initialized Snowflakemanager with connection params: {self.connection_params}")

    def connect(self):
        """
        Establishes a connection to Snowflake.

        Returns:
            snowflake.connector.Connection: The established connection object.
        """
        try:
            conn = snowflake.connector.connect(
                user=self.connection_params['user'],
                password=self.connection_params['password'],
                account=self.connection_params['account']
            )
            logging.info("Connected to Snowflake")
            return conn
        except snowflake.connector.errors.DatabaseError as e:
            logging.error(f"Failed to connect to Snowflake: {e}")
            raise

    def execute_query(self, query, conn):
        """
        Executes a query on Snowflake.

        Args:
            query (str): The query to execute.
            conn (snowflake.connector.Connection): The established connection object.

        Returns:
            list: The query results.
        """
        if not query:
            raise ValueError("Query cannot be empty")

        try:
            cursor = conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            logging.info(f"Executed query: {query}")
            return results
        except snowflake.connector.errors.DatabaseError as e:
            logging.error(f"Failed to execute query: {e}")
            raise

    def run_query(self, query):
        """
        Executes a query on Snowflake and returns the results.

        Args:
            query (str): The query to execute.

        Returns:
            list: The query results.
        """
        conn = self.connect()
        try:
            results = self.execute_query(query, conn)
            return results
        finally:
            conn.close()


# Usage example
snow = Snowflakemanager()
results = snow.run_query("SELECT * FROM aider_db.public.gaurav_test")
print(results)

# use this class to connect to snowflake and extend the class to include running quries via a class method
