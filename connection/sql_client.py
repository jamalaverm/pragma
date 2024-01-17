import pymysql.cursors
from pymysql.connections import Connection
from table.table_schema import TableSchema


class SQLClient:
    def __init__(self, host: str, port: int, user: str, password: str):
        """
        Constructor of the class, set connection parameters.
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.charset = 'utf8mb4'

    def insert_data(self, data, schema: str, table_name: str):
        """
        This method constructs a SQL 'INSERT' query and executes it using the execute_many method of the
        MysqlConnector.
        """
        table_cols = TableSchema.get_str_cols(table_name)
        table_vals = TableSchema.get_str_vals(table_name)

        query = \
            f"insert into " \
            + f"{schema}.{table_name} {table_cols} values " + table_vals

        conn = self.__create_connection()
        try:
            with conn.cursor() as cursor:
                cursor.executemany(query, data)
            conn.commit()
        except Exception:
            conn.close()
            raise

        conn.close()

    def get_data_with_query(self, query: str):
        """
        Execute a query and return the result.
        """
        result = self.__execute_with_result(query)
        return result

    def get_data(self, schema: str, table_name: str):
        """
        This method gets all the data from a table / view using a SELECT SQL statement.
        """
        query = \
            f"select * from " \
            + f"{schema}.{table_name}"

        result = self.__execute_with_result(query)
        return result

    def insert_data_from_tb(self, schema_origin: str, table_origin: str, schema_dest: str, table_dest: str):
        """
        Insert the entire data from the origin table to the destination table, the columns / content should match.
        It doesn't apply any additional transformation or filter.
        """
        table_cols = TableSchema.get_str_cols(table_dest)
        query = \
            f"insert into {schema_dest}.{table_dest} {table_cols} " \
            + f"select * from {schema_origin}.{table_origin}"

        self.__execute_without_result(query)

    def truncate(self, schema: str, table_name: str):
        """
        Truncate a table given schem and table name.
        """
        query = \
            f"truncate table " \
            + f"{schema}.{table_name}"

        self.__execute_without_result(query)

    def __execute_without_result(self, query: str):
        """
        Execute a sql transaction, not meant to return any data.
        """
        conn = self.__create_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query)
            conn.commit()
        except Exception:
            conn.close()
            raise

        conn.close()

    def __execute_with_result(self, query: str):
        """
        Execute query and fetch result.
        """
        conn = self.__create_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall()
        except Exception:
            conn.close()
            raise

        conn.close()
        return result

    def __create_connection(self) -> Connection:
        """
        This is a private method that establishes a connection to the MySQL schema using the credentials provided
        during the initialization of the SQLClient object and returns the connection object.
        """
        conn = pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            charset=self.charset,
            cursorclass=pymysql.cursors.DictCursor
        )
        return conn
