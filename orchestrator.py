import configparser
from connection.csv_client import CSVClient
from connection.sql_client import SQLClient
from datetime import datetime
import numpy as np


class Orchestrator:

    def __init__(self, config_path: str):
        """
        Initialize the orchestrator by reading and setting the connection to mysql.
        """
        config = configparser.ConfigParser()
        config.read(config_path)
        c_sql = config["sql"]

        self.db_raw = c_sql["db_raw"]
        self.db_data = c_sql["db_data"]
        self.db_log = c_sql["db_log"]
        self.table = "purchase"
        self.sql = SQLClient(c_sql["hostname"], int(c_sql["port"]), c_sql["user"], c_sql["pass"])

    def insert_file(self, file_path: str):
        """
        Insert a file into the db and calculate its corresponding metrics.
        """
        data = self.__process_file(file_path)
        try:
            # Insert into raw schema
            self.sql.insert_data(data, self.db_raw, self.table)
            metrics = self.__calculate_metrics()
            print(metrics)
            # Insert micro batch into the full table
            self.sql.insert_data_from_tb(self.db_raw, self.table, self.db_data, self.table)
        except Exception:
            print("Exception")
        finally:
            # Truncate Raw Table
            self.sql.truncate(self.db_raw, self.table)

    @staticmethod
    def __process_file(file_path: str):
        """
        Read a csv file, and apply some quality rule to be able to store the data into mysql.
        """
        df = CSVClient.get_data(file_path)
        df["price"] = df["price"].replace(np.nan, 0)
        df["timestamp"] = df["timestamp"].apply(
            lambda x: datetime.strptime(x, "%m/%d/%Y").strftime("%Y-%m-%d"))
        data = df.to_records(index=False).tolist()
        return data

    def __calculate_metrics(self):
        """
        Calculate the accumulated metrics at every iteration.
        """
        new_metrics = self.sql.get_data(self.db_log, "new_purchases")
        prev_metrics = self.sql.get_data(self.db_log, "prev_purchases")

        # Calculate Accumulated metrics or initialize table if there's no previous metrics available
        if len(prev_metrics) == 0:
            data = [tuple(row.values()) for row in new_metrics]
            self.sql.insert_data(data, self.db_log, "purchase_history")
        else:
            new_m = new_metrics[0]
            prev_m = prev_metrics[0]
            count = new_m["count"] + prev_m["count"]
            avg_price = (new_m["count"]*new_m["avg_price"] + prev_m["count"]*prev_m["avg_price"]) / count
            min_price = min(new_m["min_price"], prev_m["min_price"])
            max_price = max(new_m["max_price"], prev_m["max_price"])
            data = [(count, avg_price, min_price, max_price)]
            self.sql.insert_data(data, self.db_log, "purchase_history")
        return data
