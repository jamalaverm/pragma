import pandas as pd


class CSVClient:

    @staticmethod
    def get_data(file_path: str):
        """
        Read the data from a csv file and return a pandas Dataframe.
        """
        df = pd.read_csv(file_path, dtype=str)
        return df

    @staticmethod
    def save_data(file_path: str, file):
        """
        Save a binary file to disk.
        """
        with open(file_path, "wb+") as file_object:
            file_object.write(file.file.read())
