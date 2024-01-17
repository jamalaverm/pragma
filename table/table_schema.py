class TableSchema:
    schema = {
        'purchase': [
            'timestamp', 'price', 'user_id'],
        'purchase_history': [
            'count', 'avg_price', 'min_price', 'max_price'],
    }

    @classmethod
    def get_str_cols(cls, table_name: str):
        """
        The columns are separated by commas and enclosed in parentheses, it's useful when constructing SQL
        queries.
        """
        return "(" + ", ".join(cls.schema[table_name]) + ")"

    @classmethod
    def get_str_vals(cls, table_name: str):
        """
        This is a class method that takes table_name as a parameter. The purpose of this method is to generate a
        string of placeholders for the values of a given table.

        The placeholders are '%s', which is a common format for placeholders in SQL queries. The number of placeholders
        corresponds to the number of columns in the table. This string can be used in an SQL query to insert values into
        the table.
        """
        return "(" + ", ".join(["%s"] * len(cls.schema[table_name])) + ")"
