import pandas as pd


class DataQualityLibrary:
    """
    A library of static methods for performing data quality checks on pandas DataFrames.

    This class is intended to be used in a PyTest-based testing framework to validate
    the quality of data in DataFrames. Each method performs a specific data quality
    check and uses assertions to ensure that the data meets the expected conditions.
    """

    @staticmethod
    def check_duplicates(df, column_names=None):
        if column_names:
            duplicated_mask = df.duplicated(column_names)
        else:
            duplicated_mask = df.duplicated(df.columns)
        return duplicated_mask.any()

    @staticmethod
    def check_count(df1, df2) -> bool: #Tuple[bool, int]:
        rows_df1 = len(df1)
        rows_df2 = len(df2)
        return rows_df1 == rows_df2 #, rows_df1 - rows_df2

    @staticmethod
    def check_data_completeness(
        source_df: pd.DataFrame,
        target_df: pd.DataFrame,
        key_cols = None
    ) -> bool:
        """
        Returns True only if the target dataframe:
        1. Contains all columns that exist in the source dataframe.
        2. Contains every key combination present in the source dataframe.

        Parameters
        ----------
        source_df : pd.DataFrame
            DataFrame that represents the expected universe of data (the “source”).
        target_df : pd.DataFrame
            DataFrame to validate (the “target”).
        key_cols : Iterable[str], optional
            Columns that uniquely identify a row.  If omitted, all columns from the
            source are used (which is only practical if every column participates
            in the row identity).

        Returns
        -------
        bool
            True if both column and row (key) completeness checks pass; otherwise False.
        """
        # --- Column completeness ---
        columns_ok = set(source_df.columns).issubset(target_df.columns)
        if not columns_ok:
            return False  # short-circuit if columns already fail

        # --- Row completeness ---
        if key_cols is None:
            key_cols = list(source_df.columns)

        # Drop duplicates so we’re only comparing unique key combinations
        source_keys = source_df[key_cols].drop_duplicates()
        target_keys = target_df[key_cols].drop_duplicates()

        # Convert to a set of tuples for fast subset checking
        source_key_set = set(map(tuple, source_keys.to_numpy()))
        target_key_set = set(map(tuple, target_keys.to_numpy()))

        rows_ok = source_key_set.issubset(target_key_set)

        return rows_ok  

        
    @staticmethod
    def check_dataset_is_not_empty(df) -> bool:
        """
        Return True if the DataFrame contains at least one row, False otherwise.
        """
        return not df.empty

    @staticmethod
    def check_not_null_values(df, column_names=None):
        if column_names is None:
            columns_to_check = df.columns
        else:
            if isinstance(column_names, str):
                column_names = [column_names]

            missing_columns = set(column_names) - set(df.columns)
            if missing_columns:
                raise KeyError(f"Columns not found in DataFrame: {missing_columns}")

            columns_to_check = list(column_names)

        null_counts = df[columns_to_check].isnull().sum()
        columns_with_nulls = null_counts[null_counts > 0]

        return columns_with_nulls.empty, columns_with_nulls
