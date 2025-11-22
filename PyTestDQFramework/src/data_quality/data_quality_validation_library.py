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
        """
        Check if DataFrame contains duplicate rows.
        
        Parameters:
            df: DataFrame to check
            column_names: Specific columns to check. If None, checks all columns.
        
        Returns:
            True if duplicates exist, False otherwise.
        """
        if column_names:
            duplicated_mask = df.duplicated(column_names)
        else:
            duplicated_mask = df.duplicated()
        return duplicated_mask.any()

    @staticmethod
    def check_count(df1, df2):
        rows_df1 = len(df1)
        rows_df2 = len(df2)
        return (rows_df1 == rows_df2, rows_df1, rows_df2)

    @staticmethod
    def check_data_completeness(
        source_df: pd.DataFrame,
        target_df: pd.DataFrame
    ) -> bool:
        """
        Returns True only if the target dataframe contains all columns that exist in the source dataframe.

        Parameters
        ----------
        source_df : pd.DataFrame
            DataFrame that represents the expected universe of data (the “source”).
        target_df : pd.DataFrame
            DataFrame to validate (the “target”).

        Returns
        -------
        bool
            True if both column completeness checks pass; otherwise False.
        """
        # --- Column completeness ---
        columns_ok = set(source_df.columns).issubset(target_df.columns)
        if not columns_ok:
            return False 
        
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
