import pytest
import re
import os
import csv
import pandas as pd

file_path = "src\data\data.csv"

def test_file_not_empty(get_csv_data: pd.DataFrame):
    df = get_csv_data
    assert len(df) != 0, "Data file is empty"

@pytest.mark.validate_csv
@pytest.mark.xfail(reason="Intentional duplication in data")
def test_duplicates(get_csv_data):
    df = get_csv_data
    duplicate_rows = df[df.duplicated()]
    assert len(duplicate_rows) == 0, "Data file contains duplicate records"


@pytest.mark.validate_csv
def test_validate_schema(get_csv_data):
    df = get_csv_data
    required_schema_fields = ["id", "name", "age", "email"]
    assert set(df.columns).issuperset(set(required_schema_fields)), "Schema of the data file does not contain required fields"

@pytest.mark.validate_csv
@pytest.mark.skip(reason="Dataset may not include out-of-range ages; enable when validating data")
def test_age_column_valid(get_csv_data):
    df = get_csv_data
    filtered_df = df.query('age <= 0 or age >= 100')
    assert len(filtered_df) == 0, "Data file's age column contains invalid values, outside allowed range (0-100)"

@pytest.mark.validate_csv
def test_email_column_valid(get_csv_data):
    EMAIL_REGEX = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    EMAIL_COLUMN = 'email'
    df = get_csv_data
    invalid_email_df = df[~df[EMAIL_COLUMN].str.match(EMAIL_REGEX, na=False)]
    assert len(invalid_email_df) == 0, "Data file's email column contains invalid values, wrong email format"


@pytest.mark.parametrize("id, is_active", [(1, False), (2, True)])
def test_active_players(get_csv_data, id, is_active):
    df = get_csv_data
    assert df.loc[df['id']==id, 'is_active'].item() == is_active, "is_active status does not match expected value"


def test_active_player(get_csv_data):
    df = get_csv_data
    assert df.loc[df['id']==2, 'is_active'].item(), "is_active status does not match expected value"
