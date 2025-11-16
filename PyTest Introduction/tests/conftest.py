import pytest
import pandas as pd

# Fixture to read the CSV file
@pytest.fixture(scope="session", params=["src\data\data.csv"])
def get_csv_data(request):
    path_to_file = request.param
    df = pd.read_csv(path_to_file)
    return df

# Fixture to validate the schema of the file  ## ["id", "name", "age", "email", "is_active"]
@pytest.fixture(scope="session", params=["id", "name", "age", "email"])
def validate_schema(request):
    actual_schema = get_csv_data(request).columns
    expected_schema = request.param
    return (actual_schema, expected_schema)

# Pytest hook to mark unmarked tests with a custom mark
def pytest_collection_modifyitems(config, items):
    marker_to_use = pytest.mark.unmarked
    for item in items:
        if not item.own_markers:
            item.add_marker(marker_to_use)