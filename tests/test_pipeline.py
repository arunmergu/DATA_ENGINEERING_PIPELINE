import pytest
import pandas as pd
import sqlite3
from sqlalchemy import create_engine, text
from datetime import datetime # Import datetime for proper comparison

# Assuming your modules are correctly importable from src
from src.data_reader import CSVReader
from src.transformer import DataTransformer
from src.data_writer import DataWriter

# --- Fixtures ---

@pytest.fixture
def mock_csv_data_full():
    """
    Provides comprehensive sample CSV data as a string,
    mimicking the full raw input for CSVReader.
    Includes columns expected by the transformer.
    """
    return """customer_id, Names, Mail, Address, Transactions, Account Created At, product_category
1,John Doe,john.doe@example.com,"{'address': {'streeet': '123 Main St', 'city': 'Anytown', 'post code': '12345', 'country': 'USA'}}","[{'id': 'T1', 'amount': '€100,50'}, {'id': 'T2', 'amount': '$50.25'}]",2023-01-01 10:00:00.123456,Electronics
2,Jane Smith,jane.smith@example.com,"{'address': {'streeet': '456 Oak Ave', 'city': 'Otherville', 'post code': '67890', 'country': 'Canada'}}","[{'id': 'T3', 'amount': '£200.00'}]",2023-02-15 11:30:00.654321,Books
3,Peter Jones,peter.jones@example.com,"{'address': {'streeet': '789 Pine Ln', 'city': 'Somewhere', 'post code': '11223', 'country': 'UK'}}","[{'id': 'T4', 'amount': '¥5000'}, {'id': 'T5', 'amount': '$12.75'}]",2023-03-20 14:45:00.987654,Clothing
"""

@pytest.fixture
def mock_csv_path_full(tmp_path, mock_csv_data_full):
    """Creates a temporary full mock CSV file for testing."""
    file_path = tmp_path / "test_mock_dataset_full.csv"
    file_path.write_text(mock_csv_data_full)
    return str(file_path)

@pytest.fixture
def temp_sqlite_db_path(tmp_path):
    """Creates a temporary SQLite database path for testing."""
    db_path = tmp_path / "test_bynd_pipeline.db"
    return f"sqlite:///{db_path}"

# --- Tests for CSVReader ---

def test_csv_reader_read_data_and_clean_columns(mock_csv_path_full):
    """
    Tests if CSVReader correctly reads data and automatically cleans column names to snake_case.
    The clean_column_names method is now internal to read_data.
    """
    reader = CSVReader(file_path=mock_csv_path_full)
    df = reader.read_data()

    assert not df.empty
    # Assert that all expected columns exist and are in snake_case
    expected_cleaned_columns = [
        'customer_id', 'names', 'mail', 'address', 'transactions',
        'account_created_at', 'product_category'
    ]
    assert all(col in df.columns for col in expected_cleaned_columns)
    assert len(df.columns) == len(expected_cleaned_columns) # No extra columns
    assert len(df) == 3 # Number of rows

    # Verify a specific column was cleaned (e.g., 'Account Created At' became 'account_created_at')
    assert 'Account Created At' not in df.columns
    assert 'account_created_at' in df.columns


# --- Tests for DataTransformer ---

def test_data_transformer_transform_data(mock_csv_path_full):
    """
    Tests if DataTransformer correctly transforms data based on the full schema,
    including address, transactions, and date parsing.
    """
    # First, simulate the output of CSVReader (which now cleans columns)
    reader = CSVReader(file_path=mock_csv_path_full)
    cleaned_df = reader.read_data()

    transformer = DataTransformer()
    transformed_df = transformer.transform_data(cleaned_df)

    # Check for expected final columns and their order
    expected_final_columns = [
        'names',
        'email',
        'address_street',
        'address_city',
        'address_post_code',
        'address_country',
        'account_created_at',
        'num_transactions',
        'total_transaction_amount'
    ]
    assert list(transformed_df.columns) == expected_final_columns

    # Check data types and specific value transformations for first row
    first_row = transformed_df.iloc[0]

    assert first_row['names'] == 'John Doe'
    assert first_row['email'] == 'john.doe@example.com'
    assert first_row['address_street'] == '123 Main St'
    assert first_row['address_city'] == 'Anytown'
    assert first_row['address_post_code'] == '12345'
    assert first_row['address_country'] == 'USA'
    assert pd.api.types.is_datetime64_any_dtype(transformed_df['account_created_at'])
    assert first_row['account_created_at'] == datetime(2023, 1, 1, 10, 0, 0, 123456)
    assert first_row['num_transactions'] == 2
    assert pytest.approx(first_row['total_transaction_amount']) == 150.75 # 100.50 + 50.25

    # Check second row for different currency and single transaction
    second_row = transformed_df.iloc[1]
    assert second_row['names'] == 'Jane Smith'
    assert second_row['email'] == 'jane.smith@example.com'
    assert second_row['num_transactions'] == 1
    assert pytest.approx(second_row['total_transaction_amount']) == 200.00 # £200.00

    # Check third row for multiple currencies and date
    third_row = transformed_df.iloc[2]
    assert third_row['names'] == 'Peter Jones'
    assert third_row['email'] == 'peter.jones@example.com'
    assert third_row['num_transactions'] == 2
    assert pytest.approx(third_row['total_transaction_amount']) == 5012.75 # 5000 + 12.75
    assert third_row['account_created_at'] == datetime(2023, 3, 20, 14, 45, 0, 987654)


# --- Tests for DataWriter ---

def test_data_writer_write_data(temp_sqlite_db_path):
    """Tests if DataWriter correctly writes data to a SQLite database."""
    # Create a sample DataFrame to write, mimicking transformed output
    data = {
        'names': ['Test Customer'],
        'email': ['test@example.com'],
        'address_street': ['1 Test Lane'],
        'address_city': ['Testville'],
        'address_post_code': ['TS1 2ST'],
        'address_country': ['Testingland'],
        'account_created_at': [pd.to_datetime('2024-05-10 10:30:00')],
        'num_transactions': [5],
        'total_transaction_amount': [1234.56]
    }
    df_to_write = pd.DataFrame(data)

    table_name = "customers_and_transactions" # Use actual table name from pipeline

    writer = DataWriter(db_connection_string=temp_sqlite_db_path, table_name=table_name)
    writer.write_data(df_to_write, if_exists='replace')

    # Verify data was written by reading it back directly from SQLite
    engine = create_engine(temp_sqlite_db_path)
    with engine.connect() as conn:
        # Check if table exists
        result = conn.execute(text(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")).fetchone()
        assert result is not None, f"Table '{table_name}' was not created."

        # Read data back into a DataFrame
        read_df = pd.read_sql_table(table_name, con=conn)
        
        # Ensure 'account_created_at' is parsed as datetime on read
        read_df['account_created_at'] = pd.to_datetime(read_df['account_created_at'])

        # Basic assertions
        assert not read_df.empty
        assert len(read_df) == len(df_to_write)
        
        # Use pandas.testing.assert_frame_equal for robust DataFrame comparison
        # This will compare values and dtypes (mostly)
        pd.testing.assert_frame_equal(df_to_write, read_df)