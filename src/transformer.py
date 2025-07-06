import pandas as pd
import json
import re
from datetime import datetime
import logging
import ast


logger = logging.getLogger(__name__)

class DataTransformer:
    def __init__(self):
        pass

    def _parse_address(self, address_str: str) -> dict:
        """
        Parses a JSON string representation of an address into a dictionary.
        Handles common JSON parsing errors like single quotes and extraneous spaces.
        """
        if not isinstance(address_str, str):
            logger.warning(f"Address input was not a string: {address_str}")
            return {}
        try:
            # Clean the string first
            cleaned_string = address_str.strip()
            
            # Fix common issues
            # 1. Fix unquoted post codes with hyphens
            cleaned_string = re.sub(r'(\d+-\d+)', r"'\1'", cleaned_string)
            
            # 2. Ensure proper closing braces
            if cleaned_string.count('{') != cleaned_string.count('}'):
                # Count opening braces
                open_braces = cleaned_string.count('{')
                close_braces = cleaned_string.count('}')
                
                # Add missing closing braces
                if open_braces > close_braces:
                    missing_braces = open_braces - close_braces
                    cleaned_string += '}' * missing_braces
                    #print(f"Added {missing_braces} missing closing brace(s)")

            try:
                # Parse the string
                address_str = re.sub(r'post code\': (\d+-\d+)', r"post code': '\1'", cleaned_string)

                data = ast.literal_eval(address_str)
                
                # Extract address information
                if 'address' in data:
                    address = data['address']
                    return {
                        'street': address.get('streeet', ''),  # Handle typo
                        'city': address.get('city', ''),
                        'post_code': str(address.get('post code', '')),  # Convert to string
                        'country': address.get('country', '')
                    }
            except Exception as e:
                print(f"Error parsing string: {e}")
                return None
            except json.JSONDecodeError as e:
                logger.warning(f"Could not parse address string '{address_str}' from row. Error: {e}")
                return {}
        except Exception as e:
            print(f"Error parsing string: {e}")
            print(f"Problematic string: {address_str}")
            return None

    def _parse_transactions(self, transactions_str: str) -> list:
        """
        Parses a JSON string representation of transactions into a list of dictionaries.
        Handles common JSON parsing errors, 'None'/'Null' values, and extraneous spaces.
        """
        if not isinstance(transactions_str, str):
            logger.warning(f"Transactions input was not a string: {transactions_str}")
            return []
        try:
            # Replace single quotes with double quotes
            cleaned_str = transactions_str.replace("'", '"')
            # Replace string 'None' or 'Null' with JSON null
            cleaned_str = re.sub(r':\s*("None"|"Null"|None|Null)\b', ': null', cleaned_str)
            # Remove any trailing non-JSON characters (e.g., spaces)
            cleaned_str = cleaned_str.strip()
            return json.loads(cleaned_str)
        except json.JSONDecodeError as e:
            logger.warning(f"Could not parse transactions string '{transactions_str}' from row. Error: {e}")
            return []

    def _parse_currency_amount(self, amount_str: str) -> float:
        """
        Parses currency amounts in various formats (€123,45 or $123.45).
        Returns the numeric value as a float.
        """
        if not isinstance(amount_str, str):
            return 0.0
        
        # Remove currency symbols
        cleaned_amount = re.sub(r'[€$£¥]', '', amount_str)
        
        # Handle different decimal separators
        if ',' in cleaned_amount and '.' in cleaned_amount:
            # If both comma and period exist, assume European format (1.234,56)
            # Remove thousands separators (periods) and convert comma to decimal point
            cleaned_amount = cleaned_amount.replace('.', '').replace(',', '.')
        elif ',' in cleaned_amount:
            # Check if comma is likely a decimal separator or thousands separator
            comma_parts = cleaned_amount.split(',')
            if len(comma_parts) == 2 and len(comma_parts[1]) <= 2:
                # Likely decimal separator (123,45)
                cleaned_amount = cleaned_amount.replace(',', '.')
            else:
                # Likely thousands separator (1,234)
                cleaned_amount = cleaned_amount.replace(',', '')
        
        # Remove any remaining non-numeric characters except decimal point
        cleaned_amount = re.sub(r'[^0-9.]', '', cleaned_amount)
        
        try:
            return float(cleaned_amount)
        except ValueError:
            logger.warning(f"Could not parse currency amount: '{amount_str}'")
            return 0.0

    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Applying transformations...")

        # Start with an empty DataFrame and explicitly add transformed columns
        transformed_df = pd.DataFrame()

        # Core customer details
        transformed_df['names'] = df['names']
        transformed_df['email'] = df['mail'] # 'mail' is the cleaned column name for email

        # 1. Address Transformation and Flattening
        logger.info("Transforming and flattening address data...")
        # Parse the 'address' JSON string into dictionaries
        address_components = df['address'].apply(self._parse_address)
        
        # Extract specific fields from the parsed address dictionaries
        # Use .get() to safely access keys, returning None if key is missing
        transformed_df['address_street'] = address_components.apply(lambda x: x.get('street')) # Note 'streeet' (typo in source)
        transformed_df['address_city'] = address_components.apply(lambda x: x.get('city'))
        transformed_df['address_post_code'] = address_components.apply(lambda x: x.get('post_code')) # Note 'post code' (space in source)
        transformed_df['address_country'] = address_components.apply(lambda x: x.get('country'))
        
        # The original top-level 'country' column is now effectively ignored in favor of 'address_country'

        # 2. Transactions Transformation
        logger.info("Transforming transactions data...")
        parsed_transactions = df['transactions'].apply(self._parse_transactions)
        transformed_df['num_transactions'] = parsed_transactions.apply(len)

        def calculate_total_amount(tx_list: list) -> float:
            total = 0.0
            for tx in tx_list:
                amount_str = str(tx.get('amount', '€0,00'))
                total += self._parse_currency_amount(amount_str)
            return total

        transformed_df['total_transaction_amount'] = parsed_transactions.apply(calculate_total_amount)

        # 3. Date Transformations
        logger.info("Transforming date columns...")
        # Prioritize 'account_created_at' (from 'account Created at')
        if 'account_created_at' in df.columns:
            transformed_df['account_created_at'] = pd.to_datetime(df['account_created_at'], errors='coerce', format='%Y-%m-%d %H:%M:%S.%f')
        elif 'account_created_at_1' in df.columns:
             transformed_df['account_created_at'] = pd.to_datetime(df['account_created_at_1'], errors='coerce', format='%Y-%m-%d %H:%M:%S.%f')
        else:
            transformed_df['account_created_at'] = pd.NaT 

        # Type conversion for numerical columns
        if 'num_transactions' in transformed_df.columns:
            transformed_df['num_transactions'] = transformed_df['num_transactions'].astype(int, errors='ignore')
        
        if 'total_transaction_amount' in transformed_df.columns:
            transformed_df['total_transaction_amount'] = transformed_df['total_transaction_amount'].astype(float, errors='ignore')

        # Define the final columns and their desired order in the output table
        final_columns_order = [
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
        
        # Select and reorder columns, dropping any columns not in final_columns_order.
        transformed_df = transformed_df[final_columns_order]

        logger.info("Transformations completed.")
        return transformed_df