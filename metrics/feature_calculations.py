import pandas as pd
import re

from metrics.overallscore import calculate_overall_score
from metrics.dataquality import calculate_data_quality_metrics
from metrics.dataquality import calculate_phone_number_metrics
from metrics.dataquality import calculate_email_metrics
from metrics.dataquality import calculate_average_metrics

def calculate_unique_meter_count(df, date_column, meter_number_column):
    unique_meter_count = df.groupby(date_column)[meter_number_column].nunique().reset_index()
    unique_meter_count.columns = [date_column, 'Unique Meter Count']
    return unique_meter_count

def calculate_metrics_by_month(df, key_fields, bd_slrn, bdslrn_len, meter_slrn, mslrn_len):
    result_data = []

    for year_month in df['Year Month'].unique():
        # Filter the DataFrame for the current year_month
        df_month = df[df['Year Month'] == year_month]
        
        # Calculate metrics for the current month
        metrics_list = []

        for field_name in key_fields:
            if field_name in df.columns:
                if field_name in ['SLRN', 'Account Number', 'Meter Number', 'Meter SLRN']:
                    metrics = calculate_data_quality_metrics(df_month, field_name, bd_slrn, bdslrn_len, meter_slrn, mslrn_len)
                elif field_name == 'Phone Number':
                    metrics = calculate_phone_number_metrics(df_month, field_name, bd_slrn, corresponding_meter_field='Meter Number')
                elif field_name == 'Email':
                    metrics = calculate_email_metrics(df_month, field_name, corresponding_meter_field='Meter Number')

                metrics_list.append(metrics)

        average_completeness = calculate_average_metrics(metrics_list, 'Completeness')
        average_validity = calculate_average_metrics(metrics_list, 'Validity')
        average_integrity = calculate_average_metrics(metrics_list, 'Integrity')
        overall_score = calculate_overall_score(average_completeness, average_validity, average_integrity)

        unique_meter_count = calculate_unique_meter_count(df_month, 'Year Month', 'Meter Number')['Unique Meter Count'].iloc[0]

        # Construct metrics data for each field
        for field_name in key_fields:
            metrics_data = {
                'Year Month': year_month,
                'Key fields': field_name,
                'Completeness': metrics_list[key_fields.index(field_name)]['Completeness'],
                'Validity': metrics_list[key_fields.index(field_name)]['Validity'],
                'Integrity': metrics_list[key_fields.index(field_name)]['Integrity'],
                'Average Completeness': average_completeness,
                'Average Validity': average_validity,
                'Average Integrity': average_integrity,
                'Overall Score': overall_score,
                'Unique Meter Count': unique_meter_count
            }
            result_data.append(metrics_data)

    # Convert the list of dictionaries to a DataFrame
    result_df = pd.DataFrame(result_data)
    result_df = result_df.sort_values(by='Year Month', ascending=False)
    
    return result_df

def preprocess_meter_number(meter_number):
    """
    Preprocess a meter number to remove scientific notation.
    """
    # Convert meter number to string
    meter_number = str(meter_number)
    
    # Remove scientific notation if present
    if 'e' in meter_number.lower():
        try:
            meter_number = '{:.0f}'.format(float(meter_number))
        except ValueError:
            # If conversion to float fails, return original value
            return meter_number
    
    return meter_number


def is_valid_meter_number(meter_number):
    """
    Check if a meter number is valid.
    """
    # Define conditions for meter number validity
    valid_format = bool(re.match(r'^[0-9a-zA-Z]{5,14}$', meter_number))
    has_alpha_chars = sum(c.isalpha() for c in meter_number) <= 3
    
    # Check if all conditions are met
    return valid_format and has_alpha_chars

def preprocess_phone_number(phone_number):
    """
    Preprocess a phone number to remove non-numeric characters.
    """
    # Convert phone number to string
    phone_number = str(phone_number)
    
    # Remove non-numeric characters
    phone_number = re.sub(r'\D', '', phone_number)
    
    return phone_number


def is_valid_phone_number(phone_number):
    """
    Check if a phone number is valid.
    Valid phone numbers must start with '233' or '+233' and have a total length of 12.
    """
    # Define regular expression pattern for valid phone numbers
    pattern = r'^(\+?233)?0*\d{6,9}$'
    
    # Check if phone number matches the pattern
    return bool(re.match(pattern, phone_number))



def pn_has_integrity(phone_number, meter_number):
    """
    Check if a phone number has integrity.
    """
    pattern = r'^(\+?233)?0*\d{6,9}$'
    
    # Check if phone number matches the pattern
    
    has_meter_number = not pd.isnull(meter_number)
    
    # Check if all conditions are met
    return bool(re.match(pattern, phone_number)) and has_meter_number