import pandas as pd
import re

def calculate_data_quality_metrics(df, field_name):
    metrics = {}
    
    # Only calculate metrics for specified fields
    if field_name in ['SLRN', 'Account Number', 'Meter Number', 'Meter SLRN']:
        # Completeness
        metrics['Completeness'] = df[field_name].count() / len(df) * 100

        # Validity
        if field_name == 'SLRN':
            # Validity check for SLRN (e.g., prefix 'ECGBD' and length 12)
            valid_slrn = df[field_name].apply(lambda x: str(x).startswith('ECGBD') and len(str(x)) == 12)
            metrics['Validity'] = valid_slrn.mean() * 100
        elif field_name == 'Meter SLRN':
            # Validity check for Meter SLRN (e.g., prefix 'ECGCR' and length >= 11)
            valid_meter_slrn = df[field_name].apply(lambda x: str(x).startswith('ECGCR') and len(str(x)) >= 11)
            metrics['Validity'] = valid_meter_slrn.mean() * 100
        elif field_name == 'Account Number':
            # Validity check for Account Number (numeric only)
            valid_account_number = df[field_name].apply(lambda x: str(x).isnumeric())
            metrics['Validity'] = valid_account_number.mean() * 100
        elif field_name == 'Meter Number':
            # Validity check for Meter Number (alphanumeric, length between 5 and 14, maximum of three letters)
            valid_meter_number = df[field_name].apply(
                lambda x: bool(re.match(r'^[0-9a-zA-Z]{5,14}$', str(x))) and sum(c.isalpha() for c in str(x)) <= 3
            )
            metrics['Validity'] = valid_meter_number.mean() * 100
        else:
            metrics['Validity'] = (df[field_name].notnull() & (df[field_name] != '')).mean() * 100
        
        # Integrity check
        if field_name == 'SLRN':
            # Check if SLRN has a corresponding Meter Number or Account Number
            integrity_check = ((df['Meter Number'].notnull() & (df['Meter Number'].str.len() > 5)) | df['Account Number'].notnull()).mean() * 100
        elif field_name == 'Meter Number':
            integrity_check = (
                ((df['Meter Number'].notnull()) & (df['Meter Number'].str.len() >= 5) & (df['Meter Status'] == 'Metered')) |
                ((df['Meter Number'].isnull()) & (df['Meter Status'] == 'Unmetered'))
            ).mean() * 100
        elif field_name == 'Meter SLRN':
            # Check if Meter SLRN has a corresponding SLRN and Meter Number
            integrity_check = ((df['SLRN'].notnull()) & (df['Meter Number'].notnull())).mean() * 100
        elif field_name == 'Account Number':
            # Check if Account Number has a corresponding SLRN or Meter Number (null values are allowed)
            integrity_check = ((df['SLRN'].notnull()) | (df['Meter Number'].notnull())).mean() * 100
        else:
            integrity_check = 100  # For other fields, assume integrity by default

        metrics['Integrity'] = integrity_check
    
        # Overall Score for the field
        weights = {'Completeness': 1, 'Validity': 1, 'Integrity': 1}
        overall_score = sum(metrics[metric] * weights[metric] for metric in metrics) / sum(weights.values())
        metrics['Overall Score'] = overall_score
    
    return metrics


# Function to calculate data quality metrics for Phone Number
def calculate_phone_number_metrics(df, field_name, corresponding_meter_field='Meter Number'):
    metrics = {}
    # Completeness
    metrics['Completeness'] = df[field_name].count() / len(df) * 100
    
    # Consistency in Format
    consistent_formats = df[field_name].apply(lambda x: re.match(r'^(\+?\d{9,12})?$', str(x)) is not None)
    consistent_formats_percentage = consistent_formats.mean() * 100

    # Valid Characters
    valid_characters = df[field_name].apply(lambda x: re.match(r'^[\d\+]+$', str(x)) is not None)
    valid_characters_percentage = valid_characters.mean() * 100

    # Length of Phone Numbers
    valid_lengths = df[field_name].apply(lambda x: len(str(x)) in {9, 10, 12})
    valid_lengths_percentage = valid_lengths.mean() * 100

    # Absence of Special Characters
    special_characters = df[field_name].apply(lambda x: re.match(r'^[\d\+\s]+$', str(x)) is not None)
    special_characters_percentage = special_characters.mean() * 100

    # Absence of Placeholder Values
    no_placeholders = df[field_name].apply(lambda x: not pd.isnull(x) and str(x).strip() != '')
    no_placeholders_percentage = no_placeholders.mean() * 100

    # Overall Integrity Score for the field
    overall_integrity = (
        consistent_formats_percentage + 
        valid_characters_percentage + 
        valid_lengths_percentage +
        special_characters_percentage + 
        no_placeholders_percentage
    ) / 5

    metrics['Validity'] = overall_integrity
    
    # Integrity check
    integrity_check = (
        (consistent_formats) &
        (valid_characters) &
        (valid_lengths) &
        (special_characters) &
        (no_placeholders) &
        # (df[field_name].notnull()) &
        (df[corresponding_meter_field].notnull())
    ).mean() * 100

    metrics['Integrity'] = integrity_check
        
    weights = {'Completeness': 1, 'Validity': 1, 'Integrity': 1}
    overall_score = sum(metrics[metric] * weights[metric] for metric in metrics) / sum(weights.values())
    metrics['Overall Score'] = overall_score

    return metrics

# Function to calculate data quality metrics for Email
def calculate_email_metrics(df, field_name, corresponding_meter_field='Meter Number'):
    metrics = {}
    # Completeness
    metrics['Completeness'] = df[field_name].count() / len(df) * 100
    
    # Consistency in Format
    consistent_formats = df[field_name].apply(lambda x: re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', str(x)) is not None)
    consistent_formats_percentage = consistent_formats.mean() * 100

    # Valid Characters
    valid_characters = df[field_name].apply(lambda x: re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', str(x)) is not None)
    valid_characters_percentage = valid_characters.mean() * 100

    # Absence of Placeholder Values
    no_placeholders = df[field_name].apply(lambda x: not pd.isnull(x) and str(x).strip() != '')
    no_placeholders_percentage = no_placeholders.mean() * 100

    # Overall Integrity Score for the field
    overall_integrity = (
        consistent_formats_percentage + 
        valid_characters_percentage + 
        no_placeholders_percentage
    ) / 3

    metrics['Validity'] = overall_integrity

    # Integrity check
    integrity_check = (
        (consistent_formats) &
        (valid_characters) &
        (no_placeholders) &
        (df[corresponding_meter_field].notnull())
    ).mean() * 100

    metrics['Integrity'] = integrity_check

    weights = {'Completeness': 1, 'Validity': 1, 'Integrity': 0.25}
    overall_score = sum(metrics[metric] * weights[metric] for metric in metrics) / sum(weights.values())
    metrics['Overall Score'] = overall_score

    return metrics
