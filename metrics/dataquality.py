import pandas as pd
import re

def calculate_data_quality_metrics(df, field_name, slrn_prefix, slrn_length, meter_prefix=None, meter_length=None):
    metrics = {'Completeness': 0, 'Validity': 0, 'Integrity': 0}

    # Only calculate metrics for specified fields
    if field_name in ['SLRN', 'Account Number', 'Meter Number', 'Meter SLRN']:
        # Completeness
        metrics['Completeness'] = df[field_name].count() / len(df) * 100

        # Validity
        if field_name == 'SLRN':
            valid_slrn = df[field_name].apply(lambda x: str(x).startswith(slrn_prefix) and len(str(x)) == slrn_length)
            metrics['Validity'] = valid_slrn.mean() * 100
        elif field_name == 'Meter SLRN':
            valid_meter_slrn = df[field_name].apply(lambda x: str(x).startswith(meter_prefix) and len(str(x)) >= meter_length)
            metrics['Validity'] = valid_meter_slrn.mean() * 100
        elif field_name == 'Account Number':
            valid_account_number = df[field_name].apply(lambda x: str(x).isnumeric())
            metrics['Validity'] = valid_account_number.mean() * 100
        elif field_name == 'Meter Number' and slrn_prefix == 'AEDCBD':
            valid_meter_number = df[field_name].apply(
                lambda x: str(x).isnumeric() and 5 <= len(str(x)) <= 13
            )
            metrics['Validity'] = valid_meter_number.mean() * 100
        elif field_name == 'Meter Number' and slrn_prefix == 'ECGBD':
            valid_meter_number = df[field_name].apply(
                lambda x: bool(re.match(r'^[0-9a-zA-Z]{5,14}$', str(x))) and sum(c.isalpha() for c in str(x)) <= 3
            )
            metrics['Validity'] = valid_meter_number.mean() * 100
        else:
            metrics['Validity'] = (df[field_name].notnull() & (df[field_name] != '')).mean() * 100
        
        # Integrity check
        if field_name == 'SLRN':
            integrity_check = ((df['Meter Number'].notnull() & (df['Meter Number'].str.len() > 5)) | df['Account Number'].notnull()).mean() * 100
        elif field_name == 'Meter Number':
            integrity_check = (
                ((df['Meter Number'].notnull()) & (df['Meter Number'].str.len() >= 5) & (df['Meter Status'] == 'Metered')) |
                ((df['Meter Number'].isnull()) & (df['Meter Status'] == 'Unmetered'))
            ).mean() * 100
        elif field_name == 'Meter SLRN':
            integrity_check = ((df['SLRN'].notnull()) & (df['Meter Number'].notnull())).mean() * 100
        elif field_name == 'Account Number':
            integrity_check = ((df['Account Number'].str.len() > 3) & (df['SLRN'].notnull()) & (df['Meter Number'].notnull())).mean() * 100
        # else:
        #     integrity_check = 100  # For other fields, assume integrity by default

        metrics['Integrity'] = integrity_check
    
    return metrics

# Updated calculate_phone_number_metrics function
def calculate_phone_number_metrics(df, field_name, slrn_prefix, corresponding_meter_field='Meter Number'):
	metrics = {'Completeness': 0, 'Validity': 0, 'Integrity': 0}

	metrics['Completeness'] = df[field_name].count() / len(df) * 100

	if slrn_prefix == 'ECGBD':
		consistent_formats = df[field_name].apply(lambda x: re.match(r'^(\+?\d{9,12})?$', str(x)) is not None)
		consistent_formats_percentage = consistent_formats.mean() * 100
	elif slrn_prefix == 'AEDCBD':
		consistent_formats = df[field_name].apply(lambda x: re.match(r'^(\+?\d{11,13})?$', str(x)) is not None)
		consistent_formats_percentage = consistent_formats.mean() * 100

	valid_characters = df[field_name].apply(lambda x: re.match(r'^[\d\+]+$', str(x)) is not None)
	valid_characters_percentage = valid_characters.mean() * 100

	if slrn_prefix == 'ECGBD':
		valid_lengths = df[field_name].apply(lambda x: len(str(x)) in {9, 10, 12})
		valid_lengths_percentage = valid_lengths.mean() * 100
	elif slrn_prefix == 'AEDCBD':
		valid_lengths = df[field_name].apply(lambda x: len(str(x)) in {10, 11, 13})
		valid_lengths_percentage = valid_lengths.mean() * 100

	special_characters = df[field_name].apply(lambda x: re.match(r'^[\d\+\s]+$', str(x)) is not None)
	special_characters_percentage = special_characters.mean() * 100

	no_placeholders = df[field_name].apply(lambda x: not pd.isnull(x) and str(x).strip() != '')
	no_placeholders_percentage = no_placeholders.mean() * 100

	overall_integrity = (
		consistent_formats_percentage + 
		valid_characters_percentage + 
		valid_lengths_percentage +
		special_characters_percentage + 
		no_placeholders_percentage
	) / 5

	metrics['Validity'] = overall_integrity

	integrity_check = (
		(consistent_formats) &
		(valid_characters) &
		(valid_lengths) &
		(special_characters) &
		(no_placeholders) &
		(df[corresponding_meter_field].notnull())
	).mean() * 100

	metrics['Integrity'] = integrity_check

	return metrics

# Updated calculate_email_metrics function
def calculate_email_metrics(df, field_name, corresponding_meter_field='Meter Number'):
    metrics = {'Completeness': 0, 'Validity': 0, 'Integrity': 0}

    metrics['Completeness'] = df[field_name].count() / len(df) * 100
    
    consistent_formats = df[field_name].apply(lambda x: re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', str(x)) is not None)
    consistent_formats_percentage = consistent_formats.mean() * 100

    valid_characters = df[field_name].apply(lambda x: re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', str(x)) is not None)
    valid_characters_percentage = valid_characters.mean() * 100

    no_placeholders = df[field_name].apply(lambda x: not pd.isnull(x) and str(x).strip() != '')
    no_placeholders_percentage = no_placeholders.mean() * 100

    overall_integrity = (
        consistent_formats_percentage + 
        valid_characters_percentage + 
        no_placeholders_percentage
    ) / 3

    metrics['Validity'] = overall_integrity

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

def calculate_average_metrics(metrics_list, metric_name):
    # Calculate the average of a specific metric across all key fields
    total_metric = sum(metrics[metric_name] for metrics in metrics_list)
    average_metric = total_metric / len(metrics_list)
    return average_metric