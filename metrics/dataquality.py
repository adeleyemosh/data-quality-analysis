import pandas as pd
import re

def calculate_data_quality_metrics(df, field_name, slrn_prefix, slrn_length, meter_prefix=None, meter_length=None):
    metrics = {'Completeness': 0, 'Validity': 0, 'Integrity': 0}

    # Only calculate metrics for specified fields
    if field_name in ['SLRN', 'Account Number', 'Meter Number', 'Meter SLRN', 'Phone Number', 'Email']:
        # Completeness
        completeness = df[field_name].count() / len(df) * 100
        metrics['Completeness'] = completeness

        # Validity
        validity = calculate_validity(df, field_name, slrn_prefix, slrn_length, meter_prefix, meter_length)
        metrics['Validity'] = (validity * completeness) / 100

        # Integrity check
        integrity = calculate_integrity(df, field_name, slrn_prefix, corresponding_meter_field='Meter Number')
        metrics['Integrity'] = (integrity * completeness) / 100
    
    return metrics

# Helper functions
def calculate_validity(df, field_name, slrn_prefix='', slrn_length=0, meter_prefix='', meter_length=0, corresponding_meter_field=''):
    complete_records = df[df[field_name].notnull()].copy()
    
    if field_name == 'SLRN':
        return (complete_records[field_name].apply(lambda x: str(x).startswith(slrn_prefix) and len(str(x)) == slrn_length)).mean() * 100
    elif field_name == 'Meter SLRN':
        return (complete_records[field_name].apply(lambda x: str(x).startswith(meter_prefix) and len(str(x)) >= meter_length)).mean() * 100
    elif field_name == 'Account Number':
        if slrn_prefix in ['YEDCBD', 'AEDCBD']:
            # Convert float values to integers before converting to strings and applying isnumeric check
            # return (df['Meter Status'] == 'Unmetered').astype(int).mean() * 100
            return (complete_records[field_name].astype(str).str.len() >= 6 | complete_records[field_name].notnull()).mean() * 100
        else:
            return (complete_records[field_name].astype(str).str.len() >= 5).mean() * 100 
            #return (df[field_name].astype(str).apply(lambda x: x.isnumeric())).mean() * 100
    elif field_name == 'Meter Number':
        # Preprocess meter numbers
        complete_records.loc[:, 'Processed Meter Number'] = complete_records[field_name].apply(preprocess_meter_number)
        
        # Apply the validity check function to the preprocessed meter numbers
        complete_records.loc[:, 'Meter Number Validity'] = complete_records['Processed Meter Number'].apply(is_valid_meter_number)
        
        return complete_records['Meter Number Validity'].mean() * 100
    elif field_name == 'Phone Number':        
        # Preprocess phone numbers
        complete_records.loc[:, 'Processed Phone Number'] = complete_records[field_name].apply(preprocess_phone_number)
        
        # Apply the validity check function to the preprocessed phone numbers
        complete_records.loc[:, 'Phone Number Validity'] = complete_records['Processed Phone Number'].apply(is_valid_phone_number)
        
        return complete_records['Phone Number Validity'].mean() * 100
    elif field_name == 'Email':
        # Apply the validity check function to the email field
        complete_records.loc[:, 'Email Validity'] = complete_records[field_name].apply(is_valid_email)
        
        return complete_records['Email Validity'].mean() * 100
    else:
        return None

def calculate_integrity(df, field_name, slrn_prefix='', corresponding_meter_field=''):
    complete_records = df[df[field_name].notnull()].copy()
    
    if field_name == 'SLRN':
        return ((complete_records[field_name].notnull()) & (complete_records[corresponding_meter_field].notnull() & (complete_records[corresponding_meter_field].str.len() > 5)) | complete_records['Account Number'].notnull()).mean() * 100
    elif field_name == 'Meter SLRN':
        return ((complete_records['Meter SLRN'].str.len() > 10) & (complete_records['SLRN'].notnull()) & (complete_records['Meter Number'].notnull())).mean() * 100
    elif field_name == 'Meter Number':
        # Preprocess meter numbers
        complete_records['Processed Meter Number'] = complete_records[field_name].apply(preprocess_meter_number)
        
        # Apply the validity check function to the preprocessed meter numbers
        complete_records['Meter Number Validity'] = complete_records['Processed Meter Number'].apply(is_valid_meter_number)
        
        has_integrity = (
            (complete_records['Processed Meter Number'].notnull()) &
            (complete_records['Processed Meter Number'].str.len() >= 5) &
            (complete_records['Meter Status'] == 'Metered') &
            (complete_records['SLRN'].notnull()) &
            (complete_records['Meter Number Validity'])
        )
        
        return has_integrity.mean() * 100
    elif field_name == 'Email':
        # Apply the validity check function to the email field
        complete_records.loc[:, 'Email Validity'] = complete_records[field_name].apply(is_valid_email)
        
        valid_email = complete_records['Email Validity']
        
        combined_conditions = valid_email & (complete_records['Meter Number'].notnull() | complete_records['Account Number'].notnull())
        
        return combined_conditions.mean() * 100
    elif field_name == 'Phone Number':        
        # Preprocess phone numbers
        complete_records.loc[:, 'Processed Phone Number'] = complete_records[field_name].apply(preprocess_phone_number)
        
        # Apply the validity check function to the preprocessed phone numbers
        complete_records.loc[:, 'Phone Number Validity'] = complete_records['Processed Phone Number'].apply(is_valid_phone_number)
        
        combined_conditions = complete_records['Phone Number Validity'] & (complete_records['Meter Number'].notnull() | complete_records['Account Number'].notnull())
        
        return combined_conditions.mean() * 100
    elif field_name == 'Account Number':
        if slrn_prefix in ['YEDCBD', 'AEDCBD']:
            return ((complete_records[field_name].astype(str).str.len() >= 6 | complete_records[field_name].notnull()) & (complete_records['SLRN'].notnull() | complete_records['Meter Status'].notnull())).mean() * 100
        else:
            return ((complete_records[field_name].astype(str).str.len() > 5) &  (complete_records['SLRN'].notnull()) & (complete_records['Meter Number'].notnull())).mean() * 100
    else:
        return None

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
    Valid phone numbers must start with '233' (Ghana) or '234' (Nigeria) or their international format '+233' or '+234'
    and have a total length of 12 for Ghana and 13 for Nigeria. Numbers without the country code should have a character
    length between 9 and 10 for Ghana or 10 and 11 for Nigeria to be considered valid.
    """
    # Define regular expression pattern for valid phone numbers
    pattern = r'^(\+?233|\+?234)?0*\d{9,12}$'
    without_country_code_pattern = r'^0\d{8,9}$'  # 9-10 digits for Ghana, 10-11 digits for Nigeria
    
    # Check if phone number matches any of the patterns
    if re.match(pattern, phone_number) or re.match(without_country_code_pattern, phone_number):
        return True
    else:
        return False


def pn_has_integrity(phone_number, meter_number, account_number):
    """
    Check if a phone number has integrity.
    Valid phone numbers must start with '233' or '+233' and have a total length of 12.
    Numbers without the country code should have a character length between 9 and 10 for Ghana or 10 and 11 for Nigeria.
    """
    pattern = r'^(\+?233|\+?234)?0*\d{9,12}$'
    without_country_code_pattern = r'^0\d{8,9}$'  # 9-10 digits for Ghana, 10-11 digits for Nigeria
    
    # Check if phone number matches any of the patterns
    if re.match(pattern, phone_number) or re.match(without_country_code_pattern, phone_number):
        has_meter_number = not (pd.isnull(meter_number) or pd.isnull(account_number))
        return has_meter_number
    else:
        return False

def is_valid_email(email):
    """
    Check if an email is valid.
    """
    # Conditions for email validity
    valid_format = re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', str(email)) is not None
    has_valid_characters = re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', str(email)) is not None
    has_no_placeholders = not pd.isnull(email) and str(email).strip() != ''
    no_noemail = not re.search(r'(noemail|nomail|nil|noamail|nomai|example)', str(email), re.IGNORECASE)
    
    return valid_format and has_valid_characters and has_no_placeholders and no_noemail


def calculate_average_metrics(metrics_list, metric_name):
    # Calculate the average of a specific metric across all key fields
    valid_metrics = [metrics[metric_name] for metrics in metrics_list if metrics[metric_name] is not None]
    if valid_metrics:
        total_metric = sum(valid_metrics)
        average_metric = total_metric / len(valid_metrics)
        return average_metric
    else:
        return 0  # or any other default value preferred
