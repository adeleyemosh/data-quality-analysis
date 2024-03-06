import pandas as pd
import re

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


def calculate_validity(df, field_name, slrn_prefix='', slrn_length=0, meter_prefix='', meter_length=0, corresponding_meter_field=''):
    if field_name == 'SLRN':
        return (df[field_name].apply(lambda x: str(x).startswith(slrn_prefix) and len(str(x)) == slrn_length)).map({True: 'Valid', False: 'Not Valid'})
    elif field_name == 'Meter SLRN':
        return (df[field_name].apply(lambda x: str(x).startswith(meter_prefix) and len(str(x)) >= meter_length)).map({True: 'Valid', False: 'Not Valid'})
    elif field_name == 'Account Number':
        return ((df['Account Number'].astype(str).str.len() > 5) & df[field_name].apply(lambda x: str(x).isnumeric())).map({True: 'Valid', False: 'Not Valid'})
    elif field_name == 'Meter Number':
        df['Processed Meter Number'] = df[field_name].apply(preprocess_meter_number)
        
        df['Meter Number Validity'] = df['Processed Meter Number'].apply(is_valid_meter_number)
        
        return df['Meter Number Validity'].map({True: 'Valid', False: 'Not Valid'})
    elif field_name == 'Phone Number':        
        df['Processed Phone Number'] = df[field_name].apply(preprocess_phone_number)
        
        df['Phone Number Validity'] = df['Processed Phone Number'].apply(is_valid_phone_number)
        
        return df['Phone Number Validity'].map({True: 'Valid', False: 'Not Valid'})
    elif field_name == 'Email':
        valid_format = df[field_name].apply(lambda x: re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', str(x)) is not None)
        has_valid_characters = df[field_name].apply(lambda x: re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', str(x)) is not None)
        has_no_placeholders = df[field_name].apply(lambda x: not pd.isnull(x) and str(x).strip() != '')
        # Return 'Valid' if all conditions are met, otherwise 'Not Valid'
        return ((valid_format & has_valid_characters & has_no_placeholders).map({True: 'Valid', False: 'Not Valid'}))
    else:
        return None

def calculate_integrity(df, field_name, corresponding_meter_field=''):
    if field_name == 'SLRN':
        return ((df['SLRN'].notnull()) & (df[corresponding_meter_field].notnull() & (df[corresponding_meter_field].str.len() > 5)) | df['Account Number'].notnull()).map({True: 'Has Integrity', False: 'No Integrity'})
    elif field_name == 'Meter SLRN':
        return ((df['Meter SLRN'].str.len() > 10) & (df['SLRN'].notnull()) & (df['Meter Number'].notnull())).map({True: 'Has Integrity', False: 'No Integrity'})
    elif field_name == 'Meter Number':
        # Preprocess meter numbers
        df['Processed Meter Number'] = df[field_name].apply(preprocess_meter_number)
        
        # Apply the validity check function to the preprocessed meter numbers
        df['Meter Number Validity'] = df['Processed Meter Number'].apply(is_valid_meter_number)
        
        # Check integrity based on conditions
        has_integrity = (
            (df['Processed Meter Number'].notnull()) &
            (df['Processed Meter Number'].str.len() >= 5) &
            (df['Meter Status'] == 'Metered') &
            (df['SLRN'].notnull()) &
            (df['Meter Number Validity'])
        )
        # Map True/False to 'Has Integrity'/'No Integrity'
        return has_integrity.map({True: 'Has Integrity', False: 'No Integrity'})
    elif field_name == 'Email':
        consistent_formats = df[field_name].apply(lambda x: re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', str(x)) is not None)
        valid_characters = df[field_name].apply(lambda x: re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', str(x)) is not None)
        no_placeholders = df[field_name].apply(lambda x: not pd.isnull(x) and str(x).strip() != '')
        # Check integrity based on conditions
        return ((consistent_formats) & (valid_characters) & (no_placeholders) & (df[corresponding_meter_field].notnull())).map({True: 'Has Integrity', False: 'No Integrity'})
    elif field_name == 'Phone Number':        
        # Preprocess phone numbers
        df['Processed Phone Number'] = df[field_name].apply(preprocess_phone_number)
        
        df['Phone Number Integrity'] = df.apply(lambda row: pn_has_integrity(row['Processed Phone Number'], row['Meter Number']), axis=1)
        
        return df['Phone Number Integrity'].map({True: 'Has Integrity', False: 'No Integrity'})
    elif field_name == 'Account Number':
        return ((df['Account Number'].astype(str).str.len() > 5) & df['Account Number'].apply(lambda x: str(x).isnumeric()) & (df['SLRN'].notnull()) & (df['Meter Number'].notnull())).map({True: 'Has Integrity', False: 'No Integrity'})
    else:
        return None
