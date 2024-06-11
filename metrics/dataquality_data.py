import pandas as pd
import re

from metrics.dataquality import preprocess_meter_number
from metrics.dataquality import is_valid_meter_number
from metrics.dataquality import preprocess_phone_number
from metrics.dataquality import is_valid_phone_number
from metrics.dataquality import pn_has_integrity

def calculate_validity(df, field_name, slrn_prefix='', slrn_length=0, meter_prefix='', meter_length=0, corresponding_meter_field=''):
    if field_name == 'SLRN':
        return (df[field_name].apply(lambda x: str(x).startswith(slrn_prefix) and len(str(x)) == slrn_length)).map({True: 'Valid', False: 'Not Valid'})
    elif field_name == 'Meter SLRN':
        return (df[field_name].apply(lambda x: str(x).startswith(meter_prefix) and len(str(x)) >= meter_length)).map({True: 'Valid', False: 'Not Valid'})
    elif field_name == 'Account Number':
        if slrn_prefix in ['YEDCBD', 'AEDCBD']:
            return ((df[field_name].astype(str).str.len() >= 6)).map({True: 'Valid', False: 'Not Valid'})
        else:
            return (df['Account Number'].astype(str).str.len() > 5).map({True: 'Valid', False: 'Not Valid'})
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
        no_noemail = ~df[field_name].astype(str).str.contains('noemail', case=False) & \
            ~df[field_name].astype(str).str.contains('nomail', case=False) & \
            ~df[field_name].astype(str).str.contains('nil', case=False) & \
            ~df[field_name].astype(str).str.contains('noamail', case=False) & \
            ~df[field_name].astype(str).str.contains('nomai', case=False) & \
            ~df[field_name].astype(str).str.contains('example', case=False) 
        
        # Return 'Valid' if all conditions are met, otherwise 'Not Valid'
        return ((valid_format & has_valid_characters & has_no_placeholders & no_noemail).map({True: 'Valid', False: 'Not Valid'}))
    else:
        return None

def calculate_integrity(df, field_name, slrn_prefix='', slrn_length=None, corresponding_meter_field=''):
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
        no_noemail = ~df[field_name].astype(str).str.contains('noemail', case=False) & \
            ~df[field_name].astype(str).str.contains('nomail', case=False) & \
            ~df[field_name].astype(str).str.contains('nil', case=False) & \
            ~df[field_name].astype(str).str.contains('noamail', case=False) & \
            ~df[field_name].astype(str).str.contains('nomai', case=False) & \
            ~df[field_name].astype(str).str.contains('example', case=False) 
        
        # Check integrity based on conditions
        return ((consistent_formats) & (valid_characters) & (no_placeholders) & no_noemail  & (df[corresponding_meter_field].notnull())).map({True: 'Has Integrity', False: 'No Integrity'})
    elif field_name == 'Phone Number':        
        # Preprocess phone numbers
        df['Processed Phone Number'] = df[field_name].apply(preprocess_phone_number)
        
        df['Phone Number Integrity'] = df.apply(lambda row: pn_has_integrity(row['Processed Phone Number'], row['Meter Number']), axis=1)
        
        return df['Phone Number Integrity'].map({True: 'Has Integrity', False: 'No Integrity'})
    elif field_name == 'Account Number':
        if slrn_prefix in ['YEDCBD', 'AEDCBD']:
            return ((df[field_name].astype(str).str.len() >= 6 | df[field_name].notnull()) & (df['SLRN'].notnull() | df['Meter Number'].notnull()) | df['Meter Status'] == 'Unmetered').map({True: 'Has Integrity', False: 'No Integrity'})
        else:
            return ((df[field_name].astype(str).str.len() > 5) & df[field_name].notnull() & df['SLRN'].notnull() & df['Meter Number'].notnull()).map({True: 'Has Integrity', False: 'No Integrity'})
    else:
        return None