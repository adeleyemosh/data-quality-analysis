import pandas as pd
import re

from metrics.dataquality import preprocess_meter_number
from metrics.dataquality import is_valid_meter_number
from metrics.dataquality import preprocess_phone_number
from metrics.dataquality import is_valid_phone_number
from metrics.dataquality import pn_has_integrity
from metrics.dataquality import is_valid_email


def calculate_validity(df, field_name, slrn_prefix='', slrn_length=0, meter_prefix='', meter_length=0, corresponding_meter_field=''):
    complete_records = df[df[field_name].notnull()].copy()
    
    if field_name == 'SLRN':
        return (complete_records[field_name].apply(lambda x: str(x).startswith(slrn_prefix) and len(str(x)) == slrn_length)).map({True: 'Valid', False: 'Not Valid'})
    elif field_name == 'Meter SLRN':
        return (complete_records[field_name].apply(lambda x: str(x).startswith(meter_prefix) and len(str(x)) >= meter_length)).map({True: 'Valid', False: 'Not Valid'})
    elif field_name == 'Account Number':
        if slrn_prefix in ['YEDCBD', 'AEDCBD']:
            # Convert float values to integers before converting to strings and applying isnumeric check
            return (complete_records[field_name].astype(str).str.len() >= 6 | complete_records[field_name].notnull()).map({True: 'Valid', False: 'Not Valid'})
        else:
            return (complete_records[field_name].astype(str).str.len() >= 5).map({True: 'Valid', False: 'Not Valid'}) 
    elif field_name == 'Meter Number':
        # Preprocess meter numbers
        complete_records.loc[:, 'Processed Meter Number'] = complete_records[field_name].apply(preprocess_meter_number)
        
        # Apply the validity check function to the preprocessed meter numbers
        complete_records.loc[:, 'Meter Number Validity'] = complete_records['Processed Meter Number'].apply(is_valid_meter_number)
        
        return complete_records['Meter Number Validity'].map({True: 'Valid', False: 'Not Valid'})
    elif field_name == 'Phone Number':        
        # Preprocess phone numbers
        complete_records.loc[:, 'Processed Phone Number'] = complete_records[field_name].apply(preprocess_phone_number)
        
        # Apply the validity check function to the preprocessed phone numbers
        complete_records.loc[:, 'Phone Number Validity'] = complete_records['Processed Phone Number'].apply(is_valid_phone_number)
        
        return complete_records['Phone Number Validity'].map({True: 'Valid', False: 'Not Valid'})
    elif field_name == 'Email':
        # Apply the validity check function to the email field
        complete_records.loc[:, 'Email Validity'] = complete_records[field_name].apply(is_valid_email)
        
        return complete_records['Email Validity'].map({True: 'Valid', False: 'Not Valid'})
    else:
        return None
    

def calculate_integrity(df, field_name, slrn_prefix='', corresponding_meter_field=''):
    complete_records = df[df[field_name].notnull()].copy()
    
    if field_name == 'SLRN':
        return ((complete_records[field_name].notnull()) & (complete_records[corresponding_meter_field].notnull() & (complete_records[corresponding_meter_field].str.len() > 5)) | complete_records['Account Number'].notnull()).map({True: 'Has Integrity', False: 'No Integrity'})
    elif field_name == 'Meter SLRN':
        return ((complete_records['Meter SLRN'].str.len() > 10) & (complete_records['SLRN'].notnull()) & (complete_records['Meter Number'].notnull())).map({True: 'Has Integrity', False: 'No Integrity'})
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
        
        return has_integrity.map({True: 'Has Integrity', False: 'No Integrity'})
    elif field_name == 'Email':
        # Apply the validity check function to the email field
        complete_records.loc[:, 'Email Validity'] = complete_records[field_name].apply(is_valid_email)
        
        valid_email = complete_records['Email Validity']
        
        combined_conditions = valid_email & (complete_records['Meter Number'].notnull() | complete_records['Account Number'].notnull())
        
        return combined_conditions.map({True: 'Has Integrity', False: 'No Integrity'})
    elif field_name == 'Phone Number':        
        # Preprocess phone numbers
        complete_records.loc[:, 'Processed Phone Number'] = complete_records[field_name].apply(preprocess_phone_number)
        
        # Apply the validity check function to the preprocessed phone numbers
        complete_records.loc[:, 'Phone Number Validity'] = complete_records['Processed Phone Number'].apply(is_valid_phone_number)
        
        combined_conditions = complete_records['Phone Number Validity'] & (complete_records['Meter Number'].notnull() | complete_records['Account Number'].notnull())
        
        return combined_conditions.map({True: 'Has Integrity', False: 'No Integrity'})
    elif field_name == 'Account Number':
        if slrn_prefix in ['YEDCBD', 'AEDCBD']:
            return ((complete_records[field_name].astype(str).str.len() >= 6 | complete_records[field_name].notnull()) & (complete_records['SLRN'].notnull() | complete_records['Meter Status'].notnull())).map({True: 'Has Integrity', False: 'No Integrity'})
        else:
            return ((complete_records[field_name].astype(str).str.len() > 5) &  (complete_records['SLRN'].notnull()) & (complete_records['Meter Number'].notnull())).map({True: 'Has Integrity', False: 'No Integrity'})
    else:
        return None