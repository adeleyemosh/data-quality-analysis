import pandas as pd

from metrics.overallscore import calculate_overall_score
from metrics.dataquality import calculate_data_quality_metrics
from metrics.dataquality import calculate_phone_number_metrics
from metrics.dataquality import calculate_email_metrics
from metrics.dataquality import calculate_average_metrics

def calculate_unique_meter_count(df, date_column, meter_number_column):
    unique_meter_count = df.groupby(date_column)[meter_number_column].nunique().reset_index()
    unique_meter_count.columns = [date_column, 'Unique Meter Count']
    return unique_meter_count

def calculate_metrics_by_month(df, key_fields):
    result_data = []

    for year_month in df['Year Month'].unique():
        # Filter the DataFrame for the current year_month
        df_month = df[df['Year Month'] == year_month]
        
        # Calculate metrics for the current month
        metrics_list = []

        for field_name in key_fields:
            if field_name in df.columns:
                if field_name in ['SLRN', 'Account Number', 'Meter Number', 'Meter SLRN']:
                    metrics = calculate_data_quality_metrics(df_month, field_name, 'ECGBD', 12, 'ECGCR', 11)
                elif field_name == 'Phone Number':
                    metrics = calculate_phone_number_metrics(df_month, field_name, 'ECGBD', corresponding_meter_field='Meter Number')
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
                'Overall Score': overall_score,
                'Unique Meter Count': unique_meter_count
            }
            result_data.append(metrics_data)

    # Convert the list of dictionaries to a DataFrame
    result_df = pd.DataFrame(result_data)
    result_df = result_df.sort_values(by='Year Month', ascending=False)
    
    return result_df