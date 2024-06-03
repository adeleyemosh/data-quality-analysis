import pandas as pd

from metrics.convert_percentage_to_scale import convert_percentage_to_scale
from metrics.dataquality import calculate_data_quality_metrics, calculate_average_metrics

def calculate_overall_score(completeness_score, validity_score, integrity_score):
    # # Convert percentage scores to the specified scale
    # completeness_score = convert_percentage_to_scale(completeness_score)
    # validity_score = convert_percentage_to_scale(validity_score)
    # integrity_score = convert_percentage_to_scale(integrity_score)

    # # Weights for each metric
    # completeness_weight = 0.33333333333333333333333333333
    # validity_weight = 0.33333333333333333333333333333
    # integrity_weight = 0.33333333333333333333333333333

    # # Maximum possible score for each metric: scale from 0 to 5
    # max_completeness_score = 5
    # max_validity_score = 5
    # max_integrity_score = 5

    # overall_score = (
    #     (completeness_weight * (completeness_score / max_completeness_score)) +
    #     (validity_weight * (validity_score / max_validity_score)) +
    #     (integrity_weight * (integrity_score / max_integrity_score))
    # ) * 100
    
    total_score = (completeness_score + validity_score + integrity_score)
    overall_score = total_score / 3

    return overall_score

def calculate_overall_score_mom(df):
    overall_scores = []
    
    df = df.sort_values('Year Month')

    for _, group in df.groupby('Year Month'):
        metrics_list = []

        # Calculate metrics for each key field
        key_fields = ['SLRN', 'Account Number', 'Meter Number', 'Meter SLRN', 'Phone Number', 'Email']
        
        slrn_prefix = {
			'ecg_prefix': 'ECGBD',
			'aedc_prefix': 'AEDCBD',
            'yedc_prefix': 'YEDCBD'
		}

        for field_name in key_fields:
            if field_name in df.columns:
                if field_name in ['SLRN', 'Account Number', 'Meter Number']:
                    if slrn_prefix['ecg_prefix'] == 'ECGBD':
                        metrics = calculate_data_quality_metrics(group, field_name, 'ECGBD', 12, 'ECGCR', 11)
                    elif slrn_prefix['yedc_prefix'] == 'YEDCBD':
                        metrics = calculate_data_quality_metrics(group, field_name, 'YEDCBD', 13)
                    elif slrn_prefix['aedc_prefix'] == 'AEDCBD':
                        metrics = calculate_data_quality_metrics(group, field_name, 'AEDCBD', 13)
                    metrics_list.append(metrics)
                elif field_name == 'Meter SLRN':
                    if slrn_prefix['ecg_prefix'] == 'ECGBD':
                        metrics = calculate_data_quality_metrics(group, field_name, 'ECGBD', 12, 'ECGCR', 11)
                        metrics_list.append(metrics)
                elif field_name == 'Phone Number':
                    if slrn_prefix['ecg_prefix'] == 'ECGBD':
                        metrics = calculate_data_quality_metrics(group, field_name, 'ECGBD', 12, 'ECGCR', 11)
                    elif slrn_prefix['aedc_prefix'] == 'AEDCBD':
                        metrics = calculate_data_quality_metrics(group, field_name, 'AEDCBD', 13)
                    elif slrn_prefix['yedc_prefix'] == 'YEDCBD':
                        metrics = calculate_data_quality_metrics(group, field_name, 'YEDCBD', 13)
                    metrics_list.append(metrics)
                elif field_name == 'Email':
                    metrics = calculate_data_quality_metrics(group, field_name, 'ECGBD', 12, 'ECGCR', 11)
                    metrics_list.append(metrics)

        average_completeness = calculate_average_metrics(metrics_list, 'Completeness')
        average_validity = calculate_average_metrics(metrics_list, 'Validity')
        average_integrity = calculate_average_metrics(metrics_list, 'Integrity')

        # Calculate overall score using the calculate_overall_score function
        overall_score = calculate_overall_score(average_completeness, average_validity, average_integrity)
        
        overall_scores.append({'Year Month': group['Year Month'].iloc[0], 'Overall Score': overall_score})

    overall_scores_df = pd.DataFrame(overall_scores)
    
    # Merge MoM overall scores back to the original DataFrame
    df = df.merge(overall_scores_df, on='Year Month', how='left')

    return df
