import pandas as pd

from metrics.overallscore import calculate_overall_score
from metrics.dataquality import calculate_data_quality_metrics
from metrics.dataquality import calculate_average_metrics

def calculate_quality_score_by_collector(df, field_metrics, weights=None):
	if weights is None:
		weights = {'Completeness': 0.4, 'Validity': 0.4, 'Integrity': 0.4}

	# Group by 'First Captured Username' and calculate metrics for each collector
	collector_metrics = {}
	unique_users = df['First Captured Username'].unique()

	for user in unique_users:
		slrn_prefix = {
			'ecg_prefix': 'ECGBD',
			'aedc_prefix': 'AEDCBD'
		}
		
		user_df = df[df['First Captured Username'] == user]

		user_field_metrics = {field: {} for field in field_metrics}
		for field in field_metrics:
			if slrn_prefix['ecg_prefix'] == 'ECGBD':
				metrics = calculate_data_quality_metrics(user_df, field, 'ECGBD', 12, 'ECGCR', 11)
				user_field_metrics[field] = metrics
			elif slrn_prefix['aedc_prefix'] == 'AEDCBD':
				metrics = calculate_data_quality_metrics(user_df, field, 'AEDCBD', 13)
				user_field_metrics[field] = metrics

		metrics_list = []

		# Calculate metrics for each key field
		key_fields = ['SLRN', 'Account Number', 'Meter Number', 'Meter SLRN', 'Phone Number', 'Email']

		for field_name in key_fields:
			if field_name in df.columns:
				if field_name in ['SLRN', 'Account Number', 'Meter Number', 'Meter SLRN'] and slrn_prefix['ecg_prefix'] == 'ECGBD':
					metrics = calculate_data_quality_metrics(user_df, field_name, 'ECGBD', 12, 'ECGCR', 11)
				elif field_name in ['SLRN', 'Account Number', 'Meter Number'] and slrn_prefix['aedc_prefix'] == 'AEDCBD':
					metrics = calculate_data_quality_metrics(user_df, field_name, 'AEDCBD', 13)
				elif field_name == 'Phone Number' and slrn_prefix['ecg_prefix'] == 'ECGBD':
					metrics = calculate_data_quality_metrics(user_df, field_name, 'ECGBD', 12, 'ECGCR', 11)
				elif field_name == 'Phone Number' and slrn_prefix['aedc_prefix'] == 'AEDCBD':
					metrics = calculate_data_quality_metrics(user_df, field_name, 'AEDCBD')
				elif field_name == 'Email':
					metrics = calculate_data_quality_metrics(user_df, field_name, 'ECGBD', 12, 'ECGCR', 11)
				
				metrics_list.append(metrics)

		# Calculate average completeness, validity, and integrity
		average_completeness = calculate_average_metrics(metrics_list, 'Completeness')
		average_validity = calculate_average_metrics(metrics_list, 'Validity')
		average_integrity = calculate_average_metrics(metrics_list, 'Integrity')
		
		# Scale down the overall score
		average_completeness_scaled = average_completeness * weights['Completeness']
		average_validity_scaled = average_validity * weights['Validity']
		average_integrity_scaled = average_integrity * weights['Integrity']

		# Calculate overall score using the calculate_overall_score function
		overall_score = calculate_overall_score(average_completeness, average_validity, average_integrity)

		collector_metrics[user] = {
			'Average Completeness': average_completeness_scaled,
			'Average Validity': average_validity_scaled,
			'Average Integrity': average_integrity_scaled,
			'Overall Average': overall_score 
		}

	# Create a DataFrame from the collector_metrics dictionary
	collector_df = pd.DataFrame(collector_metrics).T

	# Remove percentage sign and convert columns to numeric
	collector_df[['Average Completeness', 'Average Validity', 'Average Integrity', 'Overall Average']] = collector_df[['Average Completeness', 'Average Validity', 'Average Integrity', 'Overall Average']].replace('%', '', regex=True).astype(float)

	def safe_mean(row):
		values = [value for value in row if pd.notna(value) and str(value).replace('.', '').isnumeric()]
		return sum(values) / len(values) if values else 0

	collector_df['Overall Average'] = collector_df[['Average Completeness', 'Average Validity', 'Average Integrity']].apply(safe_mean, axis=1)

	# Add percentage sign back to the result
	collector_df[['Average Completeness', 'Average Validity', 'Average Integrity', 'Overall Average']] = collector_df[['Average Completeness', 'Average Validity', 'Average Integrity', 'Overall Average']].applymap(lambda x: f"{x:.2f}%")

	# Sort the DataFrame by the overall average in descending order
	collector_df = collector_df.sort_values(by='Overall Average', ascending=False)

	return collector_df
