from metrics.convert_percentage_to_scale import convert_percentage_to_scale

def calculate_overall_score(completeness_score, validity_score, integrity_score):
    # Convert percentage scores to the specified scale
    completeness_score = convert_percentage_to_scale(completeness_score)
    validity_score = convert_percentage_to_scale(validity_score)
    integrity_score = convert_percentage_to_scale(integrity_score)

    # Define weights for each metric
    completeness_weight = 0.4
    validity_weight = 0.4
    integrity_weight = 0.2

    # Define the maximum possible score for each metric (assuming a scale from 0 to 5)
    max_completeness_score = 5
    max_validity_score = 5
    max_integrity_score = 5

    # Calculate the overall score using the provided formula
    overall_score = (
        (completeness_weight * (completeness_score / max_completeness_score)) +
        (validity_weight * (validity_score / max_validity_score)) +
        (integrity_weight * (integrity_score / max_integrity_score))
    ) * 100

    return overall_score