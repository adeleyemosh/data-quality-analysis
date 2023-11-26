def convert_percentage_to_scale(score):
    # Convert percentage score to a scale from 0 to 5
    if score == 0:
        return 0
    elif score == 100:
        return 5
    else:
        return round(score / 20, 1)