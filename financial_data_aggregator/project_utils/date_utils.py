from datetime import datetime, timedelta

def get_weeks(start_date_str, end_date_str):
    """
    Generates weekly intervals (Monday to Sunday) between start_date and end_date.
    Args:
        start_date_str (str): Start date in 'YYYY-MM-DD' format.
        end_date_str (str): End date in 'YYYY-MM-DD' format.
    Yields:
        tuple: (monday_date, sunday_date) for each week in the range.
    """
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')

    current_monday = start_date - timedelta(days=start_date.weekday())
    
    while current_monday <= end_date:
        current_sunday = current_monday + timedelta(days=6)
        # Ensure the week's Sunday doesn't exceed the overall end_date
        # and the week's Monday doesn't start after the overall end_date
        if current_monday > end_date:
            break
        yield current_monday, min(current_sunday, end_date)
        current_monday += timedelta(days=7)