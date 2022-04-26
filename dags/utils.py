from datetime import datetime, timedelta


def days_ago(x: float):
    return (datetime.utcnow().astimezone() - timedelta(days=x)).strftime("%Y-%m-%d")
