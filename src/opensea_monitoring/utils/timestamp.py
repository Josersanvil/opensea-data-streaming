from datetime import datetime


def parse_timestamp(timestamp: str) -> datetime:
    """
    Parses a timestamp string into a datetime object.
    Attempts to parse the timestamp using two different formats:
    - "YYYY-MM-DD"
    - "YYYY-MM-DDTHH:MM:SS"

    No timezone information is inferred from the string,
    so the datetime object returned will be naive.

    @param timestamp: The timestamp string to parse.
    @return: The datetime object.
    """
    try:
        ts = datetime.strptime(timestamp, r"%Y-%m-%d")
    except ValueError:
        ts = datetime.strptime(timestamp, r"%Y-%m-%dT%H:%M:%S")
    return ts
