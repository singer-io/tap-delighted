from datetime import datetime
from typing import Optional

DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def get_datetime_from_timestamp(timestamp: Optional[int]) -> Optional[str]:
    """Convert a timestamp to an ISO 8601 formatted date-time string.
    If the timestamp is None, return None.

    Args:
        timestamp (Optional[int]): The timestamp in seconds.

    Returns:
        Optional[str]: The ISO 8601 formatted date-time string.
    """

    if timestamp is None:
        return None
    datetime_obj = datetime.fromtimestamp(timestamp)
    return datetime_obj.strftime(DATETIME_FORMAT)


def get_timestamp_from_datetime(date_str: Optional[str]) -> Optional[int]:
    """Convert an ISO 8601 formatted date-time string to a timestamp in seconds.
    If the date_str is None, return None.

    Args:
        date_str (Optional[str]): The ISO 8601 formatted date-time string.

    Returns:
        Optional[int]: The timestamp in seconds.
    """

    if date_str is None:
        return None

    dt = datetime.strptime(date_str, DATETIME_FORMAT)
    return int(dt.timestamp())
