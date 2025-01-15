import time
import pytz
import re
import os
import logging
from datetime import datetime, timezone, timedelta
from timezonefinder import TimezoneFinder

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_timezone(tz_offset: str) -> timezone:
    # Regex to parse timezone offset
    match = re.match(r"^UTC(?P<sign>[+-])(?P<hours>\d{2}):(?P<minutes>\d{2})$", tz_offset)
    if not match:
        raise ValueError(f"Invalid timezone offset format: {tz_offset}")

    sign = -1 if match.group('sign') == '-' else 1
    hours = int(match.group('hours'))
    minutes = int(match.group('minutes'))
    delta = timedelta(hours=sign * hours, minutes=sign * minutes)
    return timezone(delta)

def shift_timestamp(timestamp: float, origin_tz: timezone) -> float:
    ''' the timestamp(utc) is shifted by origin_tz offset so that
    if timestamp/origin_tz corresponds to 01/01/2025 5PM EST
    returned timestamp corresponds to 01/01/2025 5PM GMT '''
    delta = origin_tz.utcoffset(None)
    delta_secs = float(delta.total_seconds())
    return timestamp + delta_secs

def unshift_timestamp(shifted_timestamp: float, origin_tz: timezone) -> float:
    ''' the input shifted_timestamp is unshifted by origin_tz offset so that
    if shifted_timestamp/utc corresponds to 01/01/2025 5PM GMT
    returned timestamp corresponds to 01/01/2025 5PM EST '''
    delta = origin_tz.utcoffset(None)
    delta_secs = delta.total_seconds()
    return shifted_timestamp - delta_secs

def get_shifted_timestamp(origin_tz: timezone) -> float:
    timestamp = datetime.now(tz=timezone.utc).timestamp()
    return shift_timestamp(timestamp, origin_tz)

def get_tz_offset2(tz: timezone) -> str:
    return tz.tzname(None)

def get_tz_offset(latitude: float, longitude: float) -> str:
    """
    Determine the UTC offset (in 'UTC±HH:MM' format) based on latitude and longitude.

    Args:
        latitude: Latitude of the location.
        longitude: Longitude of the location.

    Returns:
        UTC offset string in the format 'UTC±HH:MM' or 'Unknown' if it cannot be determined.
    """
    try:
        # Determine the timezone
        tf = TimezoneFinder()
        time_zone_name = tf.timezone_at(lat=latitude, lng=longitude)
        
        if not time_zone_name:
            return "Unknown"

        # Get the current time in the identified timezone
        timezone = pytz.timezone(time_zone_name)
        local_time = datetime.now(timezone)

        # Calculate UTC offset
        offset_seconds = local_time.utcoffset().total_seconds()
        hours, remainder = divmod(abs(offset_seconds), 3600)
        minutes = remainder // 60
        sign = '+' if offset_seconds >= 0 else '-'

        return f"UTC{sign}{int(hours):02d}:{int(minutes):02d}"
    except Exception as e:
        logging.error(f"Failed to determine UTC offset: {e}")
        return "Unknown"
