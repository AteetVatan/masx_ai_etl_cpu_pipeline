"""
Date validation utilities for MASX AI ETL CPU Pipeline.

Provides consistent date format validation across the entire application.
"""

from datetime import datetime
from typing import Optional


def validate_date_format(date_str: str) -> bool:
    """
    Validate that date string is in YYYY-MM-DD format.

    Args:
        date_str: Date string to validate

    Returns:
        True if valid YYYY-MM-DD format, False otherwise
    """
    if not isinstance(date_str, str):
        return False

    try:
        # Check that the string is exactly 10 characters (YYYY-MM-DD)
        if len(date_str) != 10:
            return False
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except (ValueError, TypeError):
        return False


def validate_and_raise(date_str: str, parameter_name: str = "date") -> str:
    """
    Validate date format and raise ValueError if invalid.

    Args:
        date_str: Date string to validate
        parameter_name: Name of the parameter for error messages

    Returns:
        Validated date string

    Raises:
        ValueError: If date format is invalid
    """
    if not validate_date_format(date_str):
        raise ValueError(
            f"{parameter_name} must be in YYYY-MM-DD format (e.g., '2025-07-02'), got: '{date_str}'"
        )
    return date_str


def get_today_date() -> str:
    """
    Get today's date in YYYY-MM-DD format.

    Returns:
        Today's date as YYYY-MM-DD string
    """
    return datetime.now().strftime("%Y-%m-%d")


def format_date_for_table(date_str: str) -> str:
    """
    Format date string for use in table names.

    Args:
        date_str: Date string in YYYY-MM-DD format

    Returns:
        Formatted date string for table name (converts to YYYYMMDD for table naming)

    Raises:
        ValueError: If date format is invalid
    """
    validated_date = validate_and_raise(date_str, "date")
    # Convert YYYY-MM-DD to YYYYMMDD for table naming
    table_date = validated_date.replace("-", "")
    return f"feed_entries_{table_date}"
