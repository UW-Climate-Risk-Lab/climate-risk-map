from io import BytesIO
import pandas as pd

def dataframe_to_csv_bytes(df: pd.DataFrame) -> bytes:
    """Converts a pandas DataFrame to CSV bytes.

    Args:
        df (pd.DataFrame): The pandas DataFrame to convert

    Returns:
        bytes: The CSV content as bytes, or None if an error occurs
    """
    try:
        csv_buffer = BytesIO()
        df.to_csv(csv_buffer, index=False)
        csv_bytes = csv_buffer.getvalue()
        return csv_bytes
    except Exception as e:
        print(f"An error occurred converting DataFrame to CSV bytes: {e}")
        return None