import pandas as pd
import numpy as np

def normalize_location(location, x_max=120, y_max=80):
    """
    Normalize a location (x, y) to a range of [0, 1] based on the provided max values.

    Args:
        location: A location represented as a list, tuple, or numpy array with two elements (x, y).
        x_max (int, optional): Maximum x-coordinate value for normalization. Defaults to 120.
        y_max (int, optional): Maximum y-coordinate value for normalization. Defaults to 80.

    Returns:
        List : A list containing the normalized x and y coordinates.
    """
    if isinstance(location, (list, tuple, np.ndarray)) and len(location) == 2:
        x, y = location
        return [x / x_max, y / y_max]
    return [None, None]

def normalize_end_location(location, x_max=120, y_max=80):
    """
    Normalize an end location (x, y) to a range of [0, 1] based on the provided max values.

    Args:
        location: A location represented as a list, tuple, or numpy array with two elements (x, y).
        x_max (int, optional): Maximum x-coordinate value for normalization. Defaults to 120.
        y_max (int, optional): Maximum y-coordinate value for normalization. Defaults to 80.

    Returns:
        List: A list containing the normalized end x and y coordinates.
    """
    if isinstance(location, (list, tuple, np.ndarray)) and len(location) == 2:
        return [location[0] / x_max, location[1] / y_max]
    return [None, None]

def enrich_pass_data(df):
    """
    Enrich the DataFrame with normalized pass data.

    Args:
        df: DataFrame containing event data with columns:

    Returns:
        df: DataFrame with additional columns for normalized start and end locations and recipient.
    """

    # 1. Normalize start coordinates (player position)
    df["x"], df["y"] = zip(*df["location"].map(normalize_location))

    # 2. Initialize end_x/end_y columns
    df["end_x"], df["end_y"] = None, None

    # 3. Valid passes with end_location
    is_pass = (df["type.name"] == "Pass") & df["pass.end_location"].notnull()

    # 4. Normalize end_location
    normalized_ends = (
        df.loc[is_pass, "pass.end_location"]
        .map(normalize_end_location)
        .apply(pd.Series)
    )
    normalized_ends.columns = ["end_x", "end_y"]

    # 5. Assign back to main df
    df.loc[is_pass, ["end_x", "end_y"]] = normalized_ends.values

    # 6. Optional: recipient column for convenience
    if "pass.recipient.name" in df.columns:
        df["recipient"] = df["pass.recipient.name"]

    return df