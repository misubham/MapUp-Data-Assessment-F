import pandas as pd


def generate_car_matrix(df)->pd.DataFrame:
    """
    Creates a DataFrame  for id combinations.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame: Matrix generated with 'car' values, 
                          where 'id_1' and 'id_2' are used as indices and columns respectively.
    """
       # Get the unique values for 'id_1' and 'id_2'
    unique_ids_1 = df['id_1'].unique()
    unique_ids_2 = df['id_2'].unique()

    # Create a matrix filled with zeros, with 'id_1' and 'id_2' unique values as indices and columns respectively
    car_matrix = pd.DataFrame(0, index=unique_ids_1, columns=unique_ids_2)

    # Iterate over each row in the input DataFrame
    for _, row in df.iterrows():
        # Update the value at the corresponding indices in the matrix
        car_matrix.at[row['id_1'], row['id_2']] = row['car']

    return car_matrix

    return df


def get_type_count(df)->dict:
    """
    Categorizes 'car' values into types and returns a dictionary of counts.

    Args:
        df (pandas.DataFrame)

    Returns:
        dict: A dictionary with car types as keys and their counts as values.
    """
    # Group the DataFrame by 'car' values and get the size of each group
    type_counts = df.groupby('car').size()

    # Convert the result Series to a dictionary
    return type_counts.to_dict()

    return dict()


def get_bus_indexes(df)->list:
    """
    Returns the indexes where the 'bus' values are greater than twice the mean.

    Args:
        df (pandas.DataFrame)

    Returns:
        list: List of indexes where 'bus' values exceed twice the mean.
    """
     # Calculate the mean of 'bus' values
    mean = df['bus'].mean()

    # Get the indexes where 'bus' values are greater than twice the mean
    bus_indexes = df[df['bus'] > 2 * mean].index.tolist()

    return bus_indexes
    return list()


def filter_routes(df)->list:
    """
    Filters and returns routes with average 'truck' values greater than 7.

    Args:
        df (pandas.DataFrame)

    Returns:
        list: List of route names with average 'truck' values greater than 7.
    """
    # Calculate the average 'truck' values for each route
    route_truck_means = df.groupby('route')['truck'].mean()

    # Get the routes with average 'truck' values greater than 7
    routes = route_truck_means[route_truck_means > 7].index.tolist()

    return routes
    return list()


def multiply_matrix(matrix)->pd.DataFrame:
    """
    Multiplies matrix values with custom conditions.

    Args:
        matrix (pandas.DataFrame)

    Returns:
        pandas.DataFrame: Modified matrix with values multiplied based on custom conditions.
    """
   # Custom conditions
    condition1 = (matrix['column1'] > 5)
    condition2 = (matrix['column2'] < 3)

    # Apply custom conditions to the matrix
    matrix.loc[condition1, 'column1'] *= 2
    matrix.loc[condition2, 'column2'] *= 3
    return matrix


def time_check(df)->pd.Series:
    """
    Use shared dataset-2 to verify the completeness of the data by checking whether the timestamps for each unique (`id`, `id_2`) pair cover a full 24-hour and 7 days period

    Args:
        df (pandas.DataFrame)

    Returns:
        pd.Series: return a boolean series
    """
   def time_check(df):
    def check_timedelta(series):
        # Check if the difference between maximum and minimum timestamps is >= 24 hours and 7 days
        time_diff = series.max() - series.min()
        return time_diff >= timedelta(hours=24) and time_diff >= timedelta(days=7)

    # Group by unique pairs of `id` and `id_2`, then check the completeness of timestamps
    completeness = df.groupby(['id', 'id_2'])['timestamp'].apply(check_timedelta)

    return completeness

    return pd.Series()
