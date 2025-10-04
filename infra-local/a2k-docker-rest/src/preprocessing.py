import pandas as pd
from sklearn.model_selection import train_test_split
from typing import Tuple


def load_data() -> pd.DataFrame:
    """
    Load pre-cleaned car dataset.
    Expected file: ../data/output_data/car_data_cleaned.parquet
    """
    df = pd.read_parquet("../data/output_data/car_data_cleaned.parquet")
    return df


def split_data(
    df: pd.DataFrame,
    test_size: float = 0.2,
    random_state: int = 42,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Split dataset into train and test sets.
    """
    train, test = train_test_split(
        df, test_size=test_size, random_state=random_state
    )
    return train, test
