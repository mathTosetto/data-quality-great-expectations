import pandas as pd

class TaxiDataLoader:
    def __init__(self, url: str) -> None:
        if isinstance(url, str):
            self.url = url
            self.df = None
        else:
            raise ValueError("URL must be a string")

    def _processsed_data(self) -> pd.DataFrame:
        if self.df is not None:
            transform_to_date = ["pickup_datetime", "dropoff_datetime"]
            for col in transform_to_date:
                self.df[col] = pd.to_datetime(self.df[col])
        return self.df

    def load_data(self) -> None:
        try:
            self.df = pd.read_csv(self.url)
            self.df = self._processsed_data()
        except Exception as e:
            print(f"Error loading data: {e}")
            self.df = None

    def get_data(self) -> pd.DataFrame:
        if self.df is not None:
            return self.df
