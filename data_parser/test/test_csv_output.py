import unittest
import pandas as pd
from pathlib import Path


class CsvReaderTester(unittest.TestCase):

    def test_csv_read(self):
        data_path = Path("/home/carneca/Documents/College/masters/semester1/data/assignment/spotify_warehouse/data/cleaned_playlist_data.csv")
        df = pd.read_csv(f"{data_path}")
        assert not df.empty
