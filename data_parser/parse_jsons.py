import pandas as pd
from typing import List, Tuple, Dict
from pathlib import Path
import json


def get_dict() -> Dict[Path, Tuple[int, int]]:
    data_p = Path("../data/spotify_playlist_data").resolve()

    assert data_p.exists(), "Path is wrong!, change path to correct path"

    file_number_dict = {}

    for json_file in data_p.rglob("*.json"):
        file_name: str = json_file.name
        number1, number2 = map(int, file_name.split(".")[2].split("-"))
        file_number_dict[json_file] = (number1, number2)
    return file_number_dict


file_number_dict = get_dict()


def get_files_to_download(
    file_number_dict: Dict[Path, Tuple[int, int]]
) -> List[Path]:
    NUMBER_OF_PLAYLISTS_WANTED = 15000

    files_to_download: List[Path] = []

    for p, num_tuple in file_number_dict.items():
        upper = num_tuple[1]
        lower = num_tuple[0]
        if lower >= NUMBER_OF_PLAYLISTS_WANTED:
            continue
        if lower <= NUMBER_OF_PLAYLISTS_WANTED <= upper:
            files_to_download.append(p)
            break
        else:
            files_to_download.append(p)
            continue

    return files_to_download

def get_csv_lines(play_dict: Dict) -> str:
    cols_to_make = [ 'pos',
                     'artist_name',
                     'track_uri',
                     'artist_uri',
                     'track_name',
                     'album_uri',
                     'duration_ms',
                     'album_name']

    df = pd.DataFrame(play_dict)

    for col in cols_to_make: df[col] = df.apply(lambda row: row["tracks"][col], axis=1)


    df.drop("tracks", axis=1, inplace=True)
    return df.to_csv()

def main():
    files_to_download = get_files_to_download(get_dict())

    count = 0
    for file in files_to_download:
        print(f"{count} Done: {len(files_to_download) - count} left to go!")
        count += 1
        with open(file, 'r') as f:
            play_dict_list = json.load(f)["playlists"]
        for play_dict in play_dict_list:
            with open("../data/cleaned_playlist_data.csv", 'a') as f:
                f.write(get_csv_lines(play_dict))
    print("All done! :)")

if __name__ == "__main__":
    main()
