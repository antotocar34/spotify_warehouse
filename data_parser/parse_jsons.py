import os
import json
from typing import List, Tuple, Dict
from pathlib import Path

import pandas as pd


NUMBER_OF_PLAYLISTS_WANTED: int = 500

def get_dict() -> Dict[Path, Tuple[int, int]]:
    data_p = Path(f"../data/spotify_playlist_data")

    assert (
        data_p.exists()
    ), f"Path is wrong!, change path to correct path: {str(data_p)}"

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


def get_df(play_dict: Dict) -> pd.DataFrame:
    cols_to_make = [
        "pos",
        "artist_name",
        "track_uri",
        "artist_uri",
        "track_name",
        "album_uri",
        "duration_ms",
        "album_name",
    ]

    df = pd.DataFrame(play_dict)
    try:
        assert cols_to_make == list(df.iloc[2]["tracks"].keys())
    except AssertionError:
        breakpoint()


    for col in cols_to_make:
        df[col] = df.apply(lambda row: row["tracks"][col], axis=1)

    df.drop("tracks", axis=1, inplace=True)
    return df


def main():
    files_to_download = get_files_to_download(get_dict())

    cols = [
        "name",
        "collaborative",
        "pid",
        "modified_at",
        "num_tracks",
        "num_albums",
        "num_followers",
        "num_edits",
        "duration_ms",
        "num_artists",
        "pos",
        "artist_name",
        "track_uri",
        "artist_uri",
        "track_name",
        "album_uri",
        "album_name",
    ]

    out_df = pd.DataFrame(columns=cols)
    for i, file in enumerate(files_to_download):
        print(f"{i} Done: {len(files_to_download) - i} left to go!")
        with open(file, "r") as f:
            play_dict_list = json.load(f)["playlists"]

        if len(play_dict_list) > NUMBER_OF_PLAYLISTS_WANTED:
            play_dict_list = play_dict_list[:NUMBER_OF_PLAYLISTS_WANTED]

        for play_dict in play_dict_list:
            tmp_df = get_df(play_dict)
            out_df = out_df.append(tmp_df)
    #            with open("../data/cleaned_playlist_data.csv", write_mode) as f:
    #                f.write(get_csv_lines(play_dict, header))
    print("All done! :)")
    out_df.to_csv("../data/cleaned_playlist_data.csv", index=False)



if __name__ == "__main__":
    main()
