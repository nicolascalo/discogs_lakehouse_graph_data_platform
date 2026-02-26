import os
import re


def extract_dump_date(fname):
    m = re.search(r"(\d{8})", fname)
    return m.group(1) if m else None


def get_folder_list(dir: str):
    folder_list = [x[0] for x in os.walk(dir)]

    
    return folder_list


def get_dump_dates(folder_list):
    print(folder_list)

    dump_dates = [extract_dump_date(f) for f in folder_list]
    dump_dates = [d for d in dump_dates if d]
    dump_dates = list(set(dump_dates))

    if not dump_dates:
        raise RuntimeError(f"No Discogs dump delta_tables found in {folder_list}")

    dump_dates = list(set(dump_dates))
    dump_dates.sort()

    return dump_dates
