# Imports from external libraries
from typing import List
import os
import re
import time
# Imports from internal libraries
import configs


def get_html_files(data_root: str) -> List[str]:
    filters = [
        lambda x: re.search(re.compile("__MACOSX"), x),
        lambda x: not x.endswith((".html"))
    ]

    file_list = []
    for root, dirs, files in os.walk(data_root, topdown=False):
        for f in files:
            full_path = os.path.join(root, f)
            filt = [x(full_path) for x in filters]
            if not any(filt):
                file_list.append(full_path)

    return file_list


def timeit(method):
    def timed(*args, **kwargs):
        ts = time.time()
        result = method(*args, **kwargs)
        te = time.time()
        print(f"Method took: {te - ts}")
        return result
    return timed


if __name__ == "__main__":
    files = get_html_files(configs.DATA_PATH)
    print(len(files))
