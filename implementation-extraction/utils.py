# Imports from external libraries
import os
import json
# Imports from internal libraries
import configs


def save_and_print_results(site, results, mode):
    site_name = os.path.basename(site)
    site_name = os.path.splitext(site_name)[0]
    filename = f"{configs.result_path}{mode}/{site_name}.json"
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=4)
        print(json.dumps(results))


def save_automatic_extraction_results(site_pair, results, mode):
    filename = f"{configs.result_path}{mode}/{site_pair}/alignment.txt"
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, "w") as f:
        for item in results:
            f.write(f'Site1: {item["site1"]}')
            f.write("\n")
            f.write(f'Site2: {item["site2"]}')
            f.write("\n")
            f.write("-" * 50)
            f.write("\n")


def read_and_clean_html(html_path):
    print(f"Reading file: {html_path}")
    content = open(html_path, 'r', encoding="utf8", errors='ignore').read()
    content = content.replace("\n", "")
    content = content.replace("\t", "")
    return content


def preety_print_alignment(long, short, offset):
    for i in range(offset):
        short.insert(i, '-' * len(long[i]))
    for i in range(offset, len(short)):
        l_s = len(short[i])
        l_l = len(long[i])
        if l_s < l_l:
            d = l_l - l_s
            short[i] += d * "-"
        else:
            d = l_s - l_l
            long[i] += d * "-"

    print(short)
    print(long)
