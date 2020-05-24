# Imports from external libraries
import pandas as pd
from typing import List
import time
import sys
# Imports from internal libraries
from database import index_database
import configs
from bs4 import BeautifulSoup
from nltk.tokenize import word_tokenize
import preprocessing


def get_snippet(indexes, document):
    indexes = indexes.split(",")
    indexes = [int(x) for x in indexes]

    page = open(document)
    soup = BeautifulSoup(page.read(), features="lxml")
    [s.extract() for s in soup(['style', 'script', '[document]', 'head', 'title'])]
    tokens = word_tokenize(soup.get_text(separator=" "))

    snipets = []
    for index in indexes:
        start = index - 2
        end = index + 3
        if start < 0:
            start = 0
        if end >= len(tokens):
            end = len(tokens) - 1
        snipet = tokens[start:end]
        snipet = " ".join(snipet)
        snipet = f"...{snipet}..."
        snipets.append(snipet)

    return snipets


def prepare_output_row(row: pd.Series, nsnipets=5):
    frequency = row["frequency"]
    docname = row["documentName"]
    docname_ref = docname.replace(f"{configs.DATA_PATH}/", "")
    snipet_indices = row["indexes"]
    snipets = get_snippet(snipet_indices, docname)
    snipet_aug = ""
    for s in snipets[:nsnipets]:
        snipet_aug += f"{s:<60}"
    s = f"{frequency:<15}{docname_ref:<50}{snipet_aug}"
    return s


def run_querry(words: List[str]):
    connection = index_database.get_connection()
    variables = "?," * len(words)
    variables = variables[:-1]
    q = f"""
    SELECT sum(frequency) as frequency,documentName,group_concat(indexes) as indexes FROM Posting
    WHERE word IN ({variables})
    GROUP BY documentName
    """

    q_start = time.time()
    df = pd.read_sql_query(q, connection, params=words)
    q_end = time.time()

    print(f"Querring for:{words}")
    print(f"Found {len(df)} documents in {q_end - q_start}s")

    df.sort_values(by=["frequency"], ascending=False, inplace=True)
    nsnipets = 5
    header = f"{'Frequency':<15}{'Document name':<50}"
    for i in range(nsnipets):
        snip_name = f"Snipet {i}"
        header += f"{snip_name:<60}"
    print(header)
    print("-" * (15 + 60 * nsnipets + 50))
    snipet_start = time.time()
    for i, row in df.iterrows():
        row_out = prepare_output_row(row, nsnipets=nsnipets)
        print(row_out)
    snipet_end = time.time()
    print(f"Snippets found in: {snipet_end-snipet_start}s")



querry = sys.argv[1]
querry = preprocessing.preprocess_string(querry)
run_querry(querry)

#
# if __name__ == "__main__":
#     run_querry(["hiša", "delo", "voda"])
