# Imports from external libraries
from bs4 import BeautifulSoup
from nltk.tokenize import word_tokenize
import pandas as pd
import numpy as np
# Imports from internal libraries
import tools
import configs
from stopwords import stop_words_slovene


def preprocess_string(data):
    tokens = word_tokenize(data)
    tokens = list(enumerate(tokens))
    tokens = [(x[0], x[1].lower()) for x in tokens if x[1].isalpha()]
    tokens = [x[1] for x in tokens if x[1] not in stop_words_slovene]
    return tokens


# @tools.timeit
def parse_document(page_path: str) -> pd.DataFrame:
    page = open(page_path)
    soup = BeautifulSoup(page.read(), features="lxml")
    [s.extract() for s in soup(['style', 'script', '[document]', 'head', 'title'])]
    tokens = word_tokenize(soup.get_text(separator=" "))
    tokens = list(enumerate(tokens))
    tokens = [(x[0], x[1].lower()) for x in tokens if x[1].isalpha()]
    tokens = [x for x in tokens if x[1] not in stop_words_slovene]
    tokens = [{"indexes": str(x[0]), "word": x[1], "documentName": page_path} for x in tokens]
    df = pd.DataFrame(tokens)
    if not df.empty:
        df["frequency"] = np.ones(len(df))
        agg_fun = {"indexes": lambda x: ",".join(x),
                   "documentName": "first",
                   "frequency": "sum"}

        df = df.groupby(["word", ]).agg(agg_fun).reset_index()
        return df
    else:
        return df


if __name__ == "__main__":
    pages = tools.get_html_files(configs.DATA_PATH)
    pages = [x for x in pages if "e-prostor.gov.si.147.html" in x]
    print(pages)
    page_tokens = parse_document(pages[0])
    print(page_tokens)
    print(len(page_tokens))
