# Imports from external libraries
import sys
from typing import List
from bs4 import BeautifulSoup
from nltk.tokenize import word_tokenize
import time
# Imports from internal libraries
from stopwords import stop_words_slovene
import preprocessing
import configs
import tools

def run_basic(words:List[str]):
    print(f"Querring for:{words}")
    print()

    documents = tools.get_html_files(configs.DATA_PATH)
    # documents = [x for x in documents if "podatki.gov.si.340.html" in x]

    n_found_docs = 0

    nsnipets = 5


    found_documents = []
    start_time = time.time()
    for i,doc in enumerate(documents):
        print(f"\rProcessing document {i+1}/{len(documents)}",end="")

        page = open(doc)
        soup = BeautifulSoup(page.read(), features="lxml")
        [s.extract() for s in soup(['style', 'script', '[document]', 'head', 'title'])]
        tokens_orig = word_tokenize(soup.get_text(separator=" "))
        tokens = list(enumerate(tokens_orig))
        tokens = [(x[0], x[1].lower()) for x in tokens if x[1].isalpha()]
        tokens = [x for x in tokens if x[1] not in stop_words_slovene if x[1] in words]
        if len(tokens)>0:
            n_found_docs += 1
        frequency = len(tokens)
        snipets = []
        for index,_ in tokens:
            start = index - 2
            end = index + 3
            if start < 0:
                start = 0
            if end >= len(tokens_orig):
                end = len(tokens_orig) - 1
            snipet = tokens_orig[start:end]
            snipet = " ".join(snipet)
            snipet = f"...{snipet}..."
            snipets.append(snipet)

        snipet_aug = ""
        for s in snipets[:nsnipets]:
            snipet_aug += f"{s:<60}"
        if len(tokens)>0:
            doc = doc.replace(f"{configs.DATA_PATH}/", "")
            found_documents.append({"frequency":frequency,"doc":doc,"snipet_aug":snipet_aug})
            # found_documents.append(f"{frequency:<15}{doc:<50}{snipet_aug}")
    end_time = time.time()

    print()

    print(f"Found {n_found_docs} documents in: {end_time - start_time}")
    header = f"{'Frequency':<15}{'Document name':<50}"
    for i in range(nsnipets):
        snip_name = f"Snipet {i}"
        header += f"{snip_name:<60}"
    print(header)
    print("-" * (15 + 60 * nsnipets + 50))

    found_documents.sort(key=lambda x: x["frequency"],reverse=True)
    for i in found_documents:
        print(f"{i['frequency']:<15}{i['doc']:<50}{i['snipet_aug']}")





querry = sys.argv[1]
querry = preprocessing.preprocess_string(querry)

run_basic(querry)