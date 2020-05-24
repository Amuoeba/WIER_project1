# Imports from external libraries
import pandas as pd
# Imports from internal libraries
from database import index_database
from preprocessing import parse_document
import tools
import configs

documents = tools.get_html_files(configs.DATA_PATH)
index_database.reset_databse()
connection = index_database.get_connection()
counter = 0
# documents = documents[:100]
for x in documents:
    counter += 1
    print(f"\r Processing file: {counter}/{len(documents)} ", end="")
    df = parse_document(x)
    if not df.empty:
        df.to_sql('Posting', con=connection, if_exists='append', chunksize=1000, index=False)
q = "SELECT DISTINCT(word) FROM Posting"
word_df = pd.read_sql(q, connection)
word_df.to_sql('IndexWord', con=connection, if_exists='append', chunksize=1000, index=False)
connection.close()
