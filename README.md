# WIER_project1
This project contains an implementation of a web crawler that visits .gov.si websites.

# Additional files
  * database_dump.sql --> Dump of the database after 2 hours of crawling
  * WIER1_WebCrawling.pdf --> Project report
### Running the crawler
1. Define the "CHROME_DRIVER" parameter in "configs.py" to point to the path of
the chrome driver on our computer
2. Define "USERNAME", "PASSWORD", "HOST" parameters in "configs.py" accord-
ing to your local postgresql server
3. execute "python cravler.py" in the root directory

