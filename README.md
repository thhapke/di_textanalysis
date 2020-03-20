# Text Analysis Framework deployed on SAP Data Intelligence

For a proof-of-concept we developed a text analysis framework on SAP Data Intelligence where we showed 

1. How-to scrape news web-sites  
2. How-to analyse the downloaded articles for words and topics. 

For the web-site scraping we are using the open source framework [scrapy](http://scrapy.org) that is widely used due to its simplicity and wealth of tools and for the text analysis we leverage [spacy](http://spacy.io) because it is acurate, fast and supports more languages than just English.

This repository has the following structure: 

* **/src** - contains the code of all di_textanalyis operators that can be modiefied and tested locally and uploaded using the *gensolution-package* ([on github](https://github.com/thhapke/sdi_utils)) or [pypi.org](https://pypi.org/project/sdi-utils/)
* **/solution** - containing the solution packages for the dockerfile, pipelines and operators. Each operator has a rudimentary documentation yet generated from the operator-code

In summary the framework consists of 

* **scrapy** container that receives 5 files (pipelines.py, settings.py, spider.py, items.py and middlewares.py) through its inports and delivers the articles via 2 outports (json-string and dict). The json-string data can be saved directly on an object store while the dictionary-formated output can further processed. 
* **meta-data** of the article is stored in a *HANA DB* table as reference
* **word extraction** by using spacy the text is tokenized, lemmatised and further cleasend by heuristic rules and using a *blacklist-file* for words to be ommitted. A *lexicon.csv* can be used to map the words according to the lexicon lists in order to classify the articles. The result is stored as an index in a HANA DB table. 
* **topic finding** uses an Latent Dirichlet Allocation algorithm to find word clusters that could define interpreted as *topics*
* **sentiment analysis** to provide the *sentiment and subjectivity polarity* of the articles acccording to the used words





 