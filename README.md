# Text Analysis Framework deployed on SAP Data Intelligence

For a proof-of-concept we developed a text analysis framework on SAP Data Intelligence where we showed 

1. How-to scrape news web-sites  
2. How-to analyse the downloaded articles for words and topics. 

For the web-site scraping we are using the open source framework [scrapy](http://scrapy.org) that is widely used due to its simplicity and wealth of tools and for the text analysis we leverage [spacy](http://spacy.io) because it is acurate, fast and supports more languages than just English.

This repository has the following structure: 

* **/src** - contains the code of all di_textanalyis operators that can be modiefied and tested locally and uploaded using the *gensolution-package* ([on github](https://github.com/thhapke/sdi_utils)) or [pypi.org](https://pypi.org/project/sdi-utils/)
* **/solution** - containing the solution packages for the dockerfile, pipelines and operators. Each operator has a rudimentary documentation yet generated from the operator-code
* **/data_repository** - for example data to test the operators

In summary the framework consists of 

* **scrapy** container that receives 5 files (pipelines.py, settings.py, spider.py, items.py and middlewares.py) through its inports and delivers the articles via 2 outports (json-string and dict). The json-string data can be saved directly on an object store while the dictionary-formated output can further processed. 
* **meta-data** operator for saving the metadata of the article in a *HANA DB* table as reference
* **word extraction** operator using spacy for tokenizing, lemmatising and further cleasing the text by heuristic rules and using a *blacklist-file* for words to be ommitted. A *lexicon.csv* can be used to map the words according to the lexicon lists. The result is stored as an index in a HANA DB table. 
* **topic finding** operator uses an Latent Dirichlet Allocation algorithm to find word clusters that could interpreted as *topics*
* **sentiment analysis** to provide the *sentiment and subjectivity polarity* of the articles acccording to the used words

# Short Introduction into Text Analysis
Text analysis examines a larger amount of texts (= 'corpora') by using a number of algorithms in order to extract information from it. The basic level is to divide the text into **tokens** and then into **words** with its grammatical attributes. From there you can apply the next level of analytical methods to classify the text according e.g. to the sentiment, subjectivity or topic. 

With lemmatized words (inflected form) and the identified type or grammatical position, an index can be build up, e.g. using all nouns or proper nouns like in a book index. Furthermore a dictionary can be used to kind of normalize the words into language independent keywords, e.g. 
'legal' <- legal, justice, judge, lawyer .. for English or <- gesetzlich, Richter, Gericht, Anwalt, ... for German.  

**Topics** are the next analytical level by putting words into a set. This can be either done by definition or in algorithmic terminology by searching for word clusters. That means e.g. that news articles can be categorized into 'primaries in US', 'US-China-Relationship', 'Brexit', ...

 




 