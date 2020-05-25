# Text Analysis Framework deployed on SAP Data Intelligence

There are basically 2 approaches for doing a text analysis: 

1. Brute force blackbox single step approach or 
2. Step-by-step

The brute force approach is using a text as a whole and classifies the text by using 'Deep Learning' techniques. The most familiar classifications are the **sentiment** and **subjectivity** polarity. Based on the type of the corpus the result could be quite good even for short texts like in twitter or movie and product reviews. But due to the very nature of language of being able to convey all kinds of information in numerous ways and that you need mostly an extensive amount of trained data this approach is of limited practicality.

The second step-by-step approach could also use models trained by *deep learning* techniques but in a more controlled way and combined with other techniques. For example for the grammatical analysis a deep learning trained model could be of great use to create word bags that can subsequently been evaluated to find similiar topics in texts by applying cluster algorithms. 

For a proof-of-concept we have done both ways and illustrate how SAP Data Intelligence can be used to automatically indexing texts. The result can then be used for further researches like the trend of postively annotated brands in news or forums. 



This repository has the following structure: 

* **/src** - contains the code of all di_textanalyis operators that can be modiefied and tested locally and uploaded using the *gensolution-package* ([on github](https://github.com/thhapke/sdi_utils)) or [pypi.org](https://pypi.org/project/sdi-utils/)
* **/solution** - containing the solution packages for the dockerfile, pipelines and operators. Each operator has a rudimentary documentation yet generated from the operator-code
* **/data_repository** - example data to test the operators

## Quadruple Jump of Text Analysis
The four parts of a text analysis are

1. Getting the corpus/corpora, texts that needs to be analysed 
2. Preparing the texts before funneling them into the analyse pipeline. Example: to add a language attribute, removing formating tags or correcting common format error like missing space between ending quotes and dots.
3. Identifying the words as tokens (space/dot/... separated) , grammatical and lematized items (verb, adjective, noun, ..) and as entities carrying some semantics like being a person, location or organization. 
4. Cleansing the resulting words by applying heuristics like blacklisted words that should be removed, pattern recognitions and lexicographical mappings. 

The result can be used for automatically **indexing texts**, finding **trends of words** or entities that are mentioned more frequently over time or finally identifying **topics**. Topics are set of words that appear together numerously in different texts.

### Getting the Texts by Scraping News Articles from Websites

For the web-site scraping we are using the open source framework [scrapy](http://scrapy.org) that is widely used due to its simplicity and wealth of tools and for the text analysis we leverage [spacy](http://spacy.io) because it is acurate, fast and supports more languages than just English.

The tool of choice is the widely used open source framework [**scrapy**](https://scrapy.org).  container that receives 5 files (pipelines.py, settings.py, spider.py, items.py and middlewares.py) through its inports and delivers the articles via 2 outports (json-string and dict). The json-string data can be saved directly on an object store while the dictionary-formated output can further processed. 

* **meta-data** operator for saving the metadata of the article in a *HANA DB* table as reference

### Identifying Words as Grammatical and Semantic Entities
* **word_index** operator using spacy for tokenizing, lemmatising and further cleasing the text by heuristic rules and using a *blacklist-file* for words to be ommitted. A *lexicon.csv* can be used to map the words according to the lexicon lists. The result is stored as an index in a HANA DB table. 

### Detecting Sentiment Scoring 
* **sentiment analysis** to provide the *sentiment and subjectivity polarity* of the articles acccording to the used words

### Identifying Topics 
* **topic finding** operator uses an Latent Dirichlet Allocation algorithm to find word clusters that could interpreted as *topics*



# Short Introduction into Text Analysis
Text analysis examines a larger amount of texts (= 'corpora') by using a number of algorithms in order to extract information from it. The basic level is to divide the text into **tokens** and then into **words** with its grammatical attributes. From there you can apply the next level of analytical methods to classify the text according e.g. to the sentiment, subjectivity or topic. 

With lemmatized words (inflected form) and the identified type or grammatical position, an index can be build up, e.g. using all nouns or proper nouns like in a book index. Furthermore a dictionary can be used to kind of normalize the words into language independent keywords, e.g. 
'legal' <- legal, justice, judge, lawyer .. for English or <- gesetzlich, Richter, Gericht, Anwalt, ... for German.  

**Topics** are the next analytical level by putting words into a set. This can be either done by definition or in algorithmic terminology by searching for word clusters. That means e.g. that news articles can be categorized into 'primaries in US', 'US-China-Relationship', 'Brexit', ...

 




 