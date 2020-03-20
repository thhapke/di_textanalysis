# Word Frequency - textanalysis.word_frequency (Version: 0.0.1)

Find word frequency of prepared 

## Inport

* **articles** (Type: message) Message with body as dictionary 
* **stopwords** (Type: message) Message with additional stopwords 

## outports

* **log** (Type: string) Logging data
* **data** (Type: message.Dictionary) Output words with number of occurances

## Config

* **debug_mode** - Debug mode (Type: boolean) Sending debug level information to log port
* **collect** - Collect data (Type: boolean) Collect data before sending it to the output port
* **num_words** - Number of Words (Type: integer) Number of words return to the output port


# Tags
sdi_utils : 

