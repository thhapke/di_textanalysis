# Text Condensation (German) - textanalysis.text_condensation_g (Version: 0.0.1)

Condense text to significant German words 

## Inport

* **articles** (Type: message) Message with body as dictionary 
* **disregards** (Type: message) Message with additional stopwords 

## outports

* **log** (Type: string) Logging data
* **unique_words** (Type: array) Array of new unique words
* **data** (Type: message.Dictionary) Output List of index words

## Config

* **debug_mode** - Debug mode (Type: boolean) Sending debug level information to log port
* **nouns_only** - Nouns only (Capital words) (Type: boolean) Nouns only (Capital words)


# Tags
sdi_utils : 

