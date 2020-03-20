# Topic dispatcher - textanalysis.word_indexing (Version: 0.0.1)

Sends input topics to SQL processor and topic frequency operator.

## Inport

* **articles** (Type: message) Message with list of words

## outports

* **log** (Type: string) Logging data
* **words** (Type: message) message with body is dictionary

## Config

* **debug_mode** - Debug mode (Type: boolean) Sending debug level information to log port


# Tags
sdi_utils : 

