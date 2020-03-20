"""TextBlobSentimentAnalysis operator
"""

from pysix_subengine.base_operator import BaseOperator, OperatorInfo, PortInfo, Message
from textblob import TextBlob, Blobber
from textblob_de import TextBlobDE as TextBlobDE
from textblob_fr import PatternTagger as PatternTaggerFR, PatternAnalyzer as PatternAnalyzerFR
import json


class TextBlobSentimentAnalysis(BaseOperator):
    """Applies Sentiment Analysis Using TextBlob
    """

    def __init__(self, inst_id, op_id):
        super(TextBlobSentimentAnalysis, self).__init__(inst_id=inst_id, op_id=op_id)
        self._append_port_handler([u"article"], self.__on_article)
        self._language = {'Lefigaro': 'F', 'Lemonde': 'F', 'Spiegel': 'G', 'FAZ': 'G', 'Sueddeutsche': 'G',
                          'Elpais': 'S', 'Elmundo': 'S'}
        self._aggregated = []

    def __on_article(self, article):
        self.logger.info(f"Received article")
        article_recs = json.loads(article.body)
        att_dict = article.attributes
        att_dict['operator'] = 'TextBlobSentimentAnalysis'

        for article_rec in article_recs:
            polarity, subjectivity = self._get_article_sentiment(article_rec)
            sentiment_rec = {'DATE': article_rec['date'], 'MEDIA': article_rec['media'],
                             'HASH_TEXT': article_rec['hash_text'],
                             'POLARITY': polarity, 'SUBJECTIVITY': subjectivity}
            self.logger.info(f"Finished sentiment analysis")
            if self.config.aggregate:
                if len(self._aggregated) % 10 == 0:
                    self.logger.info(f"Aggregating ({len(self._aggregated)} so far).")
                self._aggregated.append(sentiment_rec)
            else:
                self.logger.info(f"Sending single record")
                self._send_message('sentiment', Message(attributes=att_dict, body=sentiment_rec))

        if self.config.aggregate:
            if 'storage.fileIndex' in att_dict and 'storage.fileCount' in att_dict and 'storage.endOfSequence' in att_dict:
                if att_dict['storage.fileIndex'] + 1 == att_dict['storage.fileCount']:
                    self.logger.info(f"Sending aggregated records")
                    self._send_message('sentiment', Message(attributes=att_dict, body=self._aggregated))
                    self._aggregated = []

    def _get_article_sentiment(self, article):
        """
        Extracts sentiment analysis for article.
        @param article: article dictionary (retrieved from the Data Lake)
        @returns: (article_level_polarity, article_level_subjectivity)
        """
        if self._language[article['media']] == 'G':
            blob = TextBlobDE(article['text'])
            polarity, subjectivity = (blob.sentiment.polarity, blob.sentiment.subjectivity)
        elif self._language[article['media']] == 'F':
            tb = Blobber(pos_tagger=PatternTaggerFR(), analyzer=PatternAnalyzerFR())
            blob = tb(article['text'])
            polarity, subjectivity = blob.sentiment
        else:  # for now defaults to FR (just for PoC)
            blob = TextBlob(article['text'])
            polarity, subjectivity = (blob.sentiment.polarity, blob.sentiment.subjectivity)
        return polarity, subjectivity

    def _get_operator_info(self):
        config_type = "http://sap.com/vflow/com.sap.nlp.textBlobSentimentAnalysis.schema.json#"
        return OperatorInfo("Detects sentiments from articles using TextBlob",
                            inports=[PortInfo(u"article", required=True, type_=u"message")],
                            outports=[PortInfo(u"sentiment", required=True, type_=u"message")],
                            tags={"textblob": "",
                                  "textblob-de": "",
                                  "textblob-fr": ""},
                            dollar_type=config_type,
                            extensible=True,
                            icon="file-text-o")