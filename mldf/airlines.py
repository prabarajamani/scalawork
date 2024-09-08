#https://corgis-edu.github.io/corgis/csv/airlines/

rawdata = spark.read.load("../data/airlines.csv", format="csv", header=True)
rawdata.show(5)

rawdata = rawdata.dropDuplicates(['review'])

from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType, DoubleType, DateType

from nltk.stem.wordnet import WordNetLemmatizer
from nltk.corpus import stopwords
from nltk import pos_tag
import langid
import string
import re

def strip_non_ascii(data_str):
    ''' Returns the string without non ASCII characters'''
    stripped = (c for c in data_str if 0 < ord(c) < 127)
    return ''.join(stripped)

def check_blanks(data_str):
    is_blank = str(data_str.isspace())
    return is_blank

def check_lang(data_str):
    from langid.langid import LanguageIdentifier, model
    identifier = LanguageIdentifier.from_modelstring(model, norm_probs=True)
    predict_lang = identifier.classify(data_str)

    if predict_lang[1] >= .9:
        language = predict_lang[0]
    else:
        language = predict_lang[0]
    return language

def fix_abbreviation(data_str):
    data_str = data_str.lower()
    data_str = re.sub(r'\bthats\b', 'that is', data_str)
    data_str = re.sub(r'\bive\b', 'i have', data_str)
    data_str = re.sub(r'\bim\b', 'i am', data_str)
    data_str = re.sub(r'\bya\b', 'yeah', data_str)
    data_str = re.sub(r'\bcant\b', 'can not', data_str)
    data_str = re.sub(r'\bdont\b', 'do not', data_str)
    data_str = re.sub(r'\bwont\b', 'will not', data_str)
    data_str = re.sub(r'\bid\b', 'i would', data_str)
    data_str = re.sub(r'wtf', 'what the fuck', data_str)
    data_str = re.sub(r'\bwth\b', 'what the hell', data_str)
    data_str = re.sub(r'\br\b', 'are', data_str)
    data_str = re.sub(r'\bu\b', 'you', data_str)
    data_str = re.sub(r'\bk\b', 'OK', data_str)
    data_str = re.sub(r'\bsux\b', 'sucks', data_str)
    data_str = re.sub(r'\bno+\b', 'no', data_str)
    data_str = re.sub(r'\bcoo+\b', 'cool', data_str)
    data_str = re.sub(r'rt\b', '', data_str)
    data_str = data_str.strip()
    return data_str

def remove_features(data_str):
    # compile regex
    url_re = re.compile('https?://(www.)?\w+\.\w+(/\w+)*/?')
    punc_re = re.compile('[%s]' % re.escape(string.punctuation))
    num_re = re.compile('(\\d+)')
    mention_re = re.compile('@(\w+)')
    alpha_num_re = re.compile("^[a-z0-9_.]+$")
    # convert to lowercase
    data_str = data_str.lower()
    # remove hyperlinks
    data_str = url_re.sub(' ', data_str)
    # remove @mentions
    data_str = mention_re.sub(' ', data_str)
    # remove puncuation
    data_str = punc_re.sub(' ', data_str)
    # remove numeric 'words'
    data_str = num_re.sub(' ', data_str)
    # remove non a-z 0-9 characters and words shorter than 1 characters
    list_pos = 0
    cleaned_str = ''
    for word in data_str.split():
        if list_pos == 0:
            if alpha_num_re.match(word) and len(word) > 1:
                cleaned_str = word
            else:
                cleaned_str = ' '
        else:
            if alpha_num_re.match(word) and len(word) > 1:
                cleaned_str = cleaned_str + ' ' + word
            else:
                cleaned_str += ' '
        list_pos += 1
    # remove unwanted space, *.split() will automatically split on
    # whitespace and discard duplicates, the " ".join() joins the
    # resulting list into one string.
    return " ".join(cleaned_str.split())
removes stop words

# removes stop words
def remove_stops(data_str):
    # expects a string
    stops = set(stopwords.words("english"))
    list_pos = 0
    cleaned_str = ''
    text = data_str.split()
    for word in text:
        if word not in stops:
            # rebuild cleaned_str
            if list_pos == 0:
                cleaned_str = word
            else:
                cleaned_str = cleaned_str + ' ' + word
            list_pos += 1
    return cleaned_str
Part-of-Speech Tagging

# Part-of-Speech Tagging
def tag_and_remove(data_str):
    cleaned_str = ' '
    # noun tags
    nn_tags = ['NN', 'NNP', 'NNP', 'NNPS', 'NNS']
    # adjectives
    jj_tags = ['JJ', 'JJR', 'JJS']
    # verbs
    vb_tags = ['VB', 'VBD', 'VBG', 'VBN', 'VBP', 'VBZ']
    nltk_tags = nn_tags + jj_tags + vb_tags

    # break string into 'words'
    text = data_str.split()

    # tag the text and keep only those with the right tags
    tagged_text = pos_tag(text)
    for tagged_word in tagged_text:
        if tagged_word[1] in nltk_tags:
            cleaned_str += tagged_word[0] + ' '

    return cleaned_str
lemmatization

# lemmatization
def lemmatize(data_str):
    # expects a string
    list_pos = 0
    cleaned_str = ''
    lmtzr = WordNetLemmatizer()
    text = data_str.split()
    tagged_words = pos_tag(text)
    for word in tagged_words:
        if 'v' in word[1].lower():
            lemma = lmtzr.lemmatize(word[0], pos='v')
        else:
            lemma = lmtzr.lemmatize(word[0], pos='n')
        if list_pos == 0:
            cleaned_str = lemma
        else:
            cleaned_str = cleaned_str + ' ' + lemma
        list_pos += 1
    return cleaned_str
setup pyspark udf function

# setup pyspark udf function
strip_non_ascii_udf = udf(strip_non_ascii, StringType())
check_blanks_udf = udf(check_blanks, StringType())
check_lang_udf = udf(check_lang, StringType())
fix_abbreviation_udf = udf(fix_abbreviation, StringType())
remove_stops_udf = udf(remove_stops, StringType())
remove_features_udf = udf(remove_features, StringType())
tag_and_remove_udf = udf(tag_and_remove, StringType())
lemmatize_udf = udf(lemmatize, StringType())
Text processing

correct the data schema

rawdata = rawdata.withColumn('rating', rawdata.rating.cast('float'))
rawdata.printSchema()
root
|-- id: string (nullable = true)
|-- airline: string (nullable = true)
|-- date: string (nullable = true)
|-- location: string (nullable = true)
|-- rating: float (nullable = true)
|-- cabin: string (nullable = true)
|-- value: string (nullable = true)
|-- recommended: string (nullable = true)
|-- review: string (nullable = true)
from datetime import datetime
from pyspark.sql.functions import col

# https://docs.python.org/2/library/datetime.html#strftime-and-strptime-behavior
# 21-Jun-14 <----> %d-%b-%y
to_date =  udf (lambda x: datetime.strptime(x, '%d-%b-%y'), DateType())

rawdata = rawdata.withColumn('date', to_date(col('date')))
rawdata.printSchema()
root
 |-- id: string (nullable = true)
 |-- airline: string (nullable = true)
 |-- date: date (nullable = true)
 |-- location: string (nullable = true)
 |-- rating: float (nullable = true)
 |-- cabin: string (nullable = true)
 |-- value: string (nullable = true)
 |-- recommended: string (nullable = true)
 |-- review: string (nullable = true)
rawdata.show(5)
+-----+------------------+----------+--------+------+--------+-----+-----------+--------------------+
|   id|           airline|      date|location|rating|   cabin|value|recommended|              review|
+-----+------------------+----------+--------+------+--------+-----+-----------+--------------------+
|10551|Southwest Airlines|2013-11-06|     USA|   1.0|Business|    2|         NO|Flight 3246 from ...|
|10298|        US Airways|2014-03-31|      UK|   1.0|Business|    0|         NO|Flight from Manch...|
|10564|Southwest Airlines|2013-09-06|     USA|  10.0| Economy|    5|        YES|I'm Executive Pla...|
|10134|   Delta Air Lines|2013-12-10|     USA|   8.0| Economy|    4|        YES|MSP-JFK-MXP and r...|
|10912|   United Airlines|2014-04-07|     USA|   3.0| Economy|    1|         NO|Worst airline I h...|
+-----+------------------+----------+--------+------+--------+-----+-----------+--------------------+
only showing top 5 rows
rawdata = rawdata.withColumn('non_asci', strip_non_ascii_udf(rawdata['review']))


+-----+------------------+----------+--------+------+--------+-----+-----------+--------------------+--------------------+
|   id|           airline|      date|location|rating|   cabin|value|recommended|              review|            non_asci|
+-----+------------------+----------+--------+------+--------+-----+-----------+--------------------+--------------------+
|10551|Southwest Airlines|2013-11-06|     USA|   1.0|Business|    2|         NO|Flight 3246 from ...|Flight 3246 from ...|
|10298|        US Airways|2014-03-31|      UK|   1.0|Business|    0|         NO|Flight from Manch...|Flight from Manch...|
|10564|Southwest Airlines|2013-09-06|     USA|  10.0| Economy|    5|        YES|I'm Executive Pla...|I'm Executive Pla...|
|10134|   Delta Air Lines|2013-12-10|     USA|   8.0| Economy|    4|        YES|MSP-JFK-MXP and r...|MSP-JFK-MXP and r...|
|10912|   United Airlines|2014-04-07|     USA|   3.0| Economy|    1|         NO|Worst airline I h...|Worst airline I h...|
+-----+------------------+----------+--------+------+--------+-----+-----------+--------------------+--------------------+
only showing top 5 rows
rawdata = rawdata.select(raw_cols+['non_asci'])\
                 .withColumn('fixed_abbrev',fix_abbreviation_udf(rawdata['non_asci']))

+-----+------------------+----------+--------+------+--------+-----+-----------+--------------------+--------------------+--------------------+
|   id|           airline|      date|location|rating|   cabin|value|recommended|              review|            non_asci|        fixed_abbrev|
+-----+------------------+----------+--------+------+--------+-----+-----------+--------------------+--------------------+--------------------+
|10551|Southwest Airlines|2013-11-06|     USA|   1.0|Business|    2|         NO|Flight 3246 from ...|Flight 3246 from ...|flight 3246 from ...|
|10298|        US Airways|2014-03-31|      UK|   1.0|Business|    0|         NO|Flight from Manch...|Flight from Manch...|flight from manch...|
|10564|Southwest Airlines|2013-09-06|     USA|  10.0| Economy|    5|        YES|I'm Executive Pla...|I'm Executive Pla...|i'm executive pla...|
|10134|   Delta Air Lines|2013-12-10|     USA|   8.0| Economy|    4|        YES|MSP-JFK-MXP and r...|MSP-JFK-MXP and r...|msp-jfk-mxp and r...|
|10912|   United Airlines|2014-04-07|     USA|   3.0| Economy|    1|         NO|Worst airline I h...|Worst airline I h...|worst airline i h...|
+-----+------------------+----------+--------+------+--------+-----+-----------+--------------------+--------------------+--------------------+
only showing top 5 rows
 rawdata = rawdata.select(raw_cols+['fixed_abbrev'])\
                  .withColumn('stop_text',remove_stops_udf(rawdata['fixed_abbrev']))

+-----+------------------+----------+--------+------+--------+-----+-----------+--------------------+--------------------+--------------------+
|   id|           airline|      date|location|rating|   cabin|value|recommended|              review|        fixed_abbrev|           stop_text|
+-----+------------------+----------+--------+------+--------+-----+-----------+--------------------+--------------------+--------------------+
|10551|Southwest Airlines|2013-11-06|     USA|   1.0|Business|    2|         NO|Flight 3246 from ...|flight 3246 from ...|flight 3246 chica...|
|10298|        US Airways|2014-03-31|      UK|   1.0|Business|    0|         NO|Flight from Manch...|flight from manch...|flight manchester...|
|10564|Southwest Airlines|2013-09-06|     USA|  10.0| Economy|    5|        YES|I'm Executive Pla...|i'm executive pla...|i'm executive pla...|
|10134|   Delta Air Lines|2013-12-10|     USA|   8.0| Economy|    4|        YES|MSP-JFK-MXP and r...|msp-jfk-mxp and r...|msp-jfk-mxp retur...|
|10912|   United Airlines|2014-04-07|     USA|   3.0| Economy|    1|         NO|Worst airline I h...|worst airline i h...|worst airline eve...|
+-----+------------------+----------+--------+------+--------+-----+-----------+--------------------+--------------------+--------------------+
only showing top 5 rows
rawdata = rawdata.select(raw_cols+['stop_text'])\
                 .withColumn('feat_text',remove_features_udf(rawdata['stop_text']))

+-----+------------------+----------+--------+------+--------+-----+-----------+--------------------+--------------------+--------------------+
|   id|           airline|      date|location|rating|   cabin|value|recommended|              review|           stop_text|           feat_text|
+-----+------------------+----------+--------+------+--------+-----+-----------+--------------------+--------------------+--------------------+
|10551|Southwest Airlines|2013-11-06|     USA|   1.0|Business|    2|         NO|Flight 3246 from ...|flight 3246 chica...|flight chicago mi...|
|10298|        US Airways|2014-03-31|      UK|   1.0|Business|    0|         NO|Flight from Manch...|flight manchester...|flight manchester...|
|10564|Southwest Airlines|2013-09-06|     USA|  10.0| Economy|    5|        YES|I'm Executive Pla...|i'm executive pla...|executive platinu...|
|10134|   Delta Air Lines|2013-12-10|     USA|   8.0| Economy|    4|        YES|MSP-JFK-MXP and r...|msp-jfk-mxp retur...|msp jfk mxp retur...|
|10912|   United Airlines|2014-04-07|     USA|   3.0| Economy|    1|         NO|Worst airline I h...|worst airline eve...|worst airline eve...|
+-----+------------------+----------+--------+------+--------+-----+-----------+--------------------+--------------------+--------------------+
only showing top 5 rows
 rawdata = rawdata.select(raw_cols+['feat_text'])\
                  .withColumn('tagged_text',tag_and_remove_udf(rawdata['feat_text']))

+-----+------------------+----------+--------+------+--------+-----+-----------+--------------------+--------------------+--------------------+
|   id|           airline|      date|location|rating|   cabin|value|recommended|              review|           feat_text|         tagged_text|
+-----+------------------+----------+--------+------+--------+-----+-----------+--------------------+--------------------+--------------------+
|10551|Southwest Airlines|2013-11-06|     USA|   1.0|Business|    2|         NO|Flight 3246 from ...|flight chicago mi...| flight chicago m...|
|10298|        US Airways|2014-03-31|      UK|   1.0|Business|    0|         NO|Flight from Manch...|flight manchester...| flight mancheste...|
|10564|Southwest Airlines|2013-09-06|     USA|  10.0| Economy|    5|        YES|I'm Executive Pla...|executive platinu...| executive platin...|
|10134|   Delta Air Lines|2013-12-10|     USA|   8.0| Economy|    4|        YES|MSP-JFK-MXP and r...|msp jfk mxp retur...| msp jfk mxp retu...|
|10912|   United Airlines|2014-04-07|     USA|   3.0| Economy|    1|         NO|Worst airline I h...|worst airline eve...| worst airline ua...|
+-----+------------------+----------+--------+------+--------+-----+-----------+--------------------+--------------------+--------------------+
only showing top 5 rows
 rawdata = rawdata.select(raw_cols+['tagged_text']) \
                  .withColumn('lemm_text',lemmatize_udf(rawdata['tagged_text'])


+-----+------------------+----------+--------+------+--------+-----+-----------+--------------------+--------------------+--------------------+
|   id|           airline|      date|location|rating|   cabin|value|recommended|              review|         tagged_text|           lemm_text|
+-----+------------------+----------+--------+------+--------+-----+-----------+--------------------+--------------------+--------------------+
|10551|Southwest Airlines|2013-11-06|     USA|   1.0|Business|    2|         NO|Flight 3246 from ...| flight chicago m...|flight chicago mi...|
|10298|        US Airways|2014-03-31|      UK|   1.0|Business|    0|         NO|Flight from Manch...| flight mancheste...|flight manchester...|
|10564|Southwest Airlines|2013-09-06|     USA|  10.0| Economy|    5|        YES|I'm Executive Pla...| executive platin...|executive platinu...|
|10134|   Delta Air Lines|2013-12-10|     USA|   8.0| Economy|    4|        YES|MSP-JFK-MXP and r...| msp jfk mxp retu...|msp jfk mxp retur...|
|10912|   United Airlines|2014-04-07|     USA|   3.0| Economy|    1|         NO|Worst airline I h...| worst airline ua...|worst airline ual...|
+-----+------------------+----------+--------+------+--------+-----+-----------+--------------------+--------------------+--------------------+
only showing top 5 rows
 rawdata = rawdata.select(raw_cols+['lemm_text']) \
                  .withColumn("is_blank", check_blanks_udf(rawdata["lemm_text"]))


+-----+------------------+----------+--------+------+--------+-----+-----------+--------------------+--------------------+--------+
|   id|           airline|      date|location|rating|   cabin|value|recommended|              review|           lemm_text|is_blank|
+-----+------------------+----------+--------+------+--------+-----+-----------+--------------------+--------------------+--------+
|10551|Southwest Airlines|2013-11-06|     USA|   1.0|Business|    2|         NO|Flight 3246 from ...|flight chicago mi...|   False|
|10298|        US Airways|2014-03-31|      UK|   1.0|Business|    0|         NO|Flight from Manch...|flight manchester...|   False|
|10564|Southwest Airlines|2013-09-06|     USA|  10.0| Economy|    5|        YES|I'm Executive Pla...|executive platinu...|   False|
|10134|   Delta Air Lines|2013-12-10|     USA|   8.0| Economy|    4|        YES|MSP-JFK-MXP and r...|msp jfk mxp retur...|   False|
|10912|   United Airlines|2014-04-07|     USA|   3.0| Economy|    1|         NO|Worst airline I h...|worst airline ual...|   False|
+-----+------------------+----------+--------+------+--------+-----+-----------+--------------------+--------------------+--------+
only showing top 5 rows
from pyspark.sql.functions import monotonically_increasing_id
# Create Unique ID
rawdata = rawdata.withColumn("uid", monotonically_increasing_id())
data = rawdata.filter(rawdata["is_blank"] == "False")

+-----+------------------+----------+--------+------+--------+-----+-----------+--------------------+--------------------+--------+---+
|   id|           airline|      date|location|rating|   cabin|value|recommended|              review|           lemm_text|is_blank|uid|
+-----+------------------+----------+--------+------+--------+-----+-----------+--------------------+--------------------+--------+---+
|10551|Southwest Airlines|2013-11-06|     USA|   1.0|Business|    2|         NO|Flight 3246 from ...|flight chicago mi...|   False|  0|
|10298|        US Airways|2014-03-31|      UK|   1.0|Business|    0|         NO|Flight from Manch...|flight manchester...|   False|  1|
|10564|Southwest Airlines|2013-09-06|     USA|  10.0| Economy|    5|        YES|I'm Executive Pla...|executive platinu...|   False|  2|
|10134|   Delta Air Lines|2013-12-10|     USA|   8.0| Economy|    4|        YES|MSP-JFK-MXP and r...|msp jfk mxp retur...|   False|  3|
|10912|   United Airlines|2014-04-07|     USA|   3.0| Economy|    1|         NO|Worst airline I h...|worst airline ual...|   False|  4|
+-----+------------------+----------+--------+------+--------+-----+-----------+--------------------+--------------------+--------+---+
only showing top 5 rows
# Pipeline for LDA model

from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml import Pipeline
from pyspark.ml.classification import NaiveBayes, RandomForestClassifier
from pyspark.ml.clustering import LDA
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder
from pyspark.ml.tuning import CrossValidator
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer
from pyspark.ml.feature import CountVectorizer

# Configure an ML pipeline, which consists of tree stages: tokenizer, hashingTF, and nb.
tokenizer = Tokenizer(inputCol="lemm_text", outputCol="words")
#data = tokenizer.transform(data)
vectorizer = CountVectorizer(inputCol= "words", outputCol="rawFeatures")
idf = IDF(inputCol="rawFeatures", outputCol="features")
#idfModel = idf.fit(data)

lda = LDA(k=20, seed=1, optimizer="em")

pipeline = Pipeline(stages=[tokenizer, vectorizer,idf, lda])


model = pipeline.fit(data)
Results presentation

Topics

+-----+--------------------+--------------------+
|topic|         termIndices|         termWeights|
+-----+--------------------+--------------------+
|    0|[60, 7, 12, 483, ...|[0.01349507958269...|
|    1|[363, 29, 187, 55...|[0.01247250144447...|
|    2|[46, 107, 672, 27...|[0.01188684264641...|
|    3|[76, 43, 285, 152...|[0.01132638300115...|
|    4|[201, 13, 372, 69...|[0.01337529863256...|
|    5|[122, 103, 181, 4...|[0.00930415977117...|
|    6|[14, 270, 18, 74,...|[0.01253817708163...|
|    7|[111, 36, 341, 10...|[0.01269584954257...|
|    8|[477, 266, 297, 1...|[0.01017486869509...|
|    9|[10, 73, 46, 1, 2...|[0.01050875237546...|
|   10|[57, 29, 411, 10,...|[0.01777350667863...|
|   11|[293, 119, 385, 4...|[0.01280305149305...|
|   12|[116, 218, 256, 1...|[0.01570714218509...|
|   13|[433, 171, 176, 3...|[0.00819684813575...|
|   14|[74, 84, 45, 108,...|[0.01700630002172...|
|   15|[669, 215, 14, 58...|[0.00779310974971...|
|   16|[198, 21, 98, 164...|[0.01030577084202...|
|   17|[96, 29, 569, 444...|[0.01297142577633...|
|   18|[18, 60, 140, 64,...|[0.01306356985169...|
|   19|[33, 178, 95, 2, ...|[0.00907425683229...|
+-----+--------------------+--------------------+
Topic terms

from pyspark.sql.types import ArrayType, StringType

def termsIdx2Term(vocabulary):
    def termsIdx2Term(termIndices):
        return [vocabulary[int(index)] for index in termIndices]
    return udf(termsIdx2Term, ArrayType(StringType()))

vectorizerModel = model.stages[1]
vocabList = vectorizerModel.vocabulary
final = ldatopics.withColumn("Terms", termsIdx2Term(vocabList)("termIndices"))