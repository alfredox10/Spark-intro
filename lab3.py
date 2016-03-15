import re
import datetime
import sys
import os
from operator import add, div

#--------- Spark setup -----------
# Configure the environment for PySpark
if 'SPARK_HOME' not in os.environ:
    os.environ['SPARK_HOME'] = '/opt/spark'
# Create a variable for our root path
SPARK_HOME = os.environ['SPARK_HOME']
# Add the PySpark/py4j to the Python Path
sys.path.insert(0, os.path.join(SPARK_HOME, "python", "lib"))
sys.path.insert(0, os.path.join(SPARK_HOME, "python"))
from pyspark.sql import Row
from pyspark import SparkContext
sc = SparkContext("local", "Lab3")
#--------- Spark setup -----------


DATAFILE_PATTERN = '^(.+),"(.+)",(.*),(.*),(.*)'

baseDir = os.path.join('data')
inputPath = os.path.join('cs100', 'lab3')

GOOGLE_PATH = 'Google.csv'
GOOGLE_SMALL_PATH = 'Google_small.csv'
AMAZON_PATH = 'Amazon.csv'
AMAZON_SMALL_PATH = 'Amazon_small.csv'
GOLD_STANDARD_PATH = 'Amazon_Google_perfectMapping.csv'
STOPWORDS_PATH = 'stopwords.txt'

dummyrdd = sc.textFile('test', 4, 0)


def removeQuotes(s):
    """
    Remove quotation marks from an input string
    Args:
        s (str): input string that might have the quote "" characters
    Returns:
        str: a string without the quote characters
    """
    return ''.join(i for i in s if i!='"')


def parseDatafileLine(datafileLine):
    """ Parse a line of the data file using the specified regular expression pattern
    Args:
        datafileLine (str): input string that is a line from the data file
    Returns:
        str: a string parsed using the given regular expression and without the quote characters
    """
    match = re.search(DATAFILE_PATTERN, datafileLine)
    if match is None:
        print 'Invalid datafile line: %s' % datafileLine
        return (datafileLine, -1)
    elif match.group(1) == '"id"':
        print 'Header datafile line: %s' % datafileLine
        return (datafileLine, 0)
    else:
        product = '%s %s %s' % (match.group(2), match.group(3), match.group(4))
        return ((removeQuotes(match.group(1)), product), 1)


def parseData(filename):
    """ Parse a data file
    Args:
        filename (str): input file name of the data file
    Returns:
        RDD: a RDD of parsed lines
    """
    return (sc
            .textFile(filename, 4, 0)
            .map(parseDatafileLine)
            .cache())


def loadData(path):
    """ Load a data file
    Args:
        path (str): input file name of the data file
    Returns:
        RDD: a RDD of parsed valid lines
    """
    filename = os.path.join(baseDir, inputPath, path)
    raw = parseData(filename).cache()
    failed = (raw
              .filter(lambda s: s[1] == -1)
              .map(lambda s: s[0]))
    for line in failed.take(10):
        print '%s - Invalid datafile line: %s' % (path, line)
    valid = (raw
             .filter(lambda s: s[1] == 1)
             .map(lambda s: s[0])
             .cache())
    print '%s - Read %d lines, successfully parsed %d lines, failed to parse %d lines' % (path,
                                                                                          raw.count(),
                                                                                          valid.count(),
                                                                                          failed.count())
    assert failed.count() == 0
    assert raw.count() == (valid.count() + 1)
    return valid


quickbrownfox = 'A quick brown fox jumps over the lazy dog.'
split_regex = r'\W+'
# google = loadData(GOOGLE_PATH)
# amazon = loadData(AMAZON_PATH)
googleSmall = loadData(GOOGLE_SMALL_PATH)
amazonSmall = loadData(AMAZON_SMALL_PATH)


def simpleTokenize(string):
    """ A simple implementation of input string tokenization
    Args:
        string (str): input string
    Returns:
        list: a list of tokens
    """
    return filter(lambda string: string != '', re.split(split_regex, string.lower()))

print simpleTokenize(quickbrownfox) # Should give ['a', 'quick', 'brown', ... ]
stopfile = os.path.join(baseDir, inputPath, STOPWORDS_PATH)
stopwords = set(sc.textFile(stopfile).collect())
print 'These are the stopwords: %s' % stopwords

def tokenize(string):
    """ An implementation of input string tokenization that excludes stopwords
    Args:
        string (str): input string
    Returns:
        list: a list of tokens without stopwords
    """
    return filter(lambda tokens: tokens not in stopwords, simpleTokenize(string))


amazonRecToToken = amazonSmall.map(lambda (k, v): (k, tokenize(v)))
googleRecToToken = googleSmall.map(lambda (k, v): (k, tokenize(v)))

def countTokens(vendorRDD):
    """ Count and return the number of tokens
    Args:
        vendorRDD (RDD of (recordId, tokenizedValue)): Pair tuple of record ID to tokenized output
    Returns:
        count: count of all tokens
    """
    count = 0
    return vendorRDD.flatMap(lambda (id, tokens): tokens).count()

totalTokens = countTokens(amazonRecToToken) + countTokens(googleRecToToken)
print 'There are %s tokens in the combined datasets' % totalTokens


def findBiggestRecord(vendorRDD):
    """ Find and return the record with the largest number of tokens
    Args:
        vendorRDD (RDD of (recordId, tokens)): input Pair Tuple of record ID and tokens
    Returns:
        list: a list of 1 Pair Tuple of record ID and tokens
    """
    return vendorRDD.sortBy(lambda (tid, tokens): -len(tokens)).take(1)

biggestRecordAmazon = findBiggestRecord(amazonRecToToken)
print 'The Amazon record with ID "%s" has the most tokens (%s)' % (biggestRecordAmazon[0][0],
                                                                   len(biggestRecordAmazon[0][1]))


def tf(tokens):
    """ Compute TF
    Args:
        tokens (list of str): input list of tokens from tokenize
    Returns:
        dictionary: a dictionary of tokens to its TF values
    """
    tf_dict = {}
    total_count = len(tokens)
    for word in tokens:
        if word not in tf_dict.keys():
            tf_dict[word] = 1
        else:
            tf_dict[word] += 1
    tf_dict = {key: float(count)/total_count for key, count in tf_dict.items()}
    return tf_dict
    # res = (amazonSmall
    #        .flatMap(lambda (tkid, token_list): list((token, 1) for token in token_list if token in tokens))
    #        .reduceByKey(add).collectAsMap())

googleSmall.sortBy(lambda (tid, tokens): -len(tokens)).take(1)
print tf(tokenize(quickbrownfox)) # Should give { 'quick': 0.1666 ... }


corpusRDD = amazonRecToToken + googleRecToToken
print corpusRDD.take(1)

def idfs(corpus):
    """ Compute IDF
    Args:
        corpus (RDD): input corpus
    Returns:
        RDD: a RDD of (token, IDF value)
    """
    N = corpus.map(lambda (tkid, token_list): tkid).distinct().count()
    uniqueTokens = corpus.flatMap(lambda (tkid, token_list): list(token for token in set(token_list)))
    print uniqueTokens.take(3)
    tokenCountPairTuple = uniqueTokens.map(lambda (token): (token, 1))
    print tokenCountPairTuple.take(3)
    tokenSumPairTuple = tokenCountPairTuple.reduceByKey(add)
    print tokenSumPairTuple.take(3)
    return (tokenSumPairTuple.map(lambda (token, count): (token, float(N)/count)))


idfsSmall = idfs(amazonRecToToken.union(googleRecToToken))
uniqueTokenCount = idfsSmall.count()
print 'There are %s unique tokens in the small datasets.' % uniqueTokenCount


def tfidf(tokens, idfs):
    """ Compute TF-IDF
    Args:
        tokens (list of str): input list of tokens from tokenize
        idfs (dictionary): record to IDF value
    Returns:
        dictionary: a dictionary of records to TF-IDF values
    """
    tfs = tf(tokens)
    tfIdfDict = {k: v*idfs[k] for (k, v) in tfs.items()}
    return tfIdfDict

recb000hkgj8k = amazonRecToToken.filter(lambda x: x[0] == 'b000hkgj8k').collect()[0][1]
idfsSmallWeights = idfsSmall.collectAsMap()
rec_b000hkgj8k_weights = tfidf(recb000hkgj8k, idfsSmallWeights)

print 'Amazon record "b000hkgj8k" has tokens and weights:\n%s' % rec_b000hkgj8k_weights


import math

def dotprod(a, b):
    """ Compute dot product
    Args:
        a (dictionary): first dictionary of record to value
        b (dictionary): second dictionary of record to value
    Returns:
        dotProd: result of the dot product with the two input dictionaries
    """
    return sum([v*b[k] for (k, v) in a.items() if k in b.keys()])

def norm(a):
    """ Compute square root of the dot product
    Args:
        a (dictionary): a dictionary of record to value
    Returns:
        norm: a dictionary of tokens to its TF values
    """
    return math.sqrt(dotprod(a,a))

def cossim(a, b):
    """ Compute cosine similarity
    Args:
        a (dictionary): first dictionary of record to value
        b (dictionary): second dictionary of record to value
    Returns:
        cossim: dot product of two dictionaries divided by the norm of the first dictionary and
                then by the norm of the second dictionary
    """
    dp = dotprod(a,b)
    return (dp / norm(a)) / norm(b)

print amazonSmall.take(2)
print googleSmall.take(2)



