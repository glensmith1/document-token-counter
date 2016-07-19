#! document-token-counter/Cl    eanDocuments.PYsudo apt-get install sublime-text-installer

''' External libraries and programs '''
import os
import glob
import time
import sqlite3
import argparse
from psutil import virtual_memory
from collections import Counter
from nltk.tokenize import TweetTokenizer
from nltk.util import ngrams

def getArgs():
    ''' Command line parser '''
    # Defaults for command line
    MEMUSE = 65
    PATTERN = "input/en_US.*.txt"
    DB = "data/swiftkey.db"
    NGRAM = 1
    # Parse the command line
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-db', '--DATABASE', type=str, help='Database for storage',
        required=False, default=DB)        
    parser.add_argument(
        '-n', '--NGRAM', type=int, help='Ngram size',
        required=False, default=NGRAM)
    parser.add_argument(
        '-p', '--PATTERN', type=str, help='Pattern for input',
        required=False, default=PATTERN)
    parser.add_argument(
        '-m', '--MEMUSE', type=int, help='Memory to use',
        required=False, default=MEMUSE)
    args = parser.parse_args()
        
    return args.DATABASE, args.NGRAM, args.PATTERN, args.MEMUSE

def parseLine(doc: str, ngram: int):
    ''' Yield bag of ngrams to calling function '''
    tknzr = TweetTokenizer(strip_handles=True, reduce_len=True)
    tokens = (token for token in tknzr.tokenize(doc) if len(token) > 1)
    return [" ".join(token) for token in ngrams(tokens, ngram)]

def loadBagsToSQL():
    ''' Count bag of tokens at ngram level to datastore '''
    start = time.time()
    db, ngram, pattern, maxmem = getArgs()
    print("Tokenize at ngram = {}".format(ngram))
    with sqlite3.connect(db, isolation_level="IMMEDIATE") as c:                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
        # Init
        tableName = "ngram{}".format(ngram)
        mainDict = Counter()
        def flushCounter():
            ''' Flush token counts '''
            c.executemany("INSERT INTO temp VALUES (?, ?)", 
                          [(k, v) for k, v in mainDict.items()])
            mainDict.clear()
        # Count  
        c.execute("CREATE TEMPORARY TABLE temp (ngram TEXT, freq INTEGER)")
        for infile in glob.glob(pattern):
            with open(infile, 'r', encoding="utf-8") as f:
                for i, doc in enumerate(f):
                    print("\rFile {0}, line {1}".format(infile, i+1), end=" ")
                    for token in parseLine(doc, ngram):mainDict[token] += 1
                    if (virtual_memory().percent > maxmem):flushCounter()
            print()                
        flushCounter()
        # Sum
        sql = '''DROP TABLE IF EXISTS {0};
                 CREATE TABLE {0} AS
                       SELECT ngram, sum(freq) AS "freq"
                         FROM temp
                     GROUP BY ngram;
                 CREATE INDEX {0}_idx ON {0} (ngram, freq);'''.format(tableName)
        c.executescript(sql)
    print ("Load time {}".format(time.time()-start)) 
    print ("Output at {}".format(db))
        
if __name__ == "__main__":
    loadBagsToSQL()