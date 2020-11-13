
"""
from sklearn.feature_extraction.text import TfidfVectorizer
corpus = ["[5] TRANSFER CHECKSUM MISMATCH Source and destination checksums do not match 6752692d != 8a426f8e",
           "[2] DESTINATION CHECKSUM HTTP 404 : File not found",
           "[70] SOURCE CHECKSUM globus_ftp_client: the server responded with an error 451 451-GlobusErro",
           "[5] TRANSFER CHECKSUM MISMATCH USER_DEFINE and SRC checksums are different. ecdd6369 != a60305af",
          "[5] TRANSFER CHECKSUM MISMATCH Source and destination checksums do not match 2434234 != 432423442",
          "[110] TRANSFER  Operation timed out"]
import re

def remove_special_characters(text):
    # define the pattern to keep
     pat = r'[^a-zA-z0-9.,/:;\"\'\s]'
     return re.sub(pat, '', text)

corpus = [remove_special_characters(sentence) for sentence in corpus]
vect = TfidfVectorizer(min_df=0.2, stop_words="english")
tfidf = vect.fit_transform(corpus)
pairwise_similarity = tfidf * tfidf.T
array_pairwise_similarity = pairwise_similarity.toarray()
"""
# import numpy as np
#
# np.fill_diagonal(array_pairwise_similarity, np.nan)

# input_doc = "[5] TRANSFER CHECKSUM MISMATCH USER_DEFINE and SRC checksums are different. ecdd6369 != a60305af"
# input_idx = corpus.index(input_doc)
# result_idx = np.nanargmax(array_pairwise_similarity[input_idx])
# print(corpus[result_idx])

import spacy
# python -m spacy download en_core_web_lg
import time

time1 = time.time()
nlp = spacy.load("en_core_web_lg")
time2 = time.time()
print(time.time() - time1)
text = "TRANSFER [28] TRANSFER  globus_ftp_client: the server responded with an error 500 Command failed. : open/create : [ERROR] Server responded with an error: [3021] Unable to get quota space - quota not defined or exhausted /eos/cms/store/user/ddirmait/ParkingBPH2/CRAB3_tutorial_May2015_MC_analysis/201026_153757/0000/output_18.root; Disk quota exceeded e"
text_1 = "TRANSFER  Operation timed out"
# passing the text into nlp
doc = nlp(text)
doc1 = nlp(text_1) # Finding the similarity
score = doc.similarity(doc1)
print(score, time.time() - time2)