
from sklearn.feature_extraction.text import TfidfVectorizer
corpus = ["TRANSFER [5] TRANSFER CHECKSUM MISMATCH Source and destination checksums do not match 6752692d != 8a426f8e",
           "TRANSFER [2] DESTINATION CHECKSUM HTTP 404 : File not found",
           "TRANSFER [70] SOURCE CHECKSUM globus_ftp_client: the server responded with an error 451 451-GlobusErro",
           "TRANSFER [5] TRANSFER CHECKSUM MISMATCH USER_DEFINE and SRC checksums are different. ecdd6369 != a60305af"]
vect = TfidfVectorizer(min_df=1, stop_words="english")
tfidf = vect.fit_transform(corpus)
pairwise_similarity = tfidf * tfidf.T
array_pairwise_similarity = pairwise_similarity.toarray()

import numpy as np

np.fill_diagonal(array_pairwise_similarity, np.nan)

input_doc = "TRANSFER [5] TRANSFER CHECKSUM MISMATCH USER_DEFINE and SRC checksums are different. ecdd6369 != a60305af"
input_idx = corpus.index(input_doc)
result_idx = np.nanargmax(array_pairwise_similarity[input_idx])
print(corpus[result_idx])


import spacy
nlp = spacy.load('en_core_web_sm')
doc1 = nlp(u'Hello hi there!')
doc2 = nlp(u'Hello hi there!')
doc3 = nlp(u'Hey whatsup?')

print(doc1.similarity(doc2) )
print( doc2.similarity(doc3) )# 0.699032527716
print( doc1.similarity(doc3) )