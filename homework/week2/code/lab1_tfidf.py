from sklearn.feature_extraction.text import TfidfVectorizer
import pandas as pd

docs = [
    "big data can duoc xu ly phan tan",
    "spark xu ly du lieu lon theo lo",
    "kafka xu ly du lieu thoi gian thuc",
]
vectorizer = TfidfVectorizer()
X = vectorizer.fit_transform(docs)
df = pd.DataFrame(X.toarray(), columns=vectorizer.get_feature_names_out())
print(df.round(3))
