import re
import math
import pandas as pd
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from typing import Any
from infrastructure.OrionDBClient import OrionDBClient

# Initialize NLP tools
stop_words = set(stopwords.words('english'))
stemmer = PorterStemmer()
db_config = {
    "dbname": "orion_se",
    "user": "postgres",
    "password": "",
    "host": "localhost",
    "port": "5432"
}

# BM25 Parameters
k1 = 1.5
b = 0.75


def search_query(query: str) -> list[dict]:
    """
    Process the query, compute BM25 scores, and return top 10 most relevant documents.

    Returns:
        A list of dictionaries with keys: url_id, score.
    """
    db_client = OrionDBClient(db_config)
    total_documents = db_client.get_total_document_count()
    avg_doc_length = db_client.get_average_document_length()  # Function to calculate average document length
    query_tokens = __handle_query(query)
    print(f"Expanded Query Tokens: {query_tokens}")
    df = db_client.get_documents_by_terms(query_tokens)

    if df.empty:
        return []

    # Compute document frequency (DF) per term
    df_counts = df.groupby('term')['url_id'].nunique().to_dict()

    # Compute IDF for each term using BM25 formula
    idf = {
        term: math.log((total_documents - df_counts[term] + 0.5) / (df_counts[term] + 0.5) + 1.0)
        for term in df_counts
    }

    # Calculate BM25 scores
    df['bm25'] = df.apply(
        lambda row: calculate_bm25(row, idf, avg_doc_length),
        axis=1
    )

    # Aggregate BM25 by document (url_id)
    doc_scores = df.groupby('url_id')['bm25'].sum().reset_index()
    doc_scores = doc_scores.rename(columns={"bm25": "score"})

    # Sort and return top 10
    top_10 = doc_scores.sort_values(by="score", ascending=False).head(10)

    return top_10.to_dict(orient="records")


def calculate_bm25(row, idf, avg_doc_length):
    """
    Calculate BM25 score for a given row of data.
    """
    term = row['term']
    f_t_d = float(row['term_frequency'])  # Ensure term frequency is a float
    doc_length = float(row['length'])  # Ensure document length is a float
    avg_doc_length = float(avg_doc_length)  # Ensure average document length is a float

    idf_score = idf.get(term, 0)
    tf_score = f_t_d * (k1 + 1) / (f_t_d + k1 * (1 - b + b * (doc_length / avg_doc_length)))

    return idf_score * tf_score


def expand_query(tokens: list[Any]) -> list[Any]:
    return tokens


def __handle_query(query: str) -> list[Any]:
    tokens = clean_and_tokenize(query)
    expanded_tokens = expand_query(tokens)
    stemmed_tokens = stem(expanded_tokens)

    return stemmed_tokens


def clean_and_tokenize(text: str) -> list[Any]:
    """Clean, tokenize, remove stopwords, and stem."""
    # Remove emojis, punctuation, symbols
    cleaned = re.sub(r'[^A-Za-z0-9\s]', '', text)
    cleaned = cleaned.lower()

    # Tokenize
    tokens = re.findall(r'\b\w+\b', cleaned)

    # Remove stopwords (keep digits if length > 1)
    filtered = [
        t for t in tokens
        if t not in stop_words and (not t.isdigit() or len(t) > 1)
    ]
    return filtered


def stem(tokens: list[Any]) -> list[Any]:
    return [stemmer.stem(t) for t in tokens]
