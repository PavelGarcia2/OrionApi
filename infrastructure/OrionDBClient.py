import logging
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from typing import List, Tuple

from entities.page import Page

logging.basicConfig(level=logging.INFO)


class OrionDBClient:
    def __init__(self, db_config: dict):
        """Initialize connection to PostgreSQL."""
        try:
            self.conn = psycopg2.connect(
                dbname=db_config["dbname"],
                user=db_config["user"],
                password=db_config["password"],
                host=db_config["host"],
                port=db_config["port"]
            )
            self.cursor = self.conn.cursor()
        except psycopg2.Error as e:
            logging.error(f"Error connecting to database: {e}")
            raise

    import pandas as pd

    def get_average_document_length(self) -> float:
        """Returns the average document length in the corpus."""
        try:
            self.cursor.execute("SELECT AVG(length) FROM pages")
            result = self.cursor.fetchone()
            return result[0] if result else 0
        except psycopg2.Error as e:
            logging.error(f"Error fetching average document length: {e}")
            return 0

    def get_co_occurring_terms(self, term: str) -> list[str]:
        """
        Retrieve co-occurring terms from the inverted_index for the given term.

        Args:
            term: A term from the search query.

        Returns:
            A list of co-occurring terms.
        """
        try:
            self.cursor.execute("""
                SELECT DISTINCT term 
                FROM inverted_index 
                WHERE url_id IN (
                    SELECT url_id 
                    FROM inverted_index 
                    WHERE term = %s
                ) AND term != %s;
            """, (term, term))

            rows = self.cursor.fetchall()
            return [row[0] for row in rows]

        except psycopg2.Error as e:
            logging.error(f"Database error in get_co_occurring_terms: {e}")
            return []

    def get_documents_by_terms(self, terms: List[str]) -> pd.DataFrame:
        """
        Retrieve entries from the inverted_index for the given list of terms,
        including document length from the pages table.
        """
        if not terms:
            logging.warning("Empty terms list provided to get_documents_by_terms.")
            return pd.DataFrame(columns=["term", "url_id", "term_frequency", "positions", "length"])

        try:
            query = """
                SELECT ii.term, ii.url_id, ii.term_frequency, ii.positions, p.length
                FROM inverted_index ii
                JOIN pages p ON ii.url_id = p.url_id
                WHERE ii.term = ANY(%s);
            """
            self.cursor.execute(query, (terms,))
            results = self.cursor.fetchall()

            return pd.DataFrame(results, columns=["term", "url_id", "term_frequency", "positions", "length"])

        except psycopg2.Error as e:
            logging.error(f"Database error in get_documents_by_terms: {e}")
            self.conn.rollback()
            return pd.DataFrame(columns=["term", "url_id", "term_frequency", "positions", "length"])

    def get_total_document_count(self) -> int:
        self.cursor.execute("SELECT COUNT(*) FROM pages;")
        return self.cursor.fetchone()[0]

    def get_next_pages(self, last_document_id: int, limit: int = 5) -> Tuple[List[Page], int | None]:
        """Get up to `limit` pages after the given last_document_id."""
        try:
            self.cursor.execute("""
                    SELECT url_id, title, summary, content, length, hashed, 
                           is_https, is_mallorca_related, last_crawled
                    FROM pages
                    WHERE url_id > %s
                    ORDER BY url_id ASC
                    LIMIT %s;
                """, (last_document_id, limit))

            rows = self.cursor.fetchall()
            if not rows:
                return [], None  # No more pages

            pages = [Page(*row) for row in rows]
            last_id = pages[-1].url_id  # Last page’s ID

            return pages, last_id

        except psycopg2.Error as e:
            logging.error(f"Database error in get_next_pages: {e}")
            self.conn.rollback()
            return [], None

    def insert_terms(self, url_id: int, term_data: dict[str, list[int]]) -> None:
        """
        Insert or update terms for a document in the inverted index.

        Parameters:
        - url_id: ID of the document/page
        - term_data: dictionary with term -> list of positions
        """
        if not term_data:
            logging.warning(f"No terms to insert for url_id={url_id}. Skipping.")
            return

        insert_query = """
            INSERT INTO inverted_index (term, url_id, term_frequency, positions)
            VALUES %s
            ON CONFLICT (term, url_id) DO UPDATE
            SET term_frequency = EXCLUDED.term_frequency,
                positions = EXCLUDED.positions;
        """

        values = [
            (term, url_id, len(positions), positions)
            for term, positions in term_data.items()
        ]

        try:
            execute_values(self.cursor, insert_query, values)
            self.conn.commit()
            logging.info(f"Inserted/Updated {len(values)} terms for url_id={url_id}")
        except psycopg2.Error as e:
            logging.error(f"Failed to insert terms for url_id={url_id}: {e}")
            self.conn.rollback()







