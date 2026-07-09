"""
Extractor
---------
Cursor-based streaming reader from PostgreSQL.
Handles large tables without memory issues by using server-side cursors.
Supports chunked extraction by PK range for parallel loading.
"""
import psycopg2
import psycopg2.extras
from utils.logger import get_logger

log = get_logger(__name__)


class Extractor:
    def __init__(self, pg_url: str, batch_size: int = 5000):
        self.pg_url = pg_url
        self.batch_size = batch_size
        self.conn = None

    def connect(self):
        self.conn = psycopg2.connect(self.pg_url)
        self.conn.set_session(readonly=True, autocommit=True)

    def close(self):
        if self.conn:
            self.conn.close()

    def get_pk_range(self, table: str, pk: str) -> tuple:
        """Get min/max PK for chunking strategy."""
        with self.conn.cursor() as cur:
            cur.execute(f'SELECT MIN("{pk}"), MAX("{pk}") FROM "{table}"')
            return cur.fetchone()

    def get_row_count(self, table: str) -> int:
        with self.conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{table}"')
            return cur.fetchone()[0]

    def stream_table(self, table: str, columns: list[str] = None,
                     where: str = None, params: tuple = None):
        """
        Generator: streams rows from a table using a server-side cursor.
        Yields batches of `batch_size` rows as list[dict].

        This is memory-safe for 50M+ row tables.
        """
        col_clause = ", ".join(f'"{c}"' for c in columns) if columns else "*"
        query = f'SELECT {col_clause} FROM "{table}"'
        if where:
            query += f" WHERE {where}"

        cursor_name = f"cursor_{table}_{id(self)}"
        with self.conn.cursor(name=cursor_name,
                              cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.itersize = self.batch_size
            cur.execute(query, params)

            while True:
                rows = cur.fetchmany(self.batch_size)
                if not rows:
                    break
                yield [dict(r) for r in rows]

    def stream_chunk(self, table: str, pk: str, pk_min, pk_max,
                     columns: list[str] = None):
        """
        Stream a specific PK range chunk of a table.
        Used for parallel loading: each worker gets a chunk.
        """
        col_clause = ", ".join(f'"{c}"' for c in columns) if columns else "*"
        query = (
            f'SELECT {col_clause} FROM "{table}" '
            f'WHERE "{pk}" >= %s AND "{pk}" < %s '
            f'ORDER BY "{pk}"'
        )

        cursor_name = f"cursor_chunk_{table}_{pk_min}_{id(self)}"
        with self.conn.cursor(name=cursor_name,
                              cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.itersize = self.batch_size
            cur.execute(query, (pk_min, pk_max))

            while True:
                rows = cur.fetchmany(self.batch_size)
                if not rows:
                    break
                yield [dict(r) for r in rows]

    def get_pk_chunks(self, table: str, pk: str,
                      chunk_size: int = 100_000) -> list[tuple]:
        """
        Divide table into (min, max) PK range chunks for parallel processing.
        Returns list of (chunk_min, chunk_max) tuples.
        """
        pk_min, pk_max = self.get_pk_range(table, pk)
        if pk_min is None:
            return []

        chunks = []
        current = pk_min
        while current <= pk_max:
            chunk_end = current + chunk_size
            chunks.append((current, min(chunk_end, pk_max + 1)))
            current = chunk_end

        log.info(f"Table [cyan]{table}[/cyan]: {len(chunks)} chunks "
                 f"(range {pk_min}–{pk_max}, chunk_size={chunk_size:,})")
        return chunks

    def stream_since_watermark(self, table: str, watermark_col: str,
                               last_value, columns: list[str] = None):
        """
        CDC watermark-based streaming: fetch rows updated since last sync.
        Used for incremental sync.
        """
        yield from self.stream_table(
            table,
            columns=columns,
            where=f'"{watermark_col}" > %s ORDER BY "{watermark_col}"',
            params=(last_value,),
        )
