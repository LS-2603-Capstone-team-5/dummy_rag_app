import psycopg2

from src.extensions import DB_CONN_STRING

CACHE_SIMILARITY_THRESHOLD = 0.7

def cache_lookup(query_embedding, dialogue_mode):
    conn = psycopg2.connect(DB_CONN_STRING)
    cursor = conn.cursor()

    result = None

    try:
        cursor.execute("""
            SELECT response_text, 1 - (query_embedding_vector::vector <=> %s::vector) AS similarity
            FROM response_cache
            WHERE dialogue_mode = %s
            ORDER BY similarity DESC
            LIMIT 1
        """, (query_embedding, dialogue_mode))

        row = cursor.fetchone()
        if row:
            top_score = round(row[1], 4)
            retrieved = row[1] > CACHE_SIMILARITY_THRESHOLD
            print(
                f"\n\033[96m[CACHE LOOKUP]\033[0m  "
                f"highest similarity: {top_score}  |  "
                f"threshold: {CACHE_SIMILARITY_THRESHOLD}  |  "
                f"result: {'HIT' if retrieved else 'NO RETRIEVAL'}\n"
            )
            if retrieved:
                result = row[0]
        else:
            print(
                f"\n\033[96m[CACHE LOOKUP]\033[0m  "
                f"highest similarity: n/a  |  "
                f"threshold: {CACHE_SIMILARITY_THRESHOLD}  |  "
                f"result: NO RETRIEVAL (cache empty)\n"
            )

    except Exception as e:
        print("Error looking up cache:", e)

    finally:
        cursor.close()
        conn.close()

    return result  # response_text string or None
