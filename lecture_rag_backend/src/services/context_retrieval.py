import psycopg2

from src.extensions import client, DB_CONN_STRING

SIMILARITY_CUTOFF = .50


def get_embedding(query):
    response = client.embeddings.create(
        model="text-embedding-3-small",
        input=query,
    )
    # data is an array, so [0] grabs the first and only element, since we are
    # only sending one string:
    return response.data[0].embedding


def extract_contents(results):
    return [row[1] for row in results]


def retrieve_most_similar(new_embedding):
    conn = psycopg2.connect(DB_CONN_STRING)
    cursor = conn.cursor()

    result = None

    try:
        cursor.execute("""
            SELECT lecture_title, content, 1 - (embedding::vector <=> %s::vector) AS similarity
            FROM data_chunks
            WHERE (1 - (embedding::vector <=> %s::vector)) > %s
            ORDER BY (1 - (embedding::vector <=> %s::vector)) DESC
        """, (new_embedding, new_embedding, SIMILARITY_CUTOFF, new_embedding))

        raw_results = cursor.fetchall()
        result = extract_contents(raw_results)

    except Exception as e:
        print("Error retrieving similar embedding:", e)

    finally:
        cursor.close()
        conn.close()

    return result  # an array of strings
