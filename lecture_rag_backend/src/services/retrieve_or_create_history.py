import psycopg2
import json

from src.extensions import DB_CONN_STRING

def retrieve_or_create_history(uuid, query):
    conn = psycopg2.connect(DB_CONN_STRING)

    cursor = conn.cursor()

    result = None

    try:
        if uuid != '':
            cursor.execute("""
                SELECT history, llm_context
                FROM conversations
                WHERE id = %s
            """, (uuid,))

            row = cursor.fetchone()

            if row:
                result = {"history": row[0], "llm_context": row[1]}

        else:
            initial_message = json.dumps([{"role": "user", "content": query}])
            cursor.execute(
                "INSERT INTO conversations (history, llm_context) VALUES (%s, %s) RETURNING id;",
                (initial_message, initial_message)
            )
            result = cursor.fetchone()[0]
            conn.commit()

    except Exception as e:
        print("Error retrieving history:", e)

    finally:
        cursor.close()
        conn.close()

    return result  # dict {"history": ..., "llm_context": ...} for existing, or UUID string for new
