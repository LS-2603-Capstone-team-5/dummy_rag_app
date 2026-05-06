import psycopg2
import json

from src.extensions import DB_CONN_STRING


def replace_history(summary: str, uuid: str) -> None:
    conn = psycopg2.connect(DB_CONN_STRING)

    cursor = conn.cursor()

    try:
        compacted = [{"role": "system", "content": f"Previous conversation summary: {summary}"}]
        cursor.execute("""
            UPDATE conversations
            SET llm_context = %s
            WHERE id = %s;
        """, (json.dumps(compacted), uuid))
        conn.commit()

    except Exception as e:
        print("Error replacing history:", e)

    finally:
        cursor.close()
        conn.close()
