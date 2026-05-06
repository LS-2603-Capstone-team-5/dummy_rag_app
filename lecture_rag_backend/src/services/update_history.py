import psycopg2
import json

from src.extensions import DB_CONN_STRING

def update_history(role, content, uuid):
    conn = psycopg2.connect(DB_CONN_STRING)

    cursor = conn.cursor()

    try:
        message = json.dumps([{"role": role, "content": content}])
        cursor.execute("""
            UPDATE conversations
            SET history = history || %s,
                llm_context = llm_context || %s
            WHERE id = %s;
            """, (message, message, uuid))
        conn.commit()
    except Exception as e:
        print("Error updating history:", e)

    finally:
        cursor.close()
        conn.close()

    return
