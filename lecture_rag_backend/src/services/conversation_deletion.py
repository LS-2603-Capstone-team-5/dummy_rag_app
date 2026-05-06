import psycopg2

from src.extensions import DB_CONN_STRING

def conversation_deletion(uuid):
    conn = psycopg2.connect(DB_CONN_STRING)

    cursor = conn.cursor()

    try:
        cursor.execute("""
            DELETE FROM conversations
            WHERE id = %s;
            """, (uuid,))
        conn.commit()
    except Exception as e:
        print("Error updating history:", e)

    finally:
        cursor.close()
        conn.close()

    return