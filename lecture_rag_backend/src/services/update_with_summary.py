import psycopg2

from src.extensions import DB_CONN_STRING

def update_with_summary(summary, uuid):
  conn = psycopg2.connect(DB_CONN_STRING)

  cursor = conn.cursor()

  try:
      cursor.execute("""
          UPDATE conversations
          SET summary = %s
          WHERE id = %s;
          """, (summary, uuid))
      conn.commit()

  except Exception as e:
      print("Error updating history:", e)

  finally:
      cursor.close()
      conn.close()

  return