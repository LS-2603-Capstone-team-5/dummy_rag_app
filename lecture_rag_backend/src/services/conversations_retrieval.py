import psycopg2

from src.extensions import DB_CONN_STRING

def retrieve_conversation(uuid):
    conn = psycopg2.connect(DB_CONN_STRING)

    cursor = conn.cursor()

    result = []
    history = []
    summary = ''

    try:
        cursor.execute("""
            SELECT history, summary
            FROM conversations
            WHERE id = %s
        """, (uuid,))
            
        row = cursor.fetchone()
            
        if row:
            history = row[0]
            summary = row[1]
            
    except Exception as e:
        print("Error retrieving history:", e)

    finally:
        cursor.close()
        conn.close()

    return { "history": history, "summary": summary } # the history JSONB value

def retrieve_all_conversations(uuids):
    conversations = {uuid: retrieve_conversation(uuid) for uuid in uuids}
    return conversations # {'aln3450nff': [{role:...}, {content:...}], 'asdfn5243560ng': [{role:...}, {content:...}]}


# test
# print(retrieve_conversation('d32ee5a6-4594-4c3d-aaec-717205c04cf4'))
# print(retrieve_conversation('d33ded99-da56-43e4-9725-de1cc07a362f'))

# print(retrieve_all_conversations(['d32ee5a6-4594-4c3d-aaec-717205c04cf4', 'd33ded99-da56-43e4-9725-de1cc07a362f']))