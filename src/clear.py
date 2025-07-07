import psycopg2
from database import get_db_connection

def clear_table(table_name):
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE;")
        conn.commit()
        
        print(f"Таблица {table_name} успешно очищена")
        
    except Exception as e:
        print(f"Ошибка при очистке {table_name}: {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()


clear_table("DS.FT_BALANCE_F")
