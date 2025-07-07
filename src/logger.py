from database import get_db_connection

def log_etl_event(table_name, event_type, rows_processed=0, details=None):
    """
    Запись в лог-таблицу с новой структурой
    
    :param table_name: Имя таблицы, с которой работаем (например, 'DS.FT_BALANCE_F')
    :param event_type: 'START', 'FINISH' или 'ERROR'
    :param rows_processed: Количество обработанных строк
    :param details: Дополнительная информация (ошибки, предупреждения)
    """
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO logs.etl_logs 
                (event_type, table_name, rows_processed, details)
                VALUES (%s, %s, %s, %s)
            """, (event_type, table_name, rows_processed, details))
        conn.commit()
    except Exception as e:
        print(f"Ошибка при записи в лог: {e}")
    finally:
        if conn:
            conn.close()