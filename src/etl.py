import csv
import time
from datetime import datetime
import psycopg2
from database import get_db_connection
from logger import log_etl_event

# Конфигурация таблиц (keys - первичные ключи, date_cols - колонки с датами)
TABLE_CONFIGS = {
    'DS.FT_BALANCE_F': {
        'keys': ['on_date', 'account_rk'],
        'date_cols': {'on_date': '%d.%m.%Y'}
    },
    'DS.FT_POSTING_F': {
        'keys': None,  # Полная перезапись таблицы
        'date_cols': {'oper_date': '%d.%m.%Y'}
    },
    'DS.MD_CURRENCY_D': {
        'keys': ['currency_rk', 'data_actual_date'],
        'date_cols': {'data_actual_date': '%d.%m.%Y'},
        'encoding': 'windows-1251'  # Спец. кодировка для этой таблицы
    },
      'DS.MD_ACCOUNT_D': {
        'keys': ['data_actual_date', 'account_rk'],
        'date_cols': {
            'data_actual_date': '%d.%m.%Y',
            'data_actual_end_date': '%d.%m.%Y'
        }
    },
    'DS.MD_EXCHANGE_RATE_D': {
        'keys': ['data_actual_date', 'currency_rk'],
        'date_cols': {
            'data_actual_date': '%d.%m.%Y',
            'data_actual_end_date': '%d.%m.%Y'
        }
    },
    'DS.MD_LEDGER_ACCOUNT_S': {
        'keys': ['ledger_account', 'start_date'],
        'date_cols': {
            'start_date': '%d.%m.%Y',
            'end_date': '%d.%m.%Y'  # Опционально, если поле есть в CSV
        }
    }
}

def transform_value(value, date_format=None):
    """Преобразует строку в нужный формат (особенно даты)"""
    if not value or not value.strip():
        return None
    if date_format:
        try:
            return datetime.strptime(value, date_format).date()
        except ValueError:
            return None
    return value

def load_table(csv_path, table_name, encoding='utf-8'):
    """Загружает данные из CSV в указанную таблицу"""
    conn = None
    try:
        # 1. Инициализация и логирование
        config = TABLE_CONFIGS[table_name]
        log_etl_event(table_name, "START", 0, f"Начало загрузки {csv_path}")
        print(f" Начало загрузки {table_name}...")
        
        # 2. Добавляем задержку 5 секунд перед началом загрузки
        time.sleep(5)
        
        start_time = time.time()

        # 3. Чтение CSV файла
        with open(csv_path, 'r', encoding=encoding, errors='replace') as f:
            reader = csv.reader(f, delimiter=';')
            headers = [col.strip() for col in next(reader)]
            rows = list(reader)

        # 4. Подключение к БД
        conn = get_db_connection()
        cur = conn.cursor()

        # 5. Очистка таблицы (только для FT_POSTING_F)
        if table_name == "DS.FT_POSTING_F":
            cur.execute(f"TRUNCATE TABLE {table_name}")

        # 6. Формирование SQL запроса
        if config['keys']:
            # UPSERT запрос для таблиц с ключами
            update_cols = [col for col in headers if col not in config['keys']]
            query = f"""
                INSERT INTO {table_name} ({', '.join(headers)})
                VALUES ({', '.join(['%s']*len(headers))})
                ON CONFLICT ({', '.join(config['keys'])})
                DO UPDATE SET {', '.join([f"{col}=EXCLUDED.{col}" for col in update_cols])}
            """
        else:
            # Простой INSERT для таблиц без ключей
            query = f"INSERT INTO {table_name} ({', '.join(headers)}) VALUES ({', '.join(['%s']*len(headers))})"

        # 7. Обработка и загрузка данных
        for row in rows:
            # Преобразование значений (особенно дат)
            processed_row = [
                transform_value(value, config['date_cols'].get(col))
                for col, value in zip(headers, row)
            ]
            
            # Выполнение запроса
            cur.execute(query, processed_row)
        
        conn.commit()
        
        # 8. Логирование результата
        duration = round(time.time() - start_time, 2)
        msg = f"Успешно загружено {len(rows)} строк за {duration} сек"
        log_etl_event(table_name, "FINISH", len(rows), msg)
        print(f" {msg}")

    except Exception as e:
        if conn: conn.rollback()
        log_etl_event(table_name, "ERROR", 0, str(e))
        print(f" Ошибка: {e}")
        raise
    finally:
        if conn: conn.close()

def main():
    """Загружаем все таблицы по очереди"""
    tables = [
        ("data/ft_balance_f.csv", "DS.FT_BALANCE_F"),
        ("data/ft_posting_f.csv", "DS.FT_POSTING_F"),
        ("data/md_currency_d.csv", "DS.MD_CURRENCY_D", "windows-1251"),
        ("data/md_account_d.csv", "DS.MD_ACCOUNT_D"),
        ("data/md_exchange_rate_d.csv", "DS.MD_EXCHANGE_RATE_D"),
        ("data/md_ledger_account_s.csv", "DS.MD_LEDGER_ACCOUNT_S")
    ]
    
    for item in tables:
        try:
            print(f"\n{'='*50}")
            print(f" Загрузка {item[1]} из {item[0]}")
            load_table(*item)  # Распаковываем параметры
        except Exception as e:
            print(f" Прерывание загрузки: {e}")
            break

if __name__ == "__main__":
    main()