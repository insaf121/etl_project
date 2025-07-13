import csv
from datetime import datetime
import os
import sys

# Добавляем путь к папке src для импорта модулей
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
from database import get_db_connection
from logger import log_etl_event

def export_f101_to_csv(output_file='f101_report.csv'):
    """Экспорт данных формы 101 в CSV файл"""
    conn = None
    try:
        log_etl_event('DM.DM_F101_ROUND_F', 'EXPORT_START', 0, f"Экспорт в {output_file}")
        print(f" Начало экспорта формы 101...")
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT * FROM dm.dm_f101_round_f
            ORDER BY from_date, chapter, ledger_account
        """)
        
        # Получаем названия колонок
        column_names = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        
        # Создаем папку для результатов, если её нет
        os.makedirs('results', exist_ok=True)
        output_path = os.path.join('results', output_file)
        
        # Записываем в CSV
        with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile, delimiter=';')
            writer.writerow(column_names)
            writer.writerows(rows)
        
        log_etl_event('DM.DM_F101_ROUND_F', 'EXPORT_FINISH', len(rows), 
                      f"Успешно экспортировано {len(rows)} строк")
        print(f" Успешно экспортировано {len(rows)} строк в {output_path}")
        
    except Exception as e:
        log_etl_event('DM.DM_F101_ROUND_F', 'EXPORT_ERROR', 0, str(e))
        print(f" Ошибка экспорта: {e}")
        raise
    finally:
        if conn: conn.close()

def transform_value(value, date_format=None):
    """Преобразует строку в нужный формат (особенно даты и числа)"""
    if not value or not value.strip():
        return None
    
    # Преобразование дат
    if date_format:
        try:
            return datetime.strptime(value, date_format).date()
        except ValueError:
            return None
    
    # Преобразование чисел в научной нотации (например, 0,00E+00)
    if ',' in value and 'E' in value.upper():
        try:
            # Заменяем запятую на точку и преобразуем в float
            return float(value.replace(',', '.'))
        except ValueError:
            return None
    
    # Преобразование обычных чисел с запятой в качестве разделителя
    if ',' in value and value.replace(',', '').isdigit():
        try:
            return float(value.replace(',', '.'))
        except ValueError:
            return None
    
    return value
def import_f101_from_csv(input_file='f101_report_modified.csv', target_table='dm.dm_f101_round_f_v2'):
    """Импорт данных формы 101 из CSV файла"""
    conn = None
    try:
        log_etl_event(target_table, 'IMPORT_START', 0, f"Импорт из {input_file}")
        print(f" Начало импорта формы 101...")
        
        # Проверяем существование файла
        input_path = os.path.join('results', input_file)
        if not os.path.exists(input_path):
            raise FileNotFoundError(f"Файл {input_path} не найден")
        
        # Чтение CSV файла
        with open(input_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f, delimiter=';')
            headers = [col.strip() for col in next(reader)]
            rows = list(reader)
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {target_table} (LIKE dm.dm_f101_round_f INCLUDING ALL)
        """)
        
        # Очищаем таблицу перед загрузкой
        cur.execute(f"TRUNCATE TABLE {target_table}")
        
        # Формируем SQL запрос
        query = f"""
            INSERT INTO {target_table} ({', '.join(headers)})
            VALUES ({', '.join(['%s']*len(headers))})
        """
        
       # Обработка и загрузка данных
        for row in rows:
            processed_row = []
            for col, value in zip(headers, row):
                # Специальная обработка для числовых полей
                if any(field in col.lower() for field in ['balance', 'turn', 'amount']):
                    # Удаляем пробелы, заменяем запятые на точки, обрабатываем научную нотацию
                    value = value.strip().replace(' ', '').replace(',', '.')
                    if 'E' in value.upper():  # Научная нотация
                        try:
                            value = float(value)
                        except ValueError:
                            value = 0.0
                processed_row.append(transform_value(value))
            
            cur.execute(query, processed_row)
        
        conn.commit()
        
        log_etl_event(target_table, 'IMPORT_FINISH', len(rows), 
                      f"Успешно импортировано {len(rows)} строк")
        print(f" Успешно импортировано {len(rows)} строк в {target_table}")
        
    except Exception as e:
        if conn: conn.rollback()
        log_etl_event(target_table, 'IMPORT_ERROR', 0, str(e))
        print(f" Ошибка импорта: {e}")
        raise
    finally:
        if conn: conn.close()

def main():
    """Главная функция с меню выбора операций"""
    while True:
        print("\n" + "="*50)
        print(" Меню работы с формой 101")
        print("1. Экспорт формы 101 в CSV")
        print("2. Импорт формы 101 из CSV")
        print("3. Выход")
        
        choice = input(" Выберите действие (1-3): ")
        
        if choice == "1":
            # Экспорт формы 101
            output_file = input(" Введите имя файла для экспорта (по умолчанию f101_report.csv): ") or "f101_report.csv"
            try:
                export_f101_to_csv(output_file)
            except Exception as e:
                print(f" Ошибка экспорта: {e}")
        
        elif choice == "2":
            # Импорт формы 101
            input_file = input(" Введите имя файла для импорта (по умолчанию f101_report_modified.csv): ") or "f101_report_modified.csv"
            target_table = input(" Введите целевую таблицу (по умолчанию dm.dm_f101_round_f_v2): ") or "dm.dm_f101_round_f_v2"
            try:
                import_f101_from_csv(input_file, target_table)
            except Exception as e:
                print(f" Ошибка импорта: {e}")
        
        elif choice == "3":
            print(" Выход из программы")
            break
        
        else:
            print(" Неверный выбор, попробуйте снова")

if __name__ == "__main__":
    main()