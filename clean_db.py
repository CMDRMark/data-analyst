import os
import sqlite3
import sys

def clean_database(db_name="ecommerce.db"):
    """
    Очищает базу данных: удаляет все таблицы и пересоздает их
    """
    print("🧹 Очистка базы данных...")
    
    try:
        # Проверяем, существует ли файл БД
        if os.path.exists(db_name):
            print(f"📁 Найден файл базы данных: {db_name}")
            
            # Подключаемся к БД
            conn = sqlite3.connect(db_name)
            cursor = conn.cursor()
            
            # Получаем список всех таблиц
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            
            if tables:
                print(f"📋 Найдено таблиц: {len(tables)}")
                for table in tables:
                    print(f"   - {table}")
                
                # Отключаем проверку внешних ключей для удаления
                cursor.execute("PRAGMA foreign_keys=OFF")
                
                # Удаляем все таблицы
                for table in tables:
                    cursor.execute(f"DROP TABLE IF EXISTS {table}")
                    print(f"🗑️  Удалена таблица: {table}")
                
                # Включаем обратно проверку внешних ключей
                cursor.execute("PRAGMA foreign_keys=ON")
                
                conn.commit()
                print("✅ Все таблицы удалены")
            else:
                print("ℹ️  Таблицы не найдены")
            
            conn.close()
            
        else:
            print(f"ℹ️  Файл базы данных {db_name} не найден")
            
    except Exception as e:
        print(f"❌ Ошибка при очистке базы данных: {e}")
        return False
    
    return True


def main():
    """Основная функция"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Очистка базы данных e-commerce')
    parser.add_argument('--db', default='ecommerce.db', help='Имя файла базы данных')
    parser.add_argument('--reset', action='store_true', default=False, help='Полная очистка (удаление файла)')
    
    args = parser.parse_args()
    
    print("=== E-commerce Database Cleaner ===\n")
    
    success = clean_database(args.db)
    
    if success:
        print("\n✅ Очистка завершена успешно!")
        print("Теперь можно запустить 'python main.py' для генерации данных")
    else:
        print("\n❌ Очистка завершена с ошибками")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
