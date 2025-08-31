import sqlite3
import os


class DatabaseTester:
    def __init__(self, db_name="ecommerce.db"):
        self.db_name = db_name
        self.conn = None
        self.cursor = None
    
    def connect_to_database(self):
        """Подключение к существующей базе данных"""
        try:
            if not os.path.exists(self.db_name):
                print(f"❌ Файл базы данных {self.db_name} не найден")
                print("Сначала создайте базу данных и таблицы")
                return False
            
            self.conn = sqlite3.connect(self.db_name)
            self.cursor = self.conn.cursor()
            print(f"✅ Подключение к базе данных {self.db_name} установлено")
            return True
            
        except Exception as e:
            print(f"❌ Ошибка при подключении к базе данных: {e}")
            return False
    
    def test_tables_exist(self):
        """Тест 1: Проверка существования всех таблиц"""
        print("\n=== Тест 1: Проверка таблиц ===")
        
        try:
            # Проверяем существование всех таблиц
            expected_tables = ['products', 'users', 'transactions']
            existing_tables = []
            
            self.cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name IN ('products', 'users', 'transactions')
            """)
            
            for row in self.cursor.fetchall():
                existing_tables.append(row[0])
            
            # Проверяем, что все ожидаемые таблицы существуют
            missing_tables = set(expected_tables) - set(existing_tables)
            
            if not missing_tables:
                print("✅ Все таблицы созданы успешно")
                for table in existing_tables:
                    print(f"   - {table}")
                return True
            else:
                print(f"❌ Отсутствуют таблицы: {missing_tables}")
                return False
                
        except Exception as e:
            print(f"❌ Ошибка при проверке таблиц: {e}")
            return False
    
    def test_table_schemas(self):
        """Тест 2: Проверка схемы таблиц"""
        print("\n=== Тест 2: Проверка схемы таблиц ===")
        
        try:
            # Ожидаемые схемы таблиц
            expected_schemas = {
                'products': ['product_id', 'name', 'production_cost', 'retail_cost', 'tier'],
                'users': ['user_id', 'name', 'age', 'geo', 'discount_tier'],
                'transactions': ['transaction_id', 'product_id', 'user_id', 'transaction_date', 
                               'initial_cost', 'applied_discount', 'final_cost']
            }
            
            all_correct = True
            
            for table_name, expected_columns in expected_schemas.items():
                self.cursor.execute(f"PRAGMA table_info({table_name})")
                actual_columns = [row[1] for row in self.cursor.fetchall()]
                
                missing_columns = set(expected_columns) - set(actual_columns)
                extra_columns = set(actual_columns) - set(expected_columns)
                
                if not missing_columns and not extra_columns:
                    print(f"✅ Таблица {table_name}: схема корректна")
                else:
                    print(f"❌ Таблица {table_name}:")
                    if missing_columns:
                        print(f"   Отсутствуют колонки: {missing_columns}")
                    if extra_columns:
                        print(f"   Лишние колонки: {extra_columns}")
                    all_correct = False
            
            return all_correct
            
        except Exception as e:
            print(f"❌ Ошибка при проверке схемы: {e}")
            return False
    
    def test_foreign_keys(self):
        """Тест 3: Проверка внешних ключей"""
        print("\n=== Тест 3: Проверка внешних ключей ===")
        
        try:
            # Проверяем, что внешние ключи настроены
            self.cursor.execute("PRAGMA foreign_key_list(transactions)")
            foreign_keys = self.cursor.fetchall()
            
            if foreign_keys:
                print("✅ Внешние ключи настроены:")
                for fk in foreign_keys:
                    print(f"   - {fk[3]} -> {fk[4]}")
                return True
            else:
                print("❌ Внешние ключи не настроены")
                return False
                
        except Exception as e:
            print(f"❌ Ошибка при проверке внешних ключей: {e}")
            return False
    
    def test_data_types(self):
        """Тест 4: Проверка типов данных"""
        print("\n=== Тест 4: Проверка типов данных ===")
        
        try:
            # Ожидаемые типы данных для ключевых колонок
            expected_types = {
                'products': {'product_id': 'INTEGER', 'name': 'TEXT', 'production_cost': 'REAL', 'retail_cost': 'REAL', 'tier': 'INTEGER'},
                'users': {'user_id': 'INTEGER', 'name': 'TEXT', 'age': 'INTEGER', 'geo': 'TEXT', 'discount_tier': 'INTEGER'},
                'transactions': {'transaction_id': 'INTEGER', 'product_id': 'INTEGER', 'user_id': 'INTEGER', 
                               'transaction_date': 'DATE', 'initial_cost': 'REAL', 'applied_discount': 'REAL', 'final_cost': 'REAL'}
            }
            
            all_correct = True
            
            for table_name, expected_column_types in expected_types.items():
                self.cursor.execute(f"PRAGMA table_info({table_name})")
                table_info = self.cursor.fetchall()
                
                for row in table_info:
                    column_name = row[1]
                    column_type = row[2]
                    
                    if column_name in expected_column_types:
                        expected_type = expected_column_types[column_name]
                        if column_type.upper() != expected_type.upper():
                            print(f"❌ Таблица {table_name}, колонка {column_name}: ожидался тип {expected_type}, получен {column_type}")
                            all_correct = False
                
                if all_correct:
                    print(f"✅ Таблица {table_name}: типы данных корректны")
            
            return all_correct
            
        except Exception as e:
            print(f"❌ Ошибка при проверке типов данных: {e}")
            return False
    
    def run_all_tests(self):
        """Запуск всех тестов"""
        print(" Запуск тестов базы данных...\n")
        
        # Сначала подключаемся к БД
        if not self.connect_to_database():
            return False
        
        tests = [
            self.test_tables_exist,
            self.test_table_schemas,
            self.test_foreign_keys,
            self.test_data_types
        ]
        
        passed = 0
        total = len(tests)
        
        for test in tests:
            if test():
                passed += 1
        
        print(f"\n Результаты тестов: {passed}/{total} пройдено")
        
        if passed == total:
            print("✅ Все тесты пройдены! База данных создана корректно.")
        else:
            print("⚠️  Некоторые тесты не пройдены. Проверьте создание базы данных.")
        
        return passed == total
    
    def cleanup(self):
        """Очистка ресурсов"""
        if self.conn:
            self.conn.close()

def main():
    """Основная функция для запуска тестов"""
    tester = DatabaseTester()
    
    try:
        success = tester.run_all_tests()
        return 0 if success else 1
    finally:
        tester.cleanup()

if __name__ == "__main__":
    exit(main())
