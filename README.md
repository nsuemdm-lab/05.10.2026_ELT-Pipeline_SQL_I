### МЕТОДИЧЕСКОЕ ПОСОБИЕ: РАЗВЕРТЫВАНИЕ, УПРАВЛЕНИЕ И ВОССТАНОВЛЕНИЕ АНАЛИТИЧЕСКОГО ХРАНИЛИЩА ДАННЫХ (DATA WAREHOUSE) В СРЕДЕ GOOGLE COLAB

**Цель:** Формирование инженерных компетенций по проектированию реляционных баз данных, разработке ELT-пайплайнов, обеспечению идемпотентности операций загрузки и реализации механизмов Disaster Recovery в эфемерных вычислительных средах.

**Технологический стек:** Ubuntu (Colab Environment), PostgreSQL 14+, Python 3, SQLAlchemy, Pandas, Psycopg2.
**Архитектурный паттерн:** Star Schema (Схема «Звезда»), Entity-Attribute-Value (EAV).

---

#### ЭТАП 1: INFRASTRUCTURE PROVISIONING (РАЗВЕРТЫВАНИЕ ИНФРАСТРУКТУРЫ)
Инициализация локального кластера PostgreSQL внутри виртуальной машины Google Colab и установка необходимых Python-драйверов.

*Инструкция: Выполнить в ячейке типа Code.*
```bash
# Обновление индексов пакетов и установка PostgreSQL
!apt-get update -yqq
!apt-get install postgresql postgresql-contrib -yqq

# Запуск системного демона PostgreSQL
!service postgresql start

# Создание суперпользователя и базы данных для Data Warehouse
!sudo -u postgres psql -c "CREATE USER dwh_admin WITH SUPERUSER PASSWORD 'admin123';"
!sudo -u postgres psql -c "CREATE DATABASE elt_dwh OWNER dwh_admin;"

# Установка зависимостей для Data Engineering
!pip install psycopg2-binary sqlalchemy pandas -q
```

---

#### ЭТАП 2: DATA MODELING (ПРОЕКТИРОВАНИЕ DDL-СХЕМЫ)
Создание таблиц измерений (Dimensions) и фактов (Facts). Реализация суррогатных ключей (Surrogate Keys) для изоляции хранилища от изменений в системах-источниках.

*Инструкция: Выполнить в новой ячейке Python.*
```python
import pandas as pd
from sqlalchemy import create_engine, text

# Инициализация подключения к локальному инстансу PostgreSQL
DB_URI = 'postgresql://dwh_admin:admin123@localhost:5432/elt_dwh'
engine = create_engine(DB_URI)

# DDL-спецификация аналитического хранилища
ddl_script = """
DROP TABLE IF EXISTS fact_financial_metrics CASCADE;
DROP TABLE IF EXISTS dim_company CASCADE;
DROP TABLE IF EXISTS dim_metric_dictionary CASCADE;

-- ИЗМЕРЕНИЕ: КОМПАНИИ (Master Data Management)
CREATE TABLE dim_company (
    company_sk SERIAL PRIMARY KEY,
    ticker VARCHAR(10) UNIQUE NOT NULL,
    company_name VARCHAR(255) NOT NULL,
    gics_sector VARCHAR(100)
);

-- ИЗМЕРЕНИЕ: СПРАВОЧНИК МЕТРИК (Metadata Management)
CREATE TABLE dim_metric_dictionary (
    metric_id SERIAL PRIMARY KEY,
    metric_code VARCHAR(50) UNIQUE NOT NULL,
    metric_type VARCHAR(50) NOT NULL
);

-- ФАКТ: ФИНАНСОВЫЕ ПОКАЗАТЕЛИ (EAV-модель)
CREATE TABLE fact_financial_metrics (
    fact_id SERIAL PRIMARY KEY,
    report_date DATE NOT NULL,
    company_sk INT REFERENCES dim_company(company_sk),
    metric_id INT REFERENCES dim_metric_dictionary(metric_id),
    metric_value NUMERIC(20, 4) NOT NULL
);

CREATE INDEX idx_fact_fin_metrics ON fact_financial_metrics(company_sk, metric_id, report_date);
"""

with engine.begin() as conn:
    conn.execute(text(ddl_script))
    print("СТАТУС: DDL-скрипты успешно выполнены. Структура Data Warehouse создана.")
```

---

#### ЭТАП 3: INITIAL DATA SEEDING (ЗАГРУЗКА MOCK-ДАННЫХ)
Первичное наполнение хранилища с использованием идемпотентных SQL-запросов (`ON CONFLICT DO NOTHING`, `NOT EXISTS`).

*Инструкция: Выполнить в новой ячейке Python.*
```python
dml_script = """
-- Заполнение измерения dim_company
INSERT INTO dim_company (ticker, company_name, gics_sector) VALUES
('SBER', 'Sberbank', 'Financials'),
('GAZP', 'Gazprom', 'Energy'),
('YNDX', 'Yandex', 'Information Technology')
ON CONFLICT (ticker) DO NOTHING;

-- Заполнение измерения dim_metric_dictionary
INSERT INTO dim_metric_dictionary (metric_code, metric_type) VALUES
('EBITDA', 'FINANCIAL'),
('NET_PROFIT', 'FINANCIAL'),
('MARKET_CAP', 'MARKET')
ON CONFLICT (metric_code) DO NOTHING;

-- Заполнение таблицы фактов (динамическое разрешение суррогатных ключей)
INSERT INTO fact_financial_metrics (report_date, company_sk, metric_id, metric_value)
SELECT '2023-12-31', c.company_sk, m.metric_id, 1500000.00
FROM dim_company c, dim_metric_dictionary m
WHERE c.ticker = 'SBER' AND m.metric_code = 'EBITDA'
AND NOT EXISTS (SELECT 1 FROM fact_financial_metrics WHERE report_date = '2023-12-31' AND company_sk = c.company_sk AND metric_id = m.metric_id);

INSERT INTO fact_financial_metrics (report_date, company_sk, metric_id, metric_value)
SELECT '2023-12-31', c.company_sk, m.metric_id, 1400000.00
FROM dim_company c, dim_metric_dictionary m
WHERE c.ticker = 'SBER' AND m.metric_code = 'NET_PROFIT'
AND NOT EXISTS (SELECT 1 FROM fact_financial_metrics WHERE report_date = '2023-12-31' AND company_sk = c.company_sk AND metric_id = m.metric_id);
"""

with engine.begin() as conn:
    conn.execute(text(dml_script))
    print("СТАТУС: Mock-данные успешно загружены.")
```

---

#### ЭТАП 4: INTERACTIVE DATA INGESTION (ИНТЕРАКТИВНЫЙ ИМПОРТ ДАННЫХ)
Модуль для загрузки внешних датасетов (CSV/Excel) через UI Colab. Включает Data Validation, Data Cleaning и транзакционную запись (UPSERT).

*Инструкция: Выполнить в новой ячейке Python. Потребуется загрузить файл.*
```python
import io
from google.colab import files

print("Ожидаемый формат колонок: report_date, ticker, company_name, gics_sector, metric_code, metric_type, metric_value")
uploaded = files.upload()

if uploaded:
    file_name = list(uploaded.keys())[0]
    file_content = uploaded[file_name]

    try:
        # Data Parsing
        if file_name.endswith('.csv'):
            df_new_data = pd.read_csv(io.BytesIO(file_content), sep=None, engine='python')
        else:
            df_new_data = pd.read_excel(io.BytesIO(file_content))

        # Data Cleaning & Validation
        df_new_data.columns = [col.strip().lower() for col in df_new_data.columns]
        required_cols = {'report_date', 'ticker', 'company_name', 'metric_code', 'metric_value'}
        
        if not required_cols.issubset(set(df_new_data.columns)):
            raise ValueError(f"Отсутствуют обязательные колонки: {required_cols - set(df_new_data.columns)}")

        df_new_data['gics_sector'] = df_new_data.get('gics_sector', 'Unknown').fillna('Unknown')
        df_new_data['metric_type'] = df_new_data.get('metric_type', 'FINANCIAL').fillna('FINANCIAL')

        # Transactional Data Load
        with engine.begin() as conn:
            # UPSERT для dim_company
            companies = df_new_data[['ticker', 'company_name', 'gics_sector']].drop_duplicates()
            for _, row in companies.iterrows():
                conn.execute(text("""
                    INSERT INTO dim_company (ticker, company_name, gics_sector)
                    VALUES (:ticker, :company_name, :gics_sector)
                    ON CONFLICT (ticker) DO UPDATE
                    SET company_name = EXCLUDED.company_name, gics_sector = EXCLUDED.gics_sector;
                """), {"ticker": str(row['ticker']).strip().upper(), "company_name": str(row['company_name']).strip(), "gics_sector": str(row['gics_sector']).strip()})

            # UPSERT для dim_metric_dictionary
            metrics = df_new_data[['metric_code', 'metric_type']].drop_duplicates()
            for _, row in metrics.iterrows():
                conn.execute(text("""
                    INSERT INTO dim_metric_dictionary (metric_code, metric_type)
                    VALUES (:metric_code, :metric_type)
                    ON CONFLICT (metric_code) DO NOTHING;
                """), {"metric_code": str(row['metric_code']).strip().upper(), "metric_type": str(row['metric_type']).strip().upper()})

            # INSERT для fact_financial_metrics
            facts_inserted = 0
            for _, row in df_new_data.iterrows():
                rep_date = pd.to_datetime(row['report_date']).strftime('%Y-%m-%d')
                result = conn.execute(text("""
                    INSERT INTO fact_financial_metrics (report_date, company_sk, metric_id, metric_value)
                    SELECT :report_date, c.company_sk, m.metric_id, :metric_value
                    FROM dim_company c, dim_metric_dictionary m
                    WHERE c.ticker = :ticker AND m.metric_code = :metric_code
                    AND NOT EXISTS (
                        SELECT 1 FROM fact_financial_metrics
                        WHERE report_date = :report_date AND company_sk = c.company_sk AND metric_id = m.metric_id
                    );
                """), {"report_date": rep_date, "ticker": str(row['ticker']).strip().upper(), "metric_code": str(row['metric_code']).strip().upper(), "metric_value": float(row['metric_value'])})
                facts_inserted += result.rowcount

            print(f"СТАТУС: Импорт завершен. Добавлено новых фактов: {facts_inserted}")

    except Exception as e:
        print(f"[ОШИБКА ИМПОРТА]: {e}")
```

---

#### ЭТАП 5: DATA EXTRACTION & TRANSFORMATION (ИЗВЛЕЧЕНИЕ И ТРАНСФОРМАЦИЯ)
Денормализация данных через SQL JOIN и трансформация структуры (Pivot) средствами Pandas для подготовки к Exploratory Data Analysis (EDA).

*Инструкция: Выполнить в новой ячейке Python.*
```python
sql_query = """
SELECT f.report_date, c.ticker, c.gics_sector, m.metric_code, f.metric_value
FROM fact_financial_metrics f
JOIN dim_company c ON f.company_sk = c.company_sk
JOIN dim_metric_dictionary m ON f.metric_id = m.metric_id
ORDER BY c.ticker, m.metric_code;
"""

engine.dispose() # Сброс пула соединений
df_analytics = pd.read_sql_query(sql_query, engine)

print("РЕЗУЛЬТАТЫ АНАЛИТИЧЕСКОГО ЗАПРОСА (DATA EXTRACTION):")
display(df_analytics.head())

df_pivot = df_analytics.pivot(
    index=['report_date', 'ticker', 'gics_sector'],
    columns='metric_code',
    values='metric_value'
).reset_index()

print("\nДАННЫЕ ПОСЛЕ ТРАНСФОРМАЦИИ (PIVOT TABLE):")
display(df_pivot.head())
```

---

#### ЭТАП 6: DATA PERSISTENCE (РЕЗЕРВНОЕ КОПИРОВАНИЕ)
Создание логического дампа базы данных на локальный диск виртуальной машины.

*Инструкция: Выполнить в новой ячейке Python.*
```python
import os

LOCAL_BACKUP_PATH = '/content/elt_dwh_backup.sql'

# Выполнение логического бэкапа через утилиту pg_dump
!PGPASSWORD='admin123' pg_dump -h localhost -U dwh_admin -c elt_dwh > {LOCAL_BACKUP_PATH}

if os.path.exists(LOCAL_BACKUP_PATH) and os.path.getsize(LOCAL_BACKUP_PATH) > 0:
    print(f"СТАТУС: Локальный бэкап успешно создан: {LOCAL_BACKUP_PATH}")
else:
    print("КРИТИЧЕСКАЯ ОШИБКА: Не удалось создать файл бэкапа.")
```

---

#### ЭТАП 7: DISASTER RECOVERY & STATE RECONCILIATION (ВОССТАНОВЛЕНИЕ И СВЕРКА СОСТОЯНИЙ)
Автоматизированный процесс проверки целостности БД. Скрипт сравнивает текущее состояние DWH с эталонным бэкапом (путем нормализации SQL-строк). При обнаружении дельты (Data Drift) выполняется принудительный откат к стабильной версии.

*Инструкция: Выполнить в новой ячейке Python.*
```python
import os
import subprocess
from datetime import datetime

LOCAL_BACKUP_PATH = '/content/elt_dwh_backup.sql'
TEMP_COMPARE_PATH = '/content/elt_dwh_temp_compare.sql'

if not os.path.exists(LOCAL_BACKUP_PATH):
    raise FileNotFoundError(f"КРИТИЧЕСКАЯ ОШИБКА: Файл бэкапа не найден: {LOCAL_BACKUP_PATH}")

# Проверка существования БД
res = subprocess.run(["sudo", "-u", "postgres", "psql", "-tAc", "SELECT 1 FROM pg_database WHERE datname='elt_dwh';"], capture_output=True, text=True)
db_exists = res.stdout.strip() == "1"
perform_restore = False

if db_exists:
    # Генерация временного дампа для сверки
    subprocess.run(f"PGPASSWORD='admin123' pg_dump -h localhost -U dwh_admin -c elt_dwh > {TEMP_COMPARE_PATH}", shell=True)

    def get_sorted_clean_sql(filepath):
        with open(filepath, 'r', encoding='utf-8') as f:
            lines = []
            for line in f:
                line_clean = line.strip()
                if not line_clean or line_clean.startswith('--') or line_clean.startswith('\\') or 'restrict' in line_clean or line_clean.startswith('SET ') or 'set_config' in line_clean or 'setval' in line_clean.lower():
                    continue
                lines.append(line_clean.replace('"', '').lower())
        return sorted(lines)

    backup_data = get_sorted_clean_sql(LOCAL_BACKUP_PATH)
    current_data = get_sorted_clean_sql(TEMP_COMPARE_PATH)
    
    if os.path.exists(TEMP_COMPARE_PATH): os.remove(TEMP_COMPARE_PATH)

    if backup_data == current_data:
        print("СТАТУС: Изменений не обнаружено. Текущая БД идентична бэкапу.")
    else:
        print("ОБНАРУЖЕНЫ РАЗЛИЧИЯ (Data Drift). Требуется восстановление.")
        perform_restore = True
else:
    print("База данных не найдена. Требуется развертывание.")
    perform_restore = True

if perform_restore:
    if db_exists:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        pre_restore_path = f"/content/elt_dwh_backup_{timestamp}.sql"
        subprocess.run(f"PGPASSWORD='admin123' pg_dump -h localhost -U dwh_admin -c elt_dwh > {pre_restore_path}", shell=True)
        print(f"Создан предварительный бэкап измененной БД: {pre_restore_path}")

    # Терминация сессий и пересоздание БД
    recreate_db_sql = """
    SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE pg_stat_activity.datname = 'elt_dwh' AND pid <> pg_backend_pid();
    DROP DATABASE IF EXISTS elt_dwh;
    CREATE DATABASE elt_dwh OWNER dwh_admin;
    """
    with open('/content/recreate_db.sql', 'w') as f: f.write(recreate_db_sql)
    subprocess.run("sudo -u postgres psql -f /content/recreate_db.sql > /dev/null", shell=True)

    # Восстановление из бэкапа
    res_restore = subprocess.run(f"PGPASSWORD='admin123' psql -h localhost -U dwh_admin -d elt_dwh < {LOCAL_BACKUP_PATH}", shell=True, capture_output=True, text=True)
    
    filtered_errors = [line for line in res_restore.stderr.split('\n') if line and "does not exist" not in line and "NOTICE:" not in line]
    if filtered_errors:
        print("ПРЕДУПРЕЖДЕНИЯ ПРИ ВОССТАНОВЛЕНИИ:")
        for err in filtered_errors: print(err)

    print("СТАТУС: Disaster Recovery завершен. База данных приведена в соответствие со стабильным бэкапом.")
```

**Критерии приемки (DoD):**
1.  Пайплайн устойчив к повторным запускам (идемпотентность обеспечена конструкциями `ON CONFLICT` и `NOT EXISTS`).
2.  Механизм Disaster Recovery корректно идентифицирует расхождения в DDL/DML и выполняет откат без потери консистентности.
3.  Суррогатные ключи (`company_sk`, `metric_id`) корректно разрешаются при загрузке новых фактов.

#### СТРУКТУРА MOCK-ФАЙЛА ДЛЯ ИНТЕРАКТИВНОГО ИМПОРТА (DATA INGESTION)

Ниже представлен набор синтетических данных (Mock-data) в формате CSV. Данный датасет расширяет существующую модель данных за счет добавления новых временных периодов (Q1 2024), новых эмитентов (LKOH, OZON) и новых финансовых метрик (REVENUE, FREE_CASH_FLOW).

**Инструкция по применению:**
1. Скопируйте содержимое блока кода ниже.
2. Сохраните его на локальном компьютере в текстовый файл с расширением `.csv` (например, `new_financial_data.csv`). Убедитесь, что используется кодировка UTF-8.
3. Загрузите данный файл при выполнении скрипта **ЭТАПА 4 (INTERACTIVE DATA INGESTION)** из методического пособия.

```csv
report_date,ticker,company_name,gics_sector,metric_code,metric_type,metric_value
2024-03-31,SBER,Sberbank,Financials,EBITDA,FINANCIAL,380000.00
2024-03-31,SBER,Sberbank,Financials,NET_PROFIT,FINANCIAL,350000.00
2024-03-31,GAZP,Gazprom,Energy,EBITDA,FINANCIAL,600000.00
2024-03-31,GAZP,Gazprom,Energy,NET_PROFIT,FINANCIAL,450000.00
2023-12-31,LKOH,Lukoil,Energy,EBITDA,FINANCIAL,1800000.00
2023-12-31,LKOH,Lukoil,Energy,NET_PROFIT,FINANCIAL,1100000.00
2024-03-31,LKOH,Lukoil,Energy,EBITDA,FINANCIAL,450000.00
2024-03-31,LKOH,Lukoil,Energy,NET_PROFIT,FINANCIAL,280000.00
2023-12-31,OZON,Ozon Holdings,Consumer Discretionary,REVENUE,FINANCIAL,420000.00
2023-12-31,OZON,Ozon Holdings,Consumer Discretionary,EBITDA,FINANCIAL,-15000.00
2024-03-31,OZON,Ozon Holdings,Consumer Discretionary,REVENUE,FINANCIAL,125000.00
2024-03-31,OZON,Ozon Holdings,Consumer Discretionary,EBITDA,FINANCIAL,2500.00
2024-03-31,YNDX,Yandex,Information Technology,REVENUE,FINANCIAL,220000.00
2024-03-31,YNDX,Yandex,Information Technology,FREE_CASH_FLOW,FINANCIAL,45000.00
```

**Ожидаемое поведение ELT-пайплайна при загрузке данного файла:**
*   **dim_company:** Будут добавлены новые записи для `LKOH` и `OZON`. Существующие записи (`SBER`, `GAZP`, `YNDX`) будут проигнорированы или обновлены (UPSERT) без нарушения целостности суррогатных ключей.
*   **dim_metric_dictionary:** Будут добавлены новые метрики `REVENUE` и `FREE_CASH_FLOW`.
*   **fact_financial_metrics:** Будут вставлены 14 новых транзакций. Суррогатные ключи (`company_sk`, `metric_id`) будут динамически разрешены на стороне СУБД через SQL JOIN.

# ЗАДАНИЕ ДЛЯ САМОСТОЯТЕЛЬНОЙ РАБОТЫ

### ПРАКТИЧЕСКИЕ ЗАДАНИЯ ДЛЯ САМОСТОЯТЕЛЬНОЙ РАБОТЫ (PRACTICAL ASSIGNMENTS)

**Дисциплина:** Интеллектуальные хранилища данных / Data Engineering.
**Среда выполнения:** Google Colab (на базе ранее развернутого инстанса PostgreSQL).
**Формат сдачи:** Jupyter Notebook (`.ipynb`) с исполненными ячейками, DDL/DML скриптами и аналитическими выводами.

---

#### ЗАДАНИЕ 1: АРХИТЕКТУРНАЯ ЭВОЛЮЦИЯ ХРАНИЛИЩА (SCHEMA EVOLUTION & SCD TYPE 2)
**Бизнес-контекст:** Компании часто меняют секторальную принадлежность (GICS Sector) или тикеры в результате M&A сделок или ребрендинга. Текущая реализация `dim_company` (SCD Type 1) перезаписывает исторические данные, что искажает ретроспективную аналитику.

**Техническое задание:**
1.  Модифицировать DDL-схему таблицы `dim_company` для поддержки Slowly Changing Dimensions (SCD) Type 2.
2.  Добавить атрибуты: `valid_from` (DATE), `valid_to` (DATE), `is_current` (BOOLEAN).
3.  Написать SQL-скрипт (или Python-логику с SQLAlchemy), который при поступлении обновленного сектора для существующего тикера (например, `YNDX` меняет сектор с `Information Technology` на `Communication Services`):
    *   Закрывает текущую запись (`valid_to = CURRENT_DATE`, `is_current = FALSE`).
    *   Вставляет новую строку с новым суррогатным ключом (`company_sk`), `valid_from = CURRENT_DATE` и `is_current = TRUE`.

**Критерии приемки (DoD):**
*   Наличие DDL-скрипта миграции (ALTER TABLE / CREATE TABLE).
*   Успешное выполнение тестового DML-запроса, демонстрирующего сохранение истории изменений атрибутов эмитента.
*   Аналитический SQL-запрос (JOIN фактов и измерений) корректно использует суррогатные ключи для привязки исторических фактов к историческим версиям измерений.

---

#### ЗАДАНИЕ 2: АВТОМАТИЗАЦИЯ DATA INGESTION ЧЕРЕЗ REST API
**Бизнес-контекст:** Ручная загрузка CSV-файлов не соответствует стандартам автоматизированных Data Pipelines. Требуется интеграция с внешним источником данных.

**Техническое задание:**
1.  Разработать Python-скрипт для извлечения макроэкономических данных (например, Ключевая ставка ЦБ РФ или уровень инфляции) через публичный REST API (допускается использование API Банка России, MOEX ISS или генерация Mock-API через `requests.get`).
2.  Реализовать парсинг JSON/XML ответа.
3.  Добавить новую метрику (например, `KEY_RATE`, тип `MACRO`) в `dim_metric_dictionary`.
4.  Обеспечить идемпотентную загрузку (UPSERT) извлеченных временных рядов в таблицу `fact_financial_metrics`.

**Критерии приемки (DoD):**
*   Отсутствие хардкода в маппинге суррогатных ключей (использование SQL `SELECT ... FROM dim_metric_dictionary WHERE metric_code = ...`).
*   Обработка исключений (HTTP 4xx/5xx, таймауты) при запросе к API.
*   Повторный запуск ячейки с Data Ingestion не приводит к дублированию записей в DWH (Zero-duplication policy).

---

#### ЗАДАНИЕ 3: SQL-АНАЛИТИКА И ПРОВЕРКА СТАТИСТИЧЕСКИХ ГИПОТЕЗ (HYPOTHESIS TESTING)
**Бизнес-контекст:** Инвестиционному комитету требуется оценка операционной эффективности компаний в разрезе секторов.

**Техническое задание:**
1.  Написать аналитический SQL-запрос с использованием оконных функций (Window Functions) для расчета производной метрики: **EBITDA Margin** (`EBITDA / REVENUE`).
    *   *Примечание:* Поскольку данные хранятся в EAV-модели, потребуется выполнить Self-Join таблицы фактов или использовать агрегацию с `CASE WHEN` (Pivot на уровне SQL).
2.  Выгрузить результаты в Pandas DataFrame.
3.  Сформулировать нулевую гипотезу (H0): *Медианная маржинальность по EBITDA в секторе 'Energy' не имеет статистически значимых отличий от сектора 'Financials'*.
4.  Провести статистический тест (например, U-критерий Манна-Уитни через `scipy.stats.mannwhitneyu`) на основе извлеченных данных.

**Критерии приемки (DoD):**
*   SQL-запрос выполняется на стороне PostgreSQL, возвращая готовые расчетные столбцы (Pushdown computation).
*   Код статистического теста корректно обрабатывает выборки.
*   Вывод содержит строгий аналитический вердикт (отвергается или принимается H0 с указанием p-value).

---

#### ЗАДАНИЕ 4: ИМИТАЦИЯ СБОЯ И АУДИТ DISASTER RECOVERY (DATA DRIFT INJECTION)
**Бизнес-контекст:** Инженер данных должен уметь валидировать механизмы восстановления после логического повреждения данных (Data Poisoning) или несанкционированного изменения схемы (Schema Drift).

**Техническое задание:**
1.  Создать эталонный бэкап базы данных (используя скрипт из ЭТАПА 6 методического пособия).
2.  Написать и выполнить деструктивный DML/DDL скрипт (Data Poisoning):
    *   Удалить случайные 10 строк из `fact_financial_metrics`.
    *   Изменить значение `metric_value` для тикера `SBER` на некорректное (например, умножить на 1000).
    *   Удалить индекс `idx_fact_fin_metrics`.
3.  Запустить скрипт Disaster Recovery (ЭТАП 7).
4.  Написать SQL-запрос для аудита (Data Reconciliation), доказывающий, что удаленные строки восстановлены, значения корректны, а индекс пересоздан.

**Критерии приемки (DoD):**
*   Логи выполнения скрипта DR должны явно зафиксировать расхождение (Data Drift) между текущей БД и бэкапом.
*   После отработки DR скрипта, контрольная сумма (например, `SELECT SUM(metric_value) FROM fact_financial_metrics`) должна строго совпадать с эталонным значением до сбоя.
*   Наличие SQL-запроса к системному каталогу `pg_indexes`, подтверждающего восстановление удаленного индекса.
