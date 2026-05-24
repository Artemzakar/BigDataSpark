# Отчет по лабораторной работе N2

## Тема

ETL-пайплайн на Apache Spark: загрузка исходных CSV-данных в PostgreSQL, построение модели звезда и формирование отчетов в ClickHouse.

## Реализованная часть

Выполнена обязательная часть лабораторной работы:

- PostgreSQL используется для хранения исходной таблицы и модели звезда.
- Apache Spark используется для ETL-трансформаций.
- ClickHouse используется для хранения итоговых отчетов.

Опциональные Cassandra, Neo4j, MongoDB и Valkey не добавлялись, так как они указаны в задании как бонусные.

## Структура проекта

- `docker-compose.yml` - поднимает PostgreSQL, Spark и ClickHouse.
- `исходные данные/` - 10 CSV-файлов с исходными данными.
- `spark_jobs/common.py` - общие настройки подключения к PostgreSQL и ClickHouse.
- `spark_jobs/01_load_raw_to_postgres.py` - загрузка исходных CSV в PostgreSQL.
- `spark_jobs/02_build_star_schema.py` - построение модели звезда в PostgreSQL.
- `spark_jobs/03_build_clickhouse_reports.py` - построение отчетов в ClickHouse.

## Схема хранения в PostgreSQL

Исходные данные загружаются в таблицу:

- `mock_data`

После трансформации Spark создает модель звезда:

- `dim_customer` - измерение клиентов.
- `dim_seller` - измерение продавцов.
- `dim_supplier` - измерение поставщиков.
- `dim_product` - измерение продуктов.
- `dim_store` - измерение магазинов.
- `dim_date` - измерение дат.
- `fact_sales` - факт продаж.

## Отчеты в ClickHouse

Spark формирует 6 таблиц-отчетов:

- `report_product_sales` - витрина продаж по продуктам.
- `report_customer_sales` - витрина продаж по клиентам.
- `report_time_sales` - витрина продаж по времени.
- `report_store_sales` - витрина продаж по магазинам.
- `report_supplier_sales` - витрина продаж по поставщикам.
- `report_product_quality` - витрина качества продукции.

## Запуск

Поднять контейнеры:

```bash
docker compose up -d
```

Загрузить исходные CSV в PostgreSQL:

```bash
docker compose exec spark /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy2 --packages org.postgresql:postgresql:42.7.3,com.clickhouse:clickhouse-jdbc:0.6.3 /opt/spark-apps/01_load_raw_to_postgres.py
```

Построить модель звезда в PostgreSQL:

```bash
docker compose exec spark /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy2 --packages org.postgresql:postgresql:42.7.3,com.clickhouse:clickhouse-jdbc:0.6.3 /opt/spark-apps/02_build_star_schema.py
```

Построить отчеты в ClickHouse:

```bash
docker compose exec spark /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy2 --packages org.postgresql:postgresql:42.7.3,com.clickhouse:clickhouse-jdbc:0.6.3 /opt/spark-apps/03_build_clickhouse_reports.py
```

Остановить контейнеры:

```bash
docker compose down
```

## Подключение через DBeaver

PostgreSQL:

- Host: `localhost`
- Port: `55432`
- Database: `lab`
- User: `lab`
- Password: `lab`

ClickHouse:

- Host: `localhost`
- Port: `8123`
- Database: `default`
- User: `default`
- Password: `lab`

## Проверка

Проверить таблицы PostgreSQL:

```bash
docker compose exec postgres psql -U lab -d lab -c "\dt"
docker compose exec postgres psql -U lab -d lab -c "select count(*) from mock_data;"
docker compose exec postgres psql -U lab -d lab -c "select count(*) from fact_sales;"
```

Проверить таблицы ClickHouse:

```bash
docker compose exec clickhouse clickhouse-client --password lab -q "show tables"
docker compose exec clickhouse clickhouse-client --password lab -q "select * from report_product_sales order by product_sales_rank limit 10"
docker compose exec clickhouse clickhouse-client --password lab -q "select * from report_store_sales order by store_revenue_rank limit 5"
```

## Результаты проверки

Пайплайн был запущен полностью. Получены следующие результаты:

- `mock_data` в PostgreSQL: 10000 строк.
- `fact_sales` в PostgreSQL: 10000 строк.
- `report_product_sales` в ClickHouse: 10000 строк.
- `report_customer_sales` в ClickHouse: 10000 строк.
- `report_time_sales` в ClickHouse: 12 строк.
- `report_store_sales` в ClickHouse: 10000 строк.
- `report_supplier_sales` в ClickHouse: 10000 строк.
- `report_product_quality` в ClickHouse: 10000 строк.

Все обязательные отчеты в ClickHouse созданы.
