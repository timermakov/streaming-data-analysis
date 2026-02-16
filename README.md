# Streaming Data Analysis

Лабораторные работы по потоковому анализу данных.

## Lab 1: Kafka + Spark Streaming

E-Commerce pipeline: CSV → Kafka Producer (Avro) → Spark Structured Streaming → Parquet.

### Обоснование выбора технологий

**Avro** (вместо Protobuf):
- Нативная интеграция с Kafka и Spark (`from_avro` из коробки)
- Self-describing формат, поддержка эволюции схемы
- Стандарт для data pipelines

**Parquet** (вместо ORC):
- Индустриальный стандарт для аналитики (pandas, DuckDB, Spark, BigQuery)
- Эффективное колоночное сжатие, predicate pushdown

### Запуск

#### Вариант 1: Kafka в Docker, сервисы локально

```bash
# Установить зависимости
poetry install

# Запустить Kafka (KRaft mode, без Zookeeper)
docker compose up -d

# Терминал 1: запустить consumer (Spark Streaming → Parquet)
poetry run python -m src.lab1.consumer

# Терминал 2: запустить producer (CSV → Kafka)
poetry run python -m src.lab1.producer
```

#### Вариант 2: всё в Docker

```bash
docker compose --profile lab1 up --build
```

#### Остановка

```bash
docker compose --profile lab1 down
```

### Параметры producer

```
--csv-path        путь к CSV (default: data/lab1/E-Commerce Data.csv)
--bootstrap-servers  адрес Kafka (default: localhost:9092)
--topic           имя топика (default: ecommerce-transactions)
--batch-size      размер чанка CSV (default: 500)
--delay           задержка между батчами в секундах (default: 0.1)
```

### Результат

Parquet-файлы записываются в `output/lab1/`, партиционированные по `Country`.

### Тесты

```bash
poetry run pytest tests/ -v
```
