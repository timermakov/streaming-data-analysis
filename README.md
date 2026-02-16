# Streaming Data Analysis

Лабораторные работы по потоковому анализу данных.

## Lab 1: Kafka + Spark Streaming

E-Commerce pipeline: CSV → Kafka Producer (Avro) → Spark Structured Streaming → Parquet.

### Быстрый старт

```bash
# 1. Установить зависимости
poetry install

# 2. Запустить инфраструктуру (Kafka KRaft)
docker compose up -d

# 3. Запустить consumer (Spark Streaming)
poetry run python -m src.lab1.consumer

# 4. В другом терминале — запустить producer
poetry run python -m src.lab1.producer
```

### Технологии

- **Сериализация**: Avro (fastavro)
- **Брокер**: Apache Kafka (KRaft mode)
- **Обработка**: PySpark Structured Streaming
- **Хранение**: Parquet (partitioned by Country)
