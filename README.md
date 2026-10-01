# Real-Time Fraud Detection System

Учебный по MLOps. Датасеты предоставлены в рамках соревнования https://www.kaggle.com/competitions/teta-ml-1-2025

Система для обнаружения мошеннических транзакций в реальном времени с использованием ML-модели и Kafka для потоковой обработки данных.

## 🏗️ Архитектура

Компоненты системы:
1. **`interface`** (Streamlit UI):
   
   Создан для удобной симуляции потоковых данных с транзакциями. Реальный продукт использовал бы прямой поток данных из других систем.
    - Имитирует отправку транзакций в Kafka через CSV-файлы.
    - Генерирует уникальные ID для транзакций.
    - Загружает транзакции отдельными сообщениями формата JSON в топик kafka `transactions`.
    

2. **`fraud_detector`** (ML Service):
   - Загружает предобученную модель CatBoost (`my_catboost.cbm`).
   - Выполняет препроцессинг данных:
     - Извлечение временных признаков
     - Гео-расстояния
     - Кодирование категориальных переменных
   - Производит скоринг с порогом 0.98.
   - Выгружает результат скоринга в топик kafka `scoring`

3. **Kafka Infrastructure**:
   - Zookeeper + Kafka брокер
   - `kafka-setup`: автоматически создает топики `transactions` и `scoring`
    - Kafka UI: веб-интерфейс для мониторинга сообщений (порт 8080)

4. **`db_writer` и PostgreSQL**:
   - `db_writer` читает результаты из топика `scoring` и сохраняет их в PostgreSQL.
   - PostgreSQL хранит витрину `transaction_scores` в именованном Docker volume.
   - Streamlit показывает последние мошеннические операции и гистограмму последних скоров.

5. **`train`**:
   - Простенькое обучение с помощью CatBoost.
   - Сохраняет модель в общий volume. `fraud_detector` использует её при старте.

## 🚀 Быстрый старт

### Требования
- Docker 20.10+
- Docker Compose 2.0+

### Запуск
```bash
git clone https://github.com/your-repo/fraud-detection-system.git
cd fraud-detection-system

# Сборка и запуск всех сервисов
docker-compose up --build
```
Обучение не будет запускаться, так как для него задан профиль в docker-compose.yaml. 

Для первого запуска используется уже включенная в образ модель. Чтобы потренироваться в обучении и создать/обновить модель самостоятельно, выполните:

```bash
docker compose --profile training run --build --rm train
```

По умолчанию обучение использует 300 итераций. Значение настраивается переменной `TRAIN_ITERATIONS`. После обучения перезапустите обработчик, чтобы он загрузил новую модель:

```bash
docker compose restart fraud_detector
```

После запуска:
- **Streamlit UI**: http://localhost:8501
- **Kafka UI**: http://localhost:8080
- **Логи сервисов**: 
  ```bash
  docker-compose logs <service_name>  # Например: fraud_detector, kafka, interface
  ```

Результаты доступны в интерфейсе Streamlit в разделе «Результаты скоринга». Для сброса удалите volumes командой `docker compose down -v`.

## 🛠️ Использование

### 1. Загрузка данных:

 - Загрузите CSV через интерфейс Streamlit. Для тестирования работы проекта используется файл формата `test.csv` из соревнования https://www.kaggle.com/competitions/teta-ml-1-2025
 - Пример структуры данных:
    ```csv
    transaction_time,amount,lat,lon,merchant_lat,merchant_lon,gender,...
    2023-01-01 12:30:00,150.50,40.7128,-74.0060,40.7580,-73.9855,M,...
    ```
 - Для первых тестов рекомендуется загружать небольшой семпл данных (до 100 транзакций) за раз, чтобы исполнение кода не заняло много времени.

### 2. Мониторинг:
 - **Kafka UI**: Просматривайте сообщения в топиках transactions и scoring
 - **Логи обработки**: /app/logs/service.log внутри контейнера fraud_detector

### 3. Результаты:

  - Скоринговые оценки пишутся в топик `scoring` и сохраняются в PostgreSQL в формате:
     ```json
     {
     "transaction_id": "d6b0f7a0-8e1a-4a3c-9b2d-5c8f9d1e2f3a",
     "score": 0.995,
     "fraud_flag": 1
     }
    ```
## Структура проекта
```
.
├── fraud_detector/
│   ├── src/
│   │   ├── preprocessing.py # Логика препроцессинга
│   │   └── scorer.py        # Загрузка модели и предсказания
│   ├── app/app.py           # Kafka Consumer/Producer
│   ├── train.py             # Обучение модели
│   └── Dockerfile
├── db_writer/               # Kafka → PostgreSQL
│   └── app.py
├── interface/
│   └── app.py              # Streamlit UI
├── docker-compose.yaml
├── ARCHITECTURE.md
└── README.md
```

## Настройки Kafka
```yml
Топики:
- transactions (входные данные)
- scoring (результаты скоринга)

Репликация: 1 (для разработки)
Партиции: 3
```

*Примечание:* 

Для первого запуска в образ включена модель `fraud_detector/models/my_catboost.cbm`. Обучение использует `fraud_detector/train_data/train.csv`. Порты 8080, 8501 и 9095 должны быть свободны на хосте.
