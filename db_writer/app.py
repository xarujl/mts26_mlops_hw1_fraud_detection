import json
import logging
import os
import time

import psycopg2
from confluent_kafka import Consumer

logging.basicConfig(
    level=os.getenv('LOG_LEVEL', 'INFO'),
    format='%(asctime)s %(levelname)s %(message)s',
)
logger = logging.getLogger(__name__)

KAFKA_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
TOPIC = os.getenv('KAFKA_SCORING_TOPIC', 'scoring')
GROUP = os.getenv('KAFKA_GROUP_ID', 'score-db-writer')


def connect_db():
    while True:
        try:
            connection = psycopg2.connect(
                host=os.getenv('POSTGRES_HOST', 'postgres'),
                port=os.getenv('POSTGRES_PORT', '5432'),
                dbname=os.getenv('POSTGRES_DB', 'fraud'),
                user=os.getenv('POSTGRES_USER', 'fraud'),
                password=os.getenv('POSTGRES_PASSWORD', 'fraud'),
            )
            connection.autocommit = True
            with connection.cursor() as cursor:
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS transaction_scores (
                        transaction_id TEXT PRIMARY KEY,
                        score DOUBLE PRECISION NOT NULL,
                        fraud_flag SMALLINT NOT NULL CHECK (fraud_flag IN (0, 1)),
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                ''')
            logger.info('Connected to PostgreSQL and ensured table exists')
            return connection
        except psycopg2.OperationalError as exc:
            logger.warning('PostgreSQL is not ready (%s); retrying in 3 seconds', exc)
            time.sleep(3)


def main():
    connection = connect_db()
    consumer = Consumer({
        'bootstrap.servers': KAFKA_SERVERS,
        'group.id': GROUP,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
    })
    consumer.subscribe([TOPIC])
    logger.info('Listening to Kafka topic %s', TOPIC)
    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                logger.error('Kafka error: %s', message.error())
                continue
            try:
                results = json.loads(message.value().decode('utf-8'))
                # Accept a single result object and the legacy one-item JSON
                # array format emitted by the original scorer.
                if isinstance(results, dict):
                    results = [results]
                if not isinstance(results, list):
                    raise ValueError('Scoring message must be a JSON object or list')
                with connection.cursor() as cursor:
                    for result in results:
                        cursor.execute('''
                            INSERT INTO transaction_scores (transaction_id, score, fraud_flag)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (transaction_id) DO UPDATE SET
                                score = EXCLUDED.score,
                                fraud_flag = EXCLUDED.fraud_flag,
                                created_at = NOW()
                        ''', (
                            str(result['transaction_id']),
                            float(result['score']),
                            int(result['fraud_flag']),
                        ))
                consumer.commit(message=message)
            except (ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
                logger.warning('Invalid scoring message; skipping: %s', exc)
                consumer.commit(message=message)
            except psycopg2.Error:
                logger.exception('Database write failed; will retry message')
                connection.close()
                connection = connect_db()
                time.sleep(1)
    except KeyboardInterrupt:
        logger.info('Stopping database writer')
    finally:
        consumer.close()
        connection.close()


if __name__ == '__main__':
    main()
