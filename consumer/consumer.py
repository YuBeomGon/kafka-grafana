import json
import time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from sqlalchemy import create_engine, Column, Integer, Float, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Kafka and PostgreSQL configuration
KAFKA_BROKER = 'kafka:9092'  # Kafka 브로커 주소
TOPIC_NAME = 'system_metrics'  # Kafka 토픽 이름

POSTGRES_HOST = 'postgresql'
POSTGRES_DB = 'metricsdb'
POSTGRES_USER = 'jake'
POSTGRES_PASSWORD = 'kk3249'

# SQLAlchemy Base class
Base = declarative_base()

# Define the system_metrics table using SQLAlchemy ORM
class SystemMetrics(Base):
    __tablename__ = 'system_metrics'

    id = Column(Integer, primary_key=True, autoincrement=True)
    cpu_percent = Column(Float)
    memory_percent = Column(Float)
    disk_usage_percent = Column(Float)
    cpu_core_percent = Column(JSON)  # 각 CPU 코어 사용률을 JSON 형태로 저장
    gpu_info = Column(JSON)  # GPU 정보를 JSON 형태로 저장
    timestamp = Column(Float)

# Initialize Kafka consumer
while True:
    try:
        # Initialize Kafka consumer
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=KAFKA_BROKER,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True
        )
        break
    except NoBrokersAvailable:
        print("Kafka broker not available. Retrying in 5 seconds...")
        time.sleep(5)

# Set up SQLAlchemy database connection
DATABASE_URL = f'postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}/{POSTGRES_DB}'
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

def create_table():
    """
    Create the system_metrics table if it doesn't exist.
    """
    Base.metadata.create_all(engine)

def consume_metrics():
    """
    Consume metrics from Kafka and store them in PostgreSQL using SQLAlchemy.
    """
    session = Session()

    try:
        for message in consumer:
            metrics = message.value
            print(f"Received metrics: {metrics}")

            # Create a SystemMetrics object with the received data
            system_metrics = SystemMetrics(
                cpu_percent=metrics.get('cpu_percent'),
                memory_percent=metrics.get('memory_percent'),
                disk_usage_percent=metrics.get('disk_usage_percent'),
                cpu_core_percent=metrics.get('cpu_core_percent'),
                gpu_info=metrics.get('gpu_info'),
                timestamp=metrics.get('timestamp')
            )

            # Add and commit the new record to the database
            session.add(system_metrics)
            session.commit()

            time.sleep(1)  # 잠시 대기

    except Exception as e:
        print(f"Error while consuming metrics: {e}")
        session.rollback()  # 예외가 발생하면 트랜잭션을 롤백
    finally:
        session.close()  # 세션을 닫습니다.

if __name__ == '__main__':
    # Create table if not exists
    create_table()
    # Start consuming Kafka messages
    consume_metrics()
